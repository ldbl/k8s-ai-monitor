"""LLM abstraction — extracted from analyzer.py."""
import json
import logging
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass

from src import config
from src.engine import central_push

logger = logging.getLogger(__name__)

ALERT_SYSTEM_PROMPT = """You are a Kubernetes SRE assistant. Analyze the JSON diagnostic data.
Return STRICT JSON only. No text outside JSON.

{{
  "root_cause": "string - specific cause with evidence from the data",
  "confidence": 0.0-1.0,
  "severity": "warning|critical",
  "hypotheses": [
    {{"cause": "string", "confidence": 0.0-1.0, "evidence": ["string"]}}
  ],
  "impact": "string - what is affected and blast radius",
  "suggested_actions": [
    {{"action": "string - concrete kubectl/config step", "priority": 1-3}}
  ]
}}

Rules:
- hypotheses: >=1, each with evidence from the provided data
- suggested_actions: priority 1=immediate, 2=short-term, 3=preventive
- severity: critical for outage/data-loss/OOM, warning for degraded/restarts
- JSON only, no fences

Cluster: {cluster_name}"""

DAILY_REPORT_SYSTEM_PROMPT = """You are a Kubernetes operations expert. You receive cluster health data as JSON containing: current state snapshot and last 24-hour history (incidents, restarts, OOM kills).
Return STRICT JSON only. No text outside JSON.

{{
  "overall_status": "healthy|degraded|critical",
  "confidence": 0.0-1.0,
  "summary": "1-2 sentence overview",
  "issues": [
    {{"description": "string", "severity": "critical|warning|info", "affected": "what's affected"}}
  ],
  "trends": [
    {{"description": "pattern observed"}}
  ],
  "recommendations": [
    {{"action": "concrete step", "priority": 1-3}}
  ]
}}

## Severity Matrix — determines overall_status:

CRITICAL (real outage — immediate action required):
- Node(s) NotReady
- Production pods in CrashLoopBackOff RIGHT NOW (phase != "Running" or ready == false)
- Active OOMKilled events in the last 2 hours
- PVC usage > 95%
- Multiple HelmRelease/Kustomization failures (deployments blocked)

WARNING (needs attention, but workloads ARE running):
- Pods restarted recently AND still not stable (hours_since_last_restart < 2 AND restarts > 3)
- Single HelmRelease or Kustomization failure
- Certificates expiring in < 7 days
- Node memory > 90% together with OOM kills

INFORMATIONAL (mention in issues/trends as "info", do NOT raise overall_status):
- Pod restarted but currently Running stable (hours_since_last_restart > 2, phase="Running", ready=true) — RECOVERED, not degraded
- HelmRepository / HelmChart / GitRepository / OCIRepository errors — source-level Flux, does NOT impact running workloads unless a downstream HelmRelease is also failing
- System/infra pod restarts (external-dns, cert-manager, kube-proxy, coredns) with low count and currently stable
- Node memory 70-90% — normal Linux page cache, reclaimable on demand
- Resolved or acknowledged incidents from last_24h
- FailedScheduling for ephemeral/runner pods (actions-runner, github-runner) — normal queue behavior
- hcloud-csi-node image pull errors on new/autoscaled nodes — transient and expected

## Key Rule:
overall_status reflects ACTIVE impact on production workloads ONLY.
- If all pods are Running+ready, all nodes Ready, deployments succeeding → "healthy" even with info-level observations.
- "degraded" requires: production pods not Running/not ready, active crash loops, or failed deployments blocking releases.
- "critical" requires: actual outage, data loss risk, or nodes down.
- Report info/warning items in "issues" list, but do NOT escalate overall_status unless the criteria above are met.

## Pod restart triage:
- Each entry has "hours_since_last_restart", "phase", and "ready" fields.
- hours_since_last_restart > 2 AND phase="Running" AND ready=true → RECOVERED, informational only.
- hours_since_last_restart < 2 AND restarts > 3 → active crash loop, warning or critical.
- Multiple pods restarting on the SAME node → likely node-level issue (memory pressure, OOM), report as node problem.
- pod_restarts_older: restarts outside last 24h — purely informational, never raise severity.

## Flux resource triage:
- HelmRelease / Kustomization failure = deployment pipeline blocked → warning.
- HelmRepository / GitRepository / OCIRepository / HelmChart error = source-level only, existing releases keep running → informational.
- Only escalate source errors if a downstream HelmRelease is ALSO failing.

## Incident status triage (last_24h):
- "active": assess current impact using the rules above.
- "resolved": occurred and recovered — mention in trends, never raise severity.
- "acknowledged": someone is aware — mention, don't escalate.

## General:
- If all data sections are empty → overall_status="healthy", summary="All systems operational".
- issues: group by severity, reference specific pods/nodes.
- recommendations: priority 1=immediate, 2=short-term, 3=preventive.
- confidence: 0.0-1.0 based on data completeness.
- JSON only, no fences.

Cluster: {cluster_name}"""

WEEKLY_REPORT_SYSTEM_PROMPT = """You are a Kubernetes operations expert. You receive a weekly summary as JSON containing: daily report statuses, incident aggregation by type/severity, incident details, and LLM cost data for the past 7 days.
Return STRICT JSON only. No text outside JSON.

{{
  "overall_trend": "improving|stable|degrading",
  "summary": "1-2 sentence week overview",
  "recurring_issues": [
    {{"description": "string", "frequency": 0, "services_affected": ["string"], "impact_level": "production|operational|noise"}}
  ],
  "trends": [
    {{"description": "pattern observed", "direction": "improving|worsening|stable"}}
  ],
  "recommendations": [
    {{"action": "concrete step", "priority": 1-3, "impact": "expected outcome"}}
  ],
  "cost_summary": {{
    "total_llm_cost_usd": 0.0,
    "total_alerts": 0,
    "total_reports": 0
  }}
}}

## Impact Classification (CRITICAL — apply before analyzing):

PRODUCTION impact (affects users/workloads — report as recurring_issues):
- Pods in CrashLoopBackOff that did NOT recover (status remains "active")
- OOMKilled events on production workload pods
- HelmRelease / Kustomization failures (deployment pipeline blocked)
- Node NotReady events
- PVC near-full on production volumes
- Backup job failures for production databases

OPERATIONAL impact (infrastructure concern, no user impact — mention briefly):
- System/infra pod restarts that self-healed (external-dns, cert-manager, coredns, node-exporter, kube-proxy, k8s-ai-monitor) — these are self-healing infrastructure components, NOT application instability
- Single HelmRelease failure that resolved quickly
- Certificate warnings (expiring > 7 days)

NOISE (do NOT include in recurring_issues or trends):
- HelmRepository / GitRepository / OCIRepository / HelmChart errors — source-level Flux, does NOT block running workloads. Only mention if a downstream HelmRelease is ALSO failing.
- FailedScheduling for ephemeral/runner pods (actions-runner, github-runner) — normal queue behavior, runners scale up and down constantly
- hcloud-csi-node / csi-driver image pull errors on new/autoscaled nodes — transient and expected
- Resolved incidents that did not recur — happened once, self-healed, done
- Pod restarts where status="resolved" and occurrence_count=1 — one-off restart, not a pattern

## Rules for recurring_issues:
- ONLY include issues with PRODUCTION or OPERATIONAL impact_level
- Do NOT group unrelated services into one "recurring issue" — external-dns restarts and temporal-worker restarts are different issues with different root causes
- Each recurring_issue must describe ONE specific problem, not a category
- frequency = actual count of occurrences, not number of affected services
- Set impact_level: "production" for user-facing, "operational" for infra-only, "noise" (but these should not appear)

## Rules for overall_trend:
- "degrading" ONLY if production-impacting incidents are INCREASING in frequency or severity across the week
- "improving" if production incidents decreased or resolved
- "stable" if the pattern is consistent — even if there are operational/noise items
- Infrastructure noise (source-level Flux, runner scheduling, self-healing restarts) does NOT affect trend direction

## Rules for recommendations:
- Be SPECIFIC: name the exact service, namespace, and action. "Investigate restarts" is not actionable.
- Do NOT recommend generic best practices ("add probes", "validate RBAC", "add monitoring") unless there is specific evidence they are missing
- Focus on ROOT CAUSE: if external-dns restarts weekly, recommend checking the specific provider config or memory limits — not "inspect logs"
- priority 1 = fix this week (production impact), 2 = fix soon (operational), 3 = track/improve (pattern)
- Maximum 3 recommendations. Quality over quantity.

## Rules for trends:
- Compare daily report statuses across the week — are there more "degraded" days at the end vs the start?
- Look at incident severity distribution changes, not just counts
- Self-healing restarts are a PATTERN to note ("X restarts weekly but self-heals"), not "instability"

- cost_summary: use the provided LLM usage data
- JSON only, no fences

Cluster: {cluster_name}"""

# Thread-safe sliding window rate limiter
_call_timestamps: deque[float] = deque()
_rate_lock = threading.Lock()

# Pricing per million tokens: {model_prefix: (input_$/MTok, output_$/MTok)}
_MODEL_PRICING = {
    "claude-opus-4":      (5.0, 25.0),
    "claude-sonnet-4":    (3.0, 15.0),
    "claude-haiku-4":     (1.0, 5.0),
    "gpt-4o-mini":        (0.15, 0.6),
    "gpt-4o":             (2.5, 10.0),
    "gpt-4.1-mini":       (0.4, 1.6),
    "gpt-4.1-nano":       (0.1, 0.4),
    "gpt-5-mini":         (0.25, 2.0),
    "gpt-5.2":            (1.75, 14.0),
}

# Structured JSON parsing defaults
_ANALYSIS_DEFAULTS = {
    "confidence": 0.3,
    "severity": "warning",
    "impact": "unknown",
    "hypotheses": [],
    "suggested_actions": [],
}


@dataclass
class AnalysisResult:
    raw_text: str
    parsed: dict | None        # structured JSON if parse succeeded
    parse_error: bool
    model: str
    tokens_in: int
    tokens_out: int
    cost_usd: float | None
    latency_ms: float = 0.0


def parse_analysis(raw: str) -> tuple[dict, bool]:
    """Parse structured JSON from LLM response. Returns (parsed_dict, had_error)."""
    text = raw.strip()
    # Strip markdown fences if LLM added them
    if text.startswith("```"):
        lines = text.split("\n")
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].startswith("```"):
                end_idx = i
                break
        text = "\n".join(lines[1:end_idx])

    try:
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("Expected JSON object")
        # Fill missing keys with defaults
        for key, default in _ANALYSIS_DEFAULTS.items():
            data.setdefault(key, default)
        # Ensure root_cause exists
        data.setdefault("root_cause", "Unknown")
        # Clamp confidence
        try:
            data["confidence"] = max(0.0, min(1.0, float(data.get("confidence", 0.3))))
        except (ValueError, TypeError):
            data["confidence"] = 0.3

        # Validate hypotheses
        if not isinstance(data.get("hypotheses"), list):
            data["hypotheses"] = []
        for h in data["hypotheses"]:
            if isinstance(h, dict):
                h.setdefault("cause", "")
                h.setdefault("confidence", data["confidence"])
                if not isinstance(h.get("evidence"), list):
                    h["evidence"] = []

        # Validate suggested_actions
        if not isinstance(data.get("suggested_actions"), list):
            data["suggested_actions"] = []
        for sa in data["suggested_actions"]:
            if isinstance(sa, dict):
                sa.setdefault("action", "")
                try:
                    sa["priority"] = max(1, min(3, int(sa.get("priority", 2))))
                except (ValueError, TypeError):
                    sa["priority"] = 2

        # Backward compat: old schema → new schema
        if not data["hypotheses"] and data.get("evidence"):
            data["hypotheses"] = [{
                "cause": data["root_cause"],
                "confidence": data["confidence"],
                "evidence": data["evidence"],
            }]
        if not data["suggested_actions"] and data.get("action_plan"):
            data["suggested_actions"] = [
                {"action": step, "priority": 2}
                for step in data["action_plan"]
            ]

        # Auto-derive human_needed
        data["human_needed"] = data.get("human_needed", data["confidence"] < 0.7)
        if data["confidence"] < 0.7:
            data["human_needed"] = True

        return data, False
    except (json.JSONDecodeError, ValueError, TypeError):
        return {
            "root_cause": raw,
            "confidence": 0.3,
            "severity": "warning",
            "impact": "unknown",
            "hypotheses": [],
            "suggested_actions": [],
            "human_needed": True,
            "_parse_error": True,
        }, True


_DAILY_REPORT_DEFAULTS = {
    "overall_status": "healthy",
    "confidence": 0.5,
    "summary": "",
    "issues": [],
    "trends": [],
    "recommendations": [],
}


def parse_daily_report(raw: str) -> tuple[dict, bool]:
    """Parse structured JSON from daily report LLM response. Returns (parsed_dict, had_error)."""
    text = raw.strip()
    # Strip markdown fences if LLM added them
    if text.startswith("```"):
        lines = text.split("\n")
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].startswith("```"):
                end_idx = i
                break
        text = "\n".join(lines[1:end_idx])

    try:
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("Expected JSON object")
        # Fill missing keys with defaults
        for key, default in _DAILY_REPORT_DEFAULTS.items():
            data.setdefault(key, default)
        # Validate overall_status
        if data["overall_status"] not in ("healthy", "degraded", "critical"):
            data["overall_status"] = "degraded"
        # Clamp confidence
        try:
            data["confidence"] = max(0.0, min(1.0, float(data.get("confidence", 0.5))))
        except (ValueError, TypeError):
            data["confidence"] = 0.5
        # Validate issues
        if not isinstance(data.get("issues"), list):
            data["issues"] = []
        for issue in data["issues"]:
            if isinstance(issue, dict):
                issue.setdefault("description", "")
                issue.setdefault("severity", "info")
                issue.setdefault("affected", "")
        # Validate trends
        if not isinstance(data.get("trends"), list):
            data["trends"] = []
        # Validate recommendations
        if not isinstance(data.get("recommendations"), list):
            data["recommendations"] = []
        for rec in data["recommendations"]:
            if isinstance(rec, dict):
                rec.setdefault("action", "")
                try:
                    rec["priority"] = max(1, min(3, int(rec.get("priority", 2))))
                except (ValueError, TypeError):
                    rec["priority"] = 2

        return data, False
    except (json.JSONDecodeError, ValueError, TypeError):
        return {
            "overall_status": "degraded",
            "confidence": 0.3,
            "summary": raw,
            "issues": [],
            "trends": [],
            "recommendations": [],
            "_parse_error": True,
        }, True


def _check_rate_limit() -> bool:
    with _rate_lock:
        now = time.time()
        cutoff = now - 3600
        while _call_timestamps and _call_timestamps[0] < cutoff:
            _call_timestamps.popleft()
        if len(_call_timestamps) >= config.MAX_LLM_CALLS_PER_HOUR:
            logger.warning(
                "Rate limit reached: %d/%d calls in the last hour",
                len(_call_timestamps), config.MAX_LLM_CALLS_PER_HOUR,
            )
            return False
        _call_timestamps.append(now)
        return True


def _get_client():
    if config.LLM_PROVIDER == "openai":
        import openai
        return "openai", openai.OpenAI(api_key=config.OPENAI_API_KEY, timeout=60.0)
    else:
        import anthropic
        return "anthropic", anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY, timeout=60.0)


def _get_model(provider: str, tier: str = "alert") -> str:
    if tier == "report" and config.LLM_MODEL_REPORT:
        return config.LLM_MODEL_REPORT
    if tier == "alert" and config.LLM_MODEL:
        return config.LLM_MODEL
    if tier == "report":
        return "gpt-5.2" if provider == "openai" else "claude-sonnet-4-5-20250929"
    return "gpt-5-mini" if provider == "openai" else "claude-haiku-4-5-20251001"


def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float | None:
    for prefix, (inp_price, out_price) in _MODEL_PRICING.items():
        if model.startswith(prefix):
            return (input_tokens * inp_price + output_tokens * out_price) / 1_000_000
    return None


def _format_cost(cost: float | None) -> str:
    return f"${cost:.4f}" if cost is not None else "?"


def _call_llm(provider: str, client, model: str, system: str, user_content: str,
              max_tokens: int) -> tuple[str, int, int, float]:
    """Call LLM and return (text, tokens_in, tokens_out, latency_ms)."""
    t0 = time.monotonic()
    if provider == "openai":
        # GPT-5+ models require max_completion_tokens instead of max_tokens
        token_param = ("max_completion_tokens" if model.startswith("gpt-5")
                       else "max_tokens")
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_content},
            ],
            **{token_param: max_tokens},
        )
        tokens_in = resp.usage.prompt_tokens if resp.usage else 0
        tokens_out = resp.usage.completion_tokens if resp.usage else 0
        if not resp.choices:
            logger.warning("OpenAI returned empty choices for %s (usage=%s)", model, resp.usage)
            text = ""
        else:
            text = resp.choices[0].message.content or ""
            finish = resp.choices[0].finish_reason
            if not text:
                logger.warning("OpenAI returned empty content (finish_reason=%s) for %s", finish, model)
    else:
        resp = client.messages.create(
            model=model,
            system=system,
            messages=[
                {"role": "user", "content": user_content},
                {"role": "assistant", "content": "{"},
            ],
            max_tokens=max_tokens,
        )
        tokens_in = resp.usage.input_tokens if resp.usage else 0
        tokens_out = resp.usage.output_tokens if resp.usage else 0
        text = "{" + resp.content[0].text
    latency_ms = (time.monotonic() - t0) * 1000
    cost = _estimate_cost(model, tokens_in, tokens_out)
    logger.info("LLM usage [%s]: input=%d, output=%d tokens, cost=%s, latency=%.0fms",
                 model, tokens_in, tokens_out, _format_cost(cost), latency_ms)
    return text, tokens_in, tokens_out, latency_ms


# --- LLM call logging to SQLite ---

_log_local = threading.local()


def _log_llm_debug(*, call_type, resource, system_prompt, user_content, response_text):
    """Log full LLM payloads to SQLite when LLM_DEBUG is enabled. Never raises."""
    if not config.LLM_DEBUG:
        return
    try:
        if not hasattr(_log_local, "conn") or _log_local.conn is None:
            _log_local.conn = sqlite3.connect(config.SQLITE_PATH)
            _log_local.conn.execute("PRAGMA journal_mode=WAL")
            _log_local.conn.execute("PRAGMA busy_timeout=5000")
        now = time.time()
        _log_local.conn.execute(
            """INSERT INTO llm_debug_payloads
               (called_at, call_type, resource, system_prompt, user_content,
                response_text, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (now, call_type, resource, system_prompt, user_content,
             response_text, now),
        )
        _log_local.conn.commit()
    except Exception:
        logger.debug("Failed to log LLM debug payload to SQLite", exc_info=True)


def _log_llm_call(*, call_type, provider, model, resource,
                  context_bytes, section_bytes, sections,
                  tokens_in, tokens_out, cost_usd, latency_ms,
                  truncated, truncation_notes, error=False):
    """Log LLM call to SQLite. Never raises — wrapped in try/except."""
    try:
        if not hasattr(_log_local, "conn") or _log_local.conn is None:
            _log_local.conn = sqlite3.connect(config.SQLITE_PATH)
            _log_local.conn.execute("PRAGMA journal_mode=WAL")
            _log_local.conn.execute("PRAGMA busy_timeout=5000")
        now = time.time()
        _log_local.conn.execute(
            """INSERT INTO llm_calls
               (called_at, call_type, provider, model, resource,
                context_bytes, section_bytes_json, sections_json,
                tokens_in, tokens_out, cost_usd, latency_ms,
                truncated, truncation_notes, error, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (now, call_type, provider, model, resource,
             context_bytes,
             json.dumps(section_bytes) if section_bytes else "{}",
             json.dumps(sections) if sections else "[]",
             tokens_in, tokens_out, cost_usd, latency_ms,
             truncated, "; ".join(truncation_notes) if truncation_notes else None,
             error, now),
        )
        _log_local.conn.commit()
    except Exception:
        logger.debug("Failed to log LLM call to SQLite", exc_info=True)


def analyze_alert(context: dict, resource: str = "") -> AnalysisResult:
    """Analyze alert context via LLM. Accepts structured dict, returns AnalysisResult."""
    from src.engine.sanitizer import sanitize_dict
    from src.engine.context_budget import build_alert_payload

    context = sanitize_dict(context)
    json_payload, truncated, trunc_notes = build_alert_payload(
        context, resource, config.CLUSTER_NAME, config.MAX_CONTEXT_BYTES_ALERT)

    context_bytes = len(json_payload.encode("utf-8"))

    if not _check_rate_limit():
        logger.warning("Skipped analysis for %s — rate limit exhausted", resource or "alert")
        return AnalysisResult(
            raw_text=f"Rate limit reached ({config.MAX_LLM_CALLS_PER_HOUR}/hr). "
                     f"Raw context:\n\n{json_payload[:2000]}",
            parsed=None, parse_error=True, model="", tokens_in=0, tokens_out=0, cost_usd=None,
        )

    provider, client = _get_client()
    model = _get_model(provider, tier="alert")
    system = ALERT_SYSTEM_PROMPT.format(cluster_name=config.CLUSTER_NAME)

    logger.info("Analyzing %s via %s (%s), context=%dB%s",
                resource or "alert", provider, model,
                context_bytes, " [TRUNCATED]" if truncated else "")
    try:
        raw_text, tokens_in, tokens_out, latency_ms = _call_llm(
            provider, client, model, system, json_payload, 4096)
        cost = _estimate_cost(model, tokens_in, tokens_out)
        parsed, parse_error = parse_analysis(raw_text)
        if parse_error:
            logger.warning("LLM parse failed for %s: len=%d, tokens_out=%d, model=%s",
                           resource or "alert", len(raw_text) if raw_text else 0, tokens_out, model)
        _log_llm_call(
            call_type="alert", provider=provider, model=model, resource=resource,
            context_bytes=context_bytes,
            section_bytes=None,
            sections=list(context.keys()) if isinstance(context, dict) else [],
            tokens_in=tokens_in, tokens_out=tokens_out,
            cost_usd=cost, latency_ms=latency_ms,
            truncated=truncated, truncation_notes=trunc_notes,
        )
        _log_llm_debug(
            call_type="alert", resource=resource,
            system_prompt=system, user_content=json_payload,
            response_text=raw_text,
        )
        central_push.push_llm_usage({
            "called_at": time.time(),
            "call_type": "alert",
            "provider": provider,
            "model": model,
            "resource": resource,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "cost_usd": cost or 0.0,
            "latency_ms": latency_ms,
            "error": False,
        })
        return AnalysisResult(
            raw_text=raw_text, parsed=parsed, parse_error=parse_error,
            model=model, tokens_in=tokens_in, tokens_out=tokens_out,
            cost_usd=cost, latency_ms=latency_ms,
        )
    except Exception:
        logger.exception("LLM analysis failed for %s", resource or "alert")
        _log_llm_call(
            call_type="alert", provider=provider, model=model, resource=resource,
            context_bytes=context_bytes,
            section_bytes=None,
            sections=list(context.keys()) if isinstance(context, dict) else [],
            tokens_in=0, tokens_out=0, cost_usd=None, latency_ms=0,
            truncated=truncated, truncation_notes=trunc_notes, error=True,
        )
        central_push.push_llm_usage({
            "called_at": time.time(),
            "call_type": "alert",
            "provider": provider,
            "model": model,
            "resource": resource,
            "tokens_in": 0,
            "tokens_out": 0,
            "cost_usd": 0.0,
            "latency_ms": 0.0,
            "error": True,
        })
        return AnalysisResult(
            raw_text="\u26a0\ufe0f AI analysis unavailable \u2014 LLM API error.",
            parsed=None, parse_error=True, model=model, tokens_in=0, tokens_out=0, cost_usd=None,
        )


def analyze_daily_report(context: dict) -> AnalysisResult:
    """Analyze daily report — accepts structured dict, returns AnalysisResult."""
    from src.engine.sanitizer import sanitize_dict
    from src.engine.context_budget import build_report_payload, measure_report_data

    context = sanitize_dict(context)
    measured = measure_report_data(context)
    json_payload, truncated, trunc_notes = build_report_payload(
        context, config.CLUSTER_NAME, config.MAX_CONTEXT_BYTES_REPORT)

    context_bytes = len(json_payload.encode("utf-8"))

    if not _check_rate_limit():
        logger.warning("Skipped daily report analysis — rate limit exhausted")
        return AnalysisResult(
            raw_text=f"Rate limit reached ({config.MAX_LLM_CALLS_PER_HOUR}/hr). "
                     f"Raw context:\n\n{json_payload[:2000]}",
            parsed=None, parse_error=True, model="", tokens_in=0, tokens_out=0, cost_usd=None,
        )

    provider, client = _get_client()
    model = _get_model(provider, tier="report")
    system = DAILY_REPORT_SYSTEM_PROMPT.format(cluster_name=config.CLUSTER_NAME)

    logger.info("Sending daily report to %s (%s), context=%dB%s",
                provider, model, context_bytes, " [TRUNCATED]" if truncated else "")
    try:
        raw_text, tokens_in, tokens_out, latency_ms = _call_llm(
            provider, client, model, system, json_payload, 4096)
        cost = _estimate_cost(model, tokens_in, tokens_out)
        parsed, parse_error = parse_daily_report(raw_text)
        if parse_error:
            logger.warning("LLM returned non-JSON for daily report, using raw text fallback")
        _log_llm_call(
            call_type="report", provider=provider, model=model, resource="daily_report",
            context_bytes=context_bytes,
            section_bytes=measured["section_bytes"],
            sections=measured["sections"],
            tokens_in=tokens_in, tokens_out=tokens_out,
            cost_usd=cost, latency_ms=latency_ms,
            truncated=truncated, truncation_notes=trunc_notes,
        )
        _log_llm_debug(
            call_type="report", resource="daily_report",
            system_prompt=system, user_content=json_payload,
            response_text=raw_text,
        )
        central_push.push_llm_usage({
            "called_at": time.time(),
            "call_type": "report",
            "provider": provider,
            "model": model,
            "resource": "daily_report",
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "cost_usd": cost or 0.0,
            "latency_ms": latency_ms,
            "error": False,
        })
        return AnalysisResult(
            raw_text=raw_text, parsed=parsed, parse_error=parse_error,
            model=model, tokens_in=tokens_in, tokens_out=tokens_out,
            cost_usd=cost, latency_ms=latency_ms,
        )
    except Exception:
        logger.exception("LLM daily report failed")
        _log_llm_call(
            call_type="report", provider=provider, model=model, resource="daily_report",
            context_bytes=context_bytes,
            section_bytes=measured["section_bytes"],
            sections=measured["sections"],
            tokens_in=0, tokens_out=0, cost_usd=None, latency_ms=0,
            truncated=truncated, truncation_notes=trunc_notes, error=True,
        )
        central_push.push_llm_usage({
            "called_at": time.time(),
            "call_type": "report",
            "provider": provider,
            "model": model,
            "resource": "daily_report",
            "tokens_in": 0,
            "tokens_out": 0,
            "cost_usd": 0.0,
            "latency_ms": 0.0,
            "error": True,
        })
        return AnalysisResult(
            raw_text="\u26a0\ufe0f AI daily report unavailable \u2014 LLM API error.",
            parsed=None, parse_error=True, model=model, tokens_in=0, tokens_out=0, cost_usd=None,
        )


_WEEKLY_REPORT_DEFAULTS = {
    "overall_trend": "stable",
    "summary": "",
    "recurring_issues": [],
    "trends": [],
    "recommendations": [],
    "cost_summary": {},
}


def parse_weekly_report(raw: str) -> tuple[dict, bool]:
    """Parse structured JSON from weekly report LLM response."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].startswith("```"):
                end_idx = i
                break
        text = "\n".join(lines[1:end_idx])

    try:
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("Expected JSON object")
        for key, default in _WEEKLY_REPORT_DEFAULTS.items():
            data.setdefault(key, default)
        if data["overall_trend"] not in ("improving", "stable", "degrading"):
            data["overall_trend"] = "stable"
        if not isinstance(data.get("recurring_issues"), list):
            data["recurring_issues"] = []
        if not isinstance(data.get("trends"), list):
            data["trends"] = []
        if not isinstance(data.get("recommendations"), list):
            data["recommendations"] = []
        for rec in data["recommendations"]:
            if isinstance(rec, dict):
                rec.setdefault("action", "")
                rec.setdefault("impact", "")
                try:
                    rec["priority"] = max(1, min(3, int(rec.get("priority", 2))))
                except (ValueError, TypeError):
                    rec["priority"] = 2
        if not isinstance(data.get("cost_summary"), dict):
            data["cost_summary"] = {}
        return data, False
    except (json.JSONDecodeError, ValueError, TypeError):
        return {
            "overall_trend": "stable",
            "summary": raw,
            "recurring_issues": [],
            "trends": [],
            "recommendations": [],
            "cost_summary": {},
            "_parse_error": True,
        }, True


def analyze_weekly_report(context: dict) -> AnalysisResult:
    """Analyze weekly report — accepts structured dict, returns AnalysisResult."""
    from src.engine.sanitizer import sanitize_dict

    context = sanitize_dict(context)
    payload = {"cluster": config.CLUSTER_NAME, "weekly_data": context}
    json_payload = json.dumps(payload, default=str)
    context_bytes = len(json_payload.encode("utf-8"))

    # Progressive trim if over budget (byte-aware, mirrors build_report_payload)
    max_bytes = config.MAX_CONTEXT_BYTES_REPORT
    truncated = False
    weekly = payload.get("weekly_data", {})
    if context_bytes > max_bytes and isinstance(weekly, dict):
        truncated = True
        # 1. Trim incident_details to 20
        if "incident_details" in weekly and len(weekly["incident_details"]) > 20:
            weekly["incident_details"] = weekly["incident_details"][:20]
            json_payload = json.dumps(payload, default=str)
        # 2. Drop incident_details entirely
        if len(json_payload.encode("utf-8")) > max_bytes and "incident_details" in weekly:
            del weekly["incident_details"]
            json_payload = json.dumps(payload, default=str)
        # 3. Trim daily_reports to 3
        if len(json_payload.encode("utf-8")) > max_bytes and "daily_reports" in weekly and len(weekly["daily_reports"]) > 3:
            weekly["daily_reports"] = weekly["daily_reports"][:3]
            json_payload = json.dumps(payload, default=str)
        # 4. Final hard truncation (byte-safe)
        if len(json_payload.encode("utf-8")) > max_bytes:
            json_payload = json_payload.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")
        context_bytes = len(json_payload.encode("utf-8"))

    if not _check_rate_limit():
        logger.warning("Skipped weekly report analysis — rate limit exhausted")
        return AnalysisResult(
            raw_text=f"Rate limit reached ({config.MAX_LLM_CALLS_PER_HOUR}/hr).",
            parsed=None, parse_error=True, model="", tokens_in=0, tokens_out=0, cost_usd=None,
        )

    provider, client = _get_client()
    model = _get_model(provider, tier="report")
    system = WEEKLY_REPORT_SYSTEM_PROMPT.format(cluster_name=config.CLUSTER_NAME)

    logger.info("Sending weekly report to %s (%s), context=%dB%s",
                provider, model, context_bytes, " [TRUNCATED]" if truncated else "")
    try:
        raw_text, tokens_in, tokens_out, latency_ms = _call_llm(
            provider, client, model, system, json_payload, 4096)
        cost = _estimate_cost(model, tokens_in, tokens_out)
        parsed, parse_error = parse_weekly_report(raw_text)
        if parse_error:
            logger.warning("LLM returned non-JSON for weekly report, using raw text fallback")
        _log_llm_call(
            call_type="report", provider=provider, model=model, resource="weekly_report",
            context_bytes=context_bytes, section_bytes=None,
            sections=list(context.keys()) if isinstance(context, dict) else [],
            tokens_in=tokens_in, tokens_out=tokens_out,
            cost_usd=cost, latency_ms=latency_ms,
            truncated=truncated, truncation_notes=["weekly context truncated"] if truncated else [],
        )
        _log_llm_debug(
            call_type="report", resource="weekly_report",
            system_prompt=system, user_content=json_payload,
            response_text=raw_text,
        )
        central_push.push_llm_usage({
            "called_at": time.time(),
            "call_type": "report",
            "provider": provider,
            "model": model,
            "resource": "weekly_report",
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "cost_usd": cost or 0.0,
            "latency_ms": latency_ms,
            "error": False,
        })
        return AnalysisResult(
            raw_text=raw_text, parsed=parsed, parse_error=parse_error,
            model=model, tokens_in=tokens_in, tokens_out=tokens_out,
            cost_usd=cost, latency_ms=latency_ms,
        )
    except Exception:
        logger.exception("LLM weekly report failed")
        _log_llm_call(
            call_type="report", provider=provider, model=model, resource="weekly_report",
            context_bytes=context_bytes, section_bytes=None,
            sections=list(context.keys()) if isinstance(context, dict) else [],
            tokens_in=0, tokens_out=0, cost_usd=None, latency_ms=0,
            truncated=truncated, truncation_notes=[], error=True,
        )
        central_push.push_llm_usage({
            "called_at": time.time(),
            "call_type": "report",
            "provider": provider,
            "model": model,
            "resource": "weekly_report",
            "tokens_in": 0,
            "tokens_out": 0,
            "cost_usd": 0.0,
            "latency_ms": 0.0,
            "error": True,
        })
        return AnalysisResult(
            raw_text="\u26a0\ufe0f AI weekly report unavailable \u2014 LLM API error.",
            parsed=None, parse_error=True, model=model, tokens_in=0, tokens_out=0, cost_usd=None,
        )


INVESTIGATION_SYSTEM_PROMPT = """You are a Kubernetes SRE assistant. You receive multi-source investigation data (K8s API, Prometheus metrics, Elasticsearch logs, Uptrace traces, incident history) for a namespace/pod.

Analyze all data sources and return STRICT JSON only. No text outside JSON.

{{
  "summary": "1-2 sentence overview of current state",
  "timeline": [
    {{"time": "relative or absolute", "event": "what happened"}}
  ],
  "root_cause": "most likely root cause based on all evidence",
  "confidence": 0.0-1.0,
  "affected_services": ["service1", "service2"],
  "correlations": [
    {{"sources": ["logs", "metrics"], "finding": "correlation description"}}
  ],
  "suggested_actions": [
    {{"action": "concrete step", "priority": 1-3}}
  ]
}}

Rules:
- Cross-reference data sources: correlate log errors with metric spikes, trace errors with pod restarts
- timeline: chronological, most recent first
- suggested_actions: priority 1=immediate, 2=short-term, 3=preventive
- confidence: based on how much data was available and how consistent the signals are
- If data sources are missing/empty, note that and lower confidence
- JSON only, no fences

Cluster: {cluster_name}"""

_INVESTIGATION_DEFAULTS = {
    "summary": "",
    "timeline": [],
    "root_cause": "Unknown",
    "confidence": 0.3,
    "affected_services": [],
    "correlations": [],
    "suggested_actions": [],
}


def parse_investigation(raw: str) -> tuple[dict, bool]:
    """Parse structured JSON from investigation LLM response."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].startswith("```"):
                end_idx = i
                break
        text = "\n".join(lines[1:end_idx])

    try:
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("Expected JSON object")
        for key, default in _INVESTIGATION_DEFAULTS.items():
            data.setdefault(key, default)
        try:
            data["confidence"] = max(0.0, min(1.0, float(data.get("confidence", 0.3))))
        except (ValueError, TypeError):
            data["confidence"] = 0.3
        if not isinstance(data.get("timeline"), list):
            data["timeline"] = []
        if not isinstance(data.get("affected_services"), list):
            data["affected_services"] = []
        if not isinstance(data.get("correlations"), list):
            data["correlations"] = []
        if not isinstance(data.get("suggested_actions"), list):
            data["suggested_actions"] = []
        for sa in data["suggested_actions"]:
            if isinstance(sa, dict):
                sa.setdefault("action", "")
                try:
                    sa["priority"] = max(1, min(3, int(sa.get("priority", 2))))
                except (ValueError, TypeError):
                    sa["priority"] = 2
        return data, False
    except (json.JSONDecodeError, ValueError, TypeError):
        return {
            **_INVESTIGATION_DEFAULTS,
            "summary": raw,
            "_parse_error": True,
        }, True


def analyze_investigation(context: dict, namespace: str, since_minutes: int = 30) -> AnalysisResult:
    """Analyze investigation context via LLM. Multi-source data analysis."""
    from src.engine.sanitizer import sanitize_dict

    context = sanitize_dict(context)
    resource = f"investigation:{namespace}"
    payload = {"cluster": config.CLUSTER_NAME, "namespace": namespace,
               "since_minutes": since_minutes, "investigation_data": context}
    json_payload = json.dumps(payload, default=str)
    context_bytes = len(json_payload.encode("utf-8"))

    # Truncate if over budget
    max_bytes = config.MAX_CONTEXT_BYTES_REPORT
    truncated = False
    if context_bytes > max_bytes:
        truncated = True
        json_payload = json_payload.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")
        context_bytes = max_bytes

    if not _check_rate_limit():
        logger.warning("Skipped investigation analysis — rate limit exhausted")
        return AnalysisResult(
            raw_text=f"Rate limit reached ({config.MAX_LLM_CALLS_PER_HOUR}/hr).",
            parsed=None, parse_error=True, model="", tokens_in=0, tokens_out=0, cost_usd=None,
        )

    provider, client = _get_client()
    model = _get_model(provider, tier="report")
    system = INVESTIGATION_SYSTEM_PROMPT.format(cluster_name=config.CLUSTER_NAME)

    logger.info("Investigation analysis via %s (%s), context=%dB%s",
                provider, model, context_bytes, " [TRUNCATED]" if truncated else "")
    try:
        raw_text, tokens_in, tokens_out, latency_ms = _call_llm(
            provider, client, model, system, json_payload, 4096)
        cost = _estimate_cost(model, tokens_in, tokens_out)
        parsed, parse_error = parse_investigation(raw_text)
        if parse_error:
            logger.warning("LLM returned non-JSON for investigation, using raw text fallback")
        _log_llm_call(
            call_type="investigation", provider=provider, model=model, resource=resource,
            context_bytes=context_bytes, section_bytes=None,
            sections=list(context.keys()) if isinstance(context, dict) else [],
            tokens_in=tokens_in, tokens_out=tokens_out,
            cost_usd=cost, latency_ms=latency_ms,
            truncated=truncated, truncation_notes=["investigation context truncated"] if truncated else [],
        )
        _log_llm_debug(
            call_type="investigation", resource=resource,
            system_prompt=system, user_content=json_payload,
            response_text=raw_text,
        )
        return AnalysisResult(
            raw_text=raw_text, parsed=parsed, parse_error=parse_error,
            model=model, tokens_in=tokens_in, tokens_out=tokens_out,
            cost_usd=cost, latency_ms=latency_ms,
        )
    except Exception:
        logger.exception("LLM investigation analysis failed for %s", namespace)
        _log_llm_call(
            call_type="investigation", provider=provider, model=model, resource=resource,
            context_bytes=context_bytes, section_bytes=None,
            sections=list(context.keys()) if isinstance(context, dict) else [],
            tokens_in=0, tokens_out=0, cost_usd=None, latency_ms=0,
            truncated=truncated, truncation_notes=[], error=True,
        )
        return AnalysisResult(
            raw_text="\u26a0\ufe0f AI investigation unavailable \u2014 LLM API error.",
            parsed=None, parse_error=True, model=model, tokens_in=0, tokens_out=0, cost_usd=None,
        )
