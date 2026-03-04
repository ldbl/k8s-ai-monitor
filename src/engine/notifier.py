"""Slack posting — extracted from slack.py."""
import json
import logging

import requests

from src import config

logger = logging.getLogger(__name__)

SEVERITY_COLORS = {
    "critical": "#FF0000",
    "warning": "#FFA500",
    "info": "#36A64F",
}

_SEVERITY_ICON = {"critical": "\033[1;31m\U0001f534 CRITICAL", "warning": "\033[33m\U0001f7e1 WARNING", "info": "\033[32m\U0001f7e2 INFO"}
_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[90m"
_CYAN = "\033[36m"


def format_structured_analysis(analysis: dict) -> str:
    """Format structured analysis dict for Slack mrkdwn."""
    if analysis.get("_parse_error"):
        return analysis.get("root_cause") or "Analysis unavailable"

    conf = analysis.get("confidence", 0)
    conf_icon = "\U0001f7e2" if conf >= 0.8 else "\U0001f7e1" if conf >= 0.5 else "\U0001f534"

    lines = [
        f"*Root Cause* {conf_icon} _{conf*100:.0f}% confidence_",
        analysis.get("root_cause", "Unknown"), "",
        "*Impact*", analysis.get("impact", "Unknown"), "",
    ]

    # New schema: hypotheses
    if analysis.get("hypotheses"):
        lines.append("*Hypotheses*")
        for i, h in enumerate(analysis["hypotheses"], 1):
            ev = ", ".join(h.get("evidence", [])[:3])
            lines.append(f"{i}. {h.get('cause', '?')} _({h.get('confidence', 0)*100:.0f}%)_ \u2014 {ev}")
        lines.append("")

    # New schema: suggested_actions with priority icons
    if analysis.get("suggested_actions"):
        lines.append("*Suggested Actions*")
        icons = {1: "\U0001f534", 2: "\U0001f7e1", 3: "\U0001f7e2"}
        for sa in analysis["suggested_actions"]:
            lines.append(f"{icons.get(sa.get('priority', 2), '\U0001f7e1')} {sa.get('action', '?')}")
        lines.append("")

    # Backward compat: old schema
    elif analysis.get("action_plan"):
        lines.append("*Action Plan*")
        for i, step in enumerate(analysis["action_plan"], 1):
            lines.append(f"{i}. {step}")
        lines.append("")
    if not analysis.get("hypotheses") and analysis.get("evidence"):
        lines.append("*Evidence*")
        for ev in analysis["evidence"]:
            lines.append(f"\u2022 {ev}")
        lines.append("")

    if analysis.get("human_needed"):
        lines.append("\u26a0\ufe0f _Human review recommended_")
    return "\n".join(lines)


def _console_alert(title: str, analysis: str, severity: str, resource: str, namespace: str,
                   node: str = "", node_metrics: str = "", app_metrics: str = ""):
    icon = _SEVERITY_ICON.get(severity, _SEVERITY_ICON["info"])
    width = 72
    print(f"\n{_DIM}{'\u2501' * width}{_RESET}")
    print(f"  {icon} {_BOLD}{title}{_RESET}")
    node_info = f"  {_CYAN}Node:{_RESET} {node}" if node else ""
    print(f"  {_CYAN}Cluster:{_RESET} {config.CLUSTER_NAME}  {_CYAN}Namespace:{_RESET} {namespace}  {_CYAN}Resource:{_RESET} {resource}{node_info}")
    if node_metrics:
        print(f"  {_CYAN}Metrics:{_RESET} {node_metrics}")
    if app_metrics:
        print(f"  {_CYAN}App:{_RESET}     {app_metrics}")
    print(f"{_DIM}{'\u2500' * width}{_RESET}")
    print(analysis)
    print(f"{_DIM}{'\u2501' * width}{_RESET}\n")


def get_webhook_for_namespace(namespace: str) -> str | None:
    if config.is_nonprod_namespace(namespace):
        return config.SLACK_WEBHOOK_URL_NONPROD or None
    return config.SLACK_WEBHOOK_URL or None


def post_alert(title: str, analysis: str, severity: str, resource: str, namespace: str,
               event_reason: str = "", node: str = "", node_metrics: str = "",
               app_metrics: str = "", model: str = "", webhook_url: str | None = None):
    if not (webhook_url or config.SLACK_WEBHOOK_URL):
        _console_alert(title, analysis, severity, resource, namespace, node=node,
                       node_metrics=node_metrics, app_metrics=app_metrics)
        return

    color = SEVERITY_COLORS.get(severity, SEVERITY_COLORS["info"])
    fields = [
        {"type": "mrkdwn", "text": f"*Cluster:*\n{config.CLUSTER_NAME}"},
        {"type": "mrkdwn", "text": f"*Namespace:*\n{namespace}"},
        {"type": "mrkdwn", "text": f"*Resource:*\n{resource}"},
        {"type": "mrkdwn", "text": f"*Severity:*\n{severity.upper()}"},
    ]
    if node:
        fields.append({"type": "mrkdwn", "text": f"*Node:*\n{node}"})
    if event_reason:
        fields.append({"type": "mrkdwn", "text": f"*Event:*\n`{event_reason}`"})

    icon = '\U0001f534' if severity == 'critical' else '\U0001f7e1' if severity == 'warning' else '\U0001f7e2'
    header_text = f"{icon} [{config.CLUSTER_NAME}/{namespace}] {title}"
    if len(header_text) > 150:
        header_text = header_text[:147] + "..."
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text},
        },
        {
            "type": "section",
            "fields": fields,
        },
    ]
    if node_metrics:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"\U0001f4ca *Node Metrics:* {node_metrics}"}],
        })
    if app_metrics:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"\U0001f4c8 *App Metrics:* {app_metrics}"}],
        })
    if model:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"\U0001f916 *Model:* {model}"}],
        })
    blocks.append({"type": "divider"})
    # Reserve block budget: ~7 blocks already used for header/fields/context/divider
    blocks.extend(_split_text_blocks(analysis, max_blocks=40))

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": blocks,
            }
        ]
    }
    _send(payload, webhook_url=webhook_url)


_STATUS_ICON = {"healthy": "\U0001f7e2", "degraded": "\U0001f7e1", "critical": "\U0001f534"}
_STATUS_COLOR = {"healthy": "#36A64F", "degraded": "#FFA500", "critical": "#FF0000"}
_SEVERITY_ICON_SLACK = {"critical": "\U0001f534", "warning": "\U0001f7e1", "info": "\U0001f7e2"}
_PRIORITY_ICON = {1: "\U0001f534", 2: "\U0001f7e1", 3: "\U0001f7e2"}


def format_daily_report(parsed: dict) -> list[dict]:
    """Format parsed daily report dict into Slack Block Kit blocks."""
    # Daily report: header(1) + divider(1) + up to 4 sections with dividers
    # Budget ~10 blocks per section to stay under 50 total
    section_budget = 10

    if parsed.get("_parse_error"):
        return _split_text_blocks(parsed.get("summary", "Analysis unavailable"), max_blocks=section_budget)

    blocks = []

    # Status + summary
    status = parsed.get("overall_status", "degraded")
    icon = _STATUS_ICON.get(status, "\U0001f7e1")
    summary = parsed.get("summary", "")
    status_text = f"{icon} *Status: {status.upper()}*"
    if summary:
        status_text += f"\n{summary}"
    blocks.extend(_split_text_blocks(status_text, max_blocks=section_budget))

    # Issues
    issues = parsed.get("issues", [])
    if issues:
        lines = ["*Issues*"]
        for issue in issues:
            sev = issue.get("severity", "info")
            sev_icon = _SEVERITY_ICON_SLACK.get(sev, "\U0001f7e2")
            desc = issue.get("description", "")
            affected = issue.get("affected", "")
            line = f"{sev_icon} {desc}"
            if affected:
                line += f" \u2014 _{affected}_"
            lines.append(line)
        blocks.append({"type": "divider"})
        blocks.extend(_split_text_blocks("\n".join(lines), max_blocks=section_budget))

    # Trends
    trends = parsed.get("trends", [])
    if trends:
        lines = ["*Trends*"]
        for trend in trends:
            desc = trend.get("description", str(trend)) if isinstance(trend, dict) else str(trend)
            lines.append(f"\u2022 {desc}")
        blocks.append({"type": "divider"})
        blocks.extend(_split_text_blocks("\n".join(lines), max_blocks=section_budget))

    # Recommendations
    recs = parsed.get("recommendations", [])
    if recs:
        lines = ["*Recommendations*"]
        for rec in recs:
            p = rec.get("priority", 2) if isinstance(rec, dict) else 2
            action = rec.get("action", str(rec)) if isinstance(rec, dict) else str(rec)
            icon = _PRIORITY_ICON.get(p, "\U0001f7e1")
            lines.append(f"{icon} {action}")
        blocks.append({"type": "divider"})
        blocks.extend(_split_text_blocks("\n".join(lines), max_blocks=section_budget))

    return blocks


_TREND_ICON = {"improving": "\U0001f4c8", "stable": "\u2796", "degrading": "\U0001f4c9"}
_TREND_COLOR = {"improving": "#36A64F", "stable": "#FFA500", "degrading": "#FF0000"}
_DIRECTION_ICON = {"improving": "\U0001f4c8", "worsening": "\U0001f4c9", "stable": "\u2796"}


def format_weekly_report(parsed: dict) -> list[dict]:
    """Format parsed weekly report dict into Slack Block Kit blocks."""
    section_budget = 10

    if parsed.get("_parse_error"):
        return _split_text_blocks(parsed.get("summary", "Analysis unavailable"), max_blocks=section_budget)

    blocks: list[dict] = []

    # Overall trend + summary
    trend = parsed.get("overall_trend", "stable")
    icon = _TREND_ICON.get(trend, "\u2796")
    summary = parsed.get("summary", "")
    status_text = f"{icon} *Trend: {trend.upper()}*"
    if summary:
        status_text += f"\n{summary}"
    blocks.extend(_split_text_blocks(status_text, max_blocks=section_budget))

    # Recurring issues
    recurring = parsed.get("recurring_issues", [])
    if recurring:
        lines = ["*Recurring Issues*"]
        for issue in recurring:
            desc = issue.get("description", "") if isinstance(issue, dict) else str(issue)
            freq = issue.get("frequency", "?") if isinstance(issue, dict) else "?"
            services = issue.get("services_affected", []) if isinstance(issue, dict) else []
            line = f"\U0001f504 {desc} \u2014 _{freq}x_"
            if services:
                line += f" ({', '.join(services[:5])})"
            lines.append(line)
        blocks.append({"type": "divider"})
        blocks.extend(_split_text_blocks("\n".join(lines), max_blocks=section_budget))

    # Trends
    trends = parsed.get("trends", [])
    if trends:
        lines = ["*Trends*"]
        for t in trends:
            if isinstance(t, dict):
                desc = t.get("description", "")
                direction = t.get("direction", "stable")
                d_icon = _DIRECTION_ICON.get(direction, "\u2796")
                lines.append(f"{d_icon} {desc}")
            else:
                lines.append(f"\u2022 {t}")
        blocks.append({"type": "divider"})
        blocks.extend(_split_text_blocks("\n".join(lines), max_blocks=section_budget))

    # Recommendations
    recs = parsed.get("recommendations", [])
    if recs:
        lines = ["*Recommendations*"]
        for rec in recs:
            p = rec.get("priority", 2) if isinstance(rec, dict) else 2
            action = rec.get("action", str(rec)) if isinstance(rec, dict) else str(rec)
            impact = rec.get("impact", "") if isinstance(rec, dict) else ""
            p_icon = _PRIORITY_ICON.get(p, "\U0001f7e1")
            line = f"{p_icon} {action}"
            if impact:
                line += f" \u2014 _{impact}_"
            lines.append(line)
        blocks.append({"type": "divider"})
        blocks.extend(_split_text_blocks("\n".join(lines), max_blocks=section_budget))

    # Cost summary
    cost = parsed.get("cost_summary", {})
    if cost:
        cost_text = (
            f"*Cost Summary*\n"
            f"\u2022 LLM cost: ${cost.get('total_llm_cost_usd', 0):.4f}\n"
            f"\u2022 Total alerts: {cost.get('total_alerts', 0)}\n"
            f"\u2022 Total reports: {cost.get('total_reports', 0)}"
        )
        blocks.append({"type": "divider"})
        blocks.extend(_split_text_blocks(cost_text, max_blocks=3))

    return blocks


def post_weekly_report(result):
    """Post weekly report to Slack (or console)."""
    parsed = result.parsed if result.parsed else {
        "_parse_error": True,
        "summary": result.raw_text,
    }
    trend = parsed.get("overall_trend", "stable")
    color = _TREND_COLOR.get(trend, _TREND_COLOR["stable"])

    if not config.SLACK_WEBHOOK_URL:
        width = 72
        print(f"\n{_DIM}{'\u2501' * width}{_RESET}")
        print(f"  \033[34m\U0001f4ca {_BOLD}Weekly Report \u2014 {config.CLUSTER_NAME}{_RESET}")
        print(f"{_DIM}{'\u2500' * width}{_RESET}")
        trend_icon = _TREND_ICON.get(trend, "?")
        print(f"  Trend: {trend_icon} {trend.upper()}")
        if parsed.get("summary"):
            print(f"  {parsed['summary']}")
        for issue in parsed.get("recurring_issues", []):
            if isinstance(issue, dict):
                print(f"  [RECURRING] {issue.get('description', '')}")
        for t in parsed.get("trends", []):
            desc = t.get("description", str(t)) if isinstance(t, dict) else str(t)
            print(f"  - {desc}")
        for rec in parsed.get("recommendations", []):
            action = rec.get("action", str(rec)) if isinstance(rec, dict) else str(rec)
            print(f"  > {action}")
        print(f"{_DIM}{'\u2501' * width}{_RESET}\n")
        return

    report_blocks = format_weekly_report(parsed)
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"\U0001f4ca Weekly Report \u2014 {config.CLUSTER_NAME}"},
        },
        {"type": "divider"},
    ] + report_blocks
    if result.model:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"\U0001f916 *Model:* {result.model}"}],
        })

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": blocks,
            }
        ]
    }
    _send(payload)


def post_daily_report(result):
    # Backward compat: if plain string, use old path
    if isinstance(result, str):
        _post_daily_report_str(result)
        return

    # AnalysisResult path
    parsed = result.parsed if result.parsed else {
        "_parse_error": True,
        "summary": result.raw_text,
    }
    status = parsed.get("overall_status", "degraded")
    color = _STATUS_COLOR.get(status, _STATUS_COLOR["degraded"])

    if not config.SLACK_WEBHOOK_URL:
        width = 72
        print(f"\n{_DIM}{'\u2501' * width}{_RESET}")
        print(f"  \033[34m\U0001f4ca {_BOLD}Daily Report \u2014 {config.CLUSTER_NAME}{_RESET}")
        print(f"{_DIM}{'\u2500' * width}{_RESET}")
        icon = _STATUS_ICON.get(status, "?")
        print(f"  Status: {icon} {status.upper()}")
        if parsed.get("summary"):
            print(f"  {parsed['summary']}")
        for issue in parsed.get("issues", []):
            sev = issue.get("severity", "info")
            print(f"  [{sev.upper()}] {issue.get('description', '')}")
        for trend in parsed.get("trends", []):
            desc = trend.get("description", str(trend)) if isinstance(trend, dict) else str(trend)
            print(f"  - {desc}")
        for rec in parsed.get("recommendations", []):
            action = rec.get("action", str(rec)) if isinstance(rec, dict) else str(rec)
            print(f"  > {action}")
        print(f"{_DIM}{'\u2501' * width}{_RESET}\n")
        return

    report_blocks = format_daily_report(parsed)
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"\U0001f4ca Daily Report \u2014 {config.CLUSTER_NAME}"},
        },
        {"type": "divider"},
    ] + report_blocks
    if result.model:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"\U0001f916 *Model:* {result.model}"}],
        })

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": blocks,
            }
        ]
    }
    _send(payload)


def _post_daily_report_str(report: str):
    """Legacy path for plain string reports."""
    if not config.SLACK_WEBHOOK_URL:
        width = 72
        print(f"\n{_DIM}{'\u2501' * width}{_RESET}")
        print(f"  \033[34m\U0001f4ca {_BOLD}Daily Report \u2014 {config.CLUSTER_NAME}{_RESET}")
        print(f"{_DIM}{'\u2500' * width}{_RESET}")
        print(report)
        print(f"{_DIM}{'\u2501' * width}{_RESET}\n")
        return

    payload = {
        "attachments": [
            {
                "color": SEVERITY_COLORS["info"],
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f"\U0001f4ca Daily Report \u2014 {config.CLUSTER_NAME}"},
                    },
                    {"type": "divider"},
                ] + _split_text_blocks(report),
            }
        ]
    }
    _send(payload)


def post_maintenance_notice(activated: bool, reason: str, duration_hours: float = 0) -> None:
    """Post a maintenance mode start/end notice to Slack."""
    if activated:
        icon = "\U0001f6e0\ufe0f"
        title = f"{icon} Maintenance Mode Activated — {config.CLUSTER_NAME}"
        text = f"*Reason:* {reason}\n*Duration:* {duration_hours:.1f}h\nLLM analysis will be skipped. Alerts will still be posted with `[Maintenance]` prefix."
        color = "#FFA500"
    else:
        icon = "\u2705"
        title = f"{icon} Maintenance Mode Ended — {config.CLUSTER_NAME}"
        text = f"*Reason:* {reason}\nNormal alerting resumed."
        color = "#36A64F"

    if not config.SLACK_WEBHOOK_URL:
        print(f"\n  {title}\n  {text}\n")
        return

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": title},
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": text},
                    },
                ],
            }
        ]
    }
    _send(payload)


def post_resolved(state_key: str, namespace: str, resource: str, duration_min: float,
                   last_seen_at: float | None = None, webhook_url: str | None = None):
    """Post a green 'resolved' message to Slack for critical incidents."""
    if not (webhook_url or config.SLACK_WEBHOOK_URL):
        print(f"  \033[32m\u2705 RESOLVED: {state_key} (after {duration_min:.0f}m)\033[0m")
        return

    fields = [
        {"type": "mrkdwn", "text": f"*Resource:*\n{resource}"},
        {"type": "mrkdwn", "text": f"*Recovery time:*\n{duration_min:.0f} min"},
    ]
    if last_seen_at is not None:
        from datetime import datetime, timezone
        last_fail = datetime.fromtimestamp(last_seen_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        fields.append({"type": "mrkdwn", "text": f"*Last failure:*\n{last_fail}"})

    payload = {
        "attachments": [
            {
                "color": "#36A64F",
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f"\u2705 [{config.CLUSTER_NAME}/{namespace}] Resolved"},
                    },
                    {
                        "type": "section",
                        "fields": fields,
                    },
                ],
            }
        ]
    }
    _send(payload, webhook_url=webhook_url)


def _split_text_blocks(text: str, max_len: int = 2900, max_blocks: int = 45) -> list[dict]:
    """Split long text into multiple Slack section blocks at paragraph boundaries.

    Enforces max_blocks to stay within Slack's 50-block-per-attachment limit
    (leaving room for header, fields, context, dividers added by callers).
    """
    if not text or not text.strip():
        text = "Analysis unavailable"
    # Remove null bytes and control chars (except newline/tab) that break Slack
    text = "".join(c for c in text if c in ('\n', '\t') or (ord(c) >= 32))
    if len(text) <= max_len:
        return [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]

    blocks = []
    remaining = text
    while remaining:
        if len(blocks) >= max_blocks - 1 and len(remaining) > max_len:
            # Last allowed block — truncate remaining
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": remaining[:max_len - 20] + "\n\n…(truncated)"}})
            break
        if len(remaining) <= max_len:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": remaining}})
            break
        # Find split point at paragraph boundary
        split_at = remaining.rfind("\n\n", 0, max_len)
        if split_at <= 0:
            split_at = remaining.rfind("\n", 0, max_len)
        if split_at <= 0:
            split_at = max_len
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": remaining[:split_at]}})
        remaining = remaining[split_at:].lstrip("\n")
    return blocks


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _send(payload: dict, webhook_url: str | None = None):
    url = webhook_url or config.SLACK_WEBHOOK_URL
    if not url:
        return
    try:
        resp = requests.post(
            url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.error("Slack webhook returned %s: %s", resp.status_code, resp.text)
            # Retry with simplified plain-text fallback
            if resp.status_code == 400:
                _send_fallback(payload, webhook_url=webhook_url)
    except Exception:
        logger.exception("Failed to send Slack message")


def _send_fallback(original_payload: dict, webhook_url: str | None = None):
    """Send a simplified plain-text message when Block Kit payload fails."""
    url = webhook_url or config.SLACK_WEBHOOK_URL
    if not url:
        return
    try:
        # Extract text content from blocks
        parts = []
        for att in original_payload.get("attachments", []):
            for block in att.get("blocks", []):
                if block.get("type") == "header":
                    parts.append(block["text"]["text"])
                elif block.get("type") == "section":
                    if "text" in block:
                        parts.append(block["text"].get("text", ""))
                    for f in block.get("fields", []):
                        parts.append(f.get("text", ""))
        text = "\n".join(p for p in parts if p)
        if len(text) > 3900:
            text = text[:3900] + "\n…(truncated)"
        fallback_payload = {"text": text}
        resp = requests.post(
            url,
            data=json.dumps(fallback_payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info("Slack fallback (plain text) sent successfully")
        else:
            logger.error("Slack fallback also failed: %s: %s", resp.status_code, resp.text)
    except Exception:
        logger.exception("Slack fallback send failed")
