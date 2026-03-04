"""Warning K8s event handler — extracted from handlers.py."""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

import kopf
from kubernetes import client as k8s

from src import config
from src.engine.constants import IMPORTANT_EVENT_REASONS, PROBLEM_ALIASES, CONTAINER_STATUS_REASONS
from src.engine.owner import resolve_owner_key_by_name
from src.engine.llm import analyze_alert
from src.engine.notifier import post_alert, format_structured_analysis, get_webhook_for_namespace
from src.collectors import Collector
from src.collectors.node import get_node_metrics_summary
from src.collectors.app_metrics import get_app_metrics_summary

logger = logging.getLogger(__name__)

_startup_time = time.time()
_STARTUP_GRACE_SECONDS = 30

# In-flight state keys to prevent duplicate concurrent alerts
_in_flight: set[str] = set()

_collector: Collector | None = None

# Batching for all pod Warning events (groups by namespace, flushes after window)
_pod_event_batch: dict[str, list] = {}   # ns -> [(pod_name, owner_key, state_key, node, display_reason)]
_pod_event_batch_lock = asyncio.Lock()
_pod_event_batch_task: dict[str, asyncio.Task] = {}  # ns -> pending flush task
_BATCH_WINDOW_SECONDS = 30
_BATCH_THRESHOLD = 3  # min events to trigger batch mode

# Flux resource kinds — source-level (informational only, skip) and deployment-level (grace period)
_FLUX_SOURCE_KINDS = {"HelmRepository", "HelmChart", "GitRepository", "OCIRepository"}
_FLUX_KINDS = {"HelmRelease", "Kustomization"}

# Transient infra reasons: CNI/sandbox errors that often self-resolve within seconds
_TRANSIENT_INFRA_REASONS = {
    "FailedCreatePodSandBox", "FailedCreatePodContainer",
    "NetworkNotReady", "FailedSync",
}
_TRANSIENT_GRACE_SECONDS = 90


def _get_collector() -> Collector:
    global _collector
    if _collector is None:
        _collector = Collector()
    return _collector


def _infer_reason_from_message(event_reason: str, message: str) -> str:
    msg = message.lower()
    if event_reason == "BackOff":
        if "pulling image" in msg or "image" in msg:
            return "ImagePullBackOff"
        if "restarting failed container" in msg:
            return "CrashLoopBackOff"
    elif event_reason == "Failed":
        if "failed to pull image" in msg or "pull image" in msg:
            return "ErrImagePull"
        if "not found" in msg and "configmap" in msg:
            return "CreateContainerConfigError"
        if "not found" in msg and "secret" in msg:
            return "CreateContainerConfigError"
        if "failed to create" in msg:
            return "CreateContainerError"
    return event_reason


def _get_pod_status_reason(namespace: str, pod_name: str) -> tuple[str, str]:
    try:
        core = k8s.CoreV1Api()
        pod = core.read_namespaced_pod(pod_name, namespace)
        node = pod.spec.node_name or ""

        for cs in (pod.status.container_statuses or []):
            if cs.state and cs.state.waiting and cs.state.waiting.reason in CONTAINER_STATUS_REASONS:
                return cs.state.waiting.reason, node
            if cs.state and cs.state.terminated and cs.state.terminated.reason in CONTAINER_STATUS_REASONS:
                return cs.state.terminated.reason, node
            if cs.last_state and cs.last_state.terminated and cs.last_state.terminated.reason == "OOMKilled":
                return "OOMKilled", node

        return "", node
    except Exception:
        return "", ""


def _is_pod_still_pending(namespace: str, pod_name: str) -> bool:
    try:
        core = k8s.CoreV1Api()
        pod = core.read_namespaced_pod(pod_name, namespace)
        return pod.status.phase == "Pending"
    except k8s.ApiException as e:
        if e.status == 404:
            return False
        return True
    except Exception:
        logger.error("_is_pod_still_pending failed for %s/%s", namespace, pod_name, exc_info=True)
        return True


def _pod_exists(namespace: str, pod_name: str) -> bool:
    """Return True only if pod exists and is NOT being terminated."""
    try:
        core = k8s.CoreV1Api()
        pod = core.read_namespaced_pod(pod_name, namespace)
        if pod.metadata.deletion_timestamp is not None:
            return False  # Terminating — treat as gone
        return True
    except k8s.ApiException as e:
        if e.status == 404:
            return False
        return True
    except Exception:
        logger.error("_pod_exists failed for %s/%s", namespace, pod_name, exc_info=True)
        return True


_MIN_UNHEALTHY_GRACE = 30
_MAX_UNHEALTHY_GRACE = 600


def _get_probe_grace_seconds(namespace: str, pod_name: str) -> tuple[int, bool]:
    """Calculate grace period from pod probe config.

    Returns (grace_seconds, pod_is_young) where grace_seconds is the max
    startup budget across all containers/probes, and pod_is_young indicates
    whether the pod was created within that window.
    """
    try:
        core = k8s.CoreV1Api()
        pod = core.read_namespaced_pod(pod_name, namespace)
    except Exception:
        return _MIN_UNHEALTHY_GRACE, False

    # Calculate max probe startup budget across all containers
    max_budget = _MIN_UNHEALTHY_GRACE
    for c in (pod.spec.containers or []):
        if c.startup_probe:
            p = c.startup_probe
            budget = (p.initial_delay_seconds or 0) + \
                     (p.failure_threshold or 3) * (p.period_seconds or 10)
            max_budget = max(max_budget, budget)
        else:
            # No startup probe: liveness/readiness start immediately
            for probe in (c.liveness_probe, c.readiness_probe):
                if probe:
                    budget = (probe.initial_delay_seconds or 0) + \
                             (probe.failure_threshold or 3) * (probe.period_seconds or 10)
                    max_budget = max(max_budget, budget)

    grace = min(max_budget + 10, _MAX_UNHEALTHY_GRACE)  # +10s buffer, cap at 10 min

    # Check if pod is within the grace window
    created = pod.metadata.creation_timestamp
    if created:
        if hasattr(created, 'tzinfo') and created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - created).total_seconds()
        return grace, age <= grace

    return grace, False


def _is_pod_ready_now(namespace: str, pod_name: str) -> bool:
    """Return True if all containers in the pod are Ready."""
    try:
        core = k8s.CoreV1Api()
        pod = core.read_namespaced_pod(pod_name, namespace)
        statuses = pod.status.container_statuses if pod.status else None
        if not statuses:
            return False
        return all(cs.ready for cs in statuses)
    except Exception:
        return False


def _is_node_not_ready(node_name: str) -> bool:
    """Return True if node exists and has Ready condition != True."""
    try:
        core = k8s.CoreV1Api()
        node = core.read_node(node_name)
        for cond in (node.status.conditions or []):
            if cond.type == "Ready":
                return cond.status != "True"
        return True  # no Ready condition found — treat as not ready
    except Exception:
        return False  # node gone or API error — don't block alert


def _is_node_being_deleted(node_name: str) -> tuple[bool, bool, bool]:
    """Check node state. Returns (exists, is_ready, marked_for_deletion)."""
    try:
        core = k8s.CoreV1Api()
        node = core.read_node(node_name)
        is_ready = True
        for cond in (node.status.conditions or []):
            if cond.type == "Ready":
                is_ready = cond.status == "True"
                break
        marked = False
        for taint in (node.spec.taints or []):
            if taint.key == "ToBeDeletedByClusterAutoscaler":
                marked = True
                break
        if node.metadata.deletion_timestamp is not None:
            marked = True
        return True, is_ready, marked
    except k8s.ApiException as e:
        if e.status == 404:
            return False, False, True  # node gone
        return True, False, False  # API error, assume exists
    except Exception:
        return True, False, False


def _is_autoscaler_scaling_up() -> bool:
    """Return True if cluster autoscaler is actively scaling up.

    Checks two signals:
    - Recent scale-up events in kube-system (last 10 min)
    - New NotReady nodes younger than 10 min (being provisioned)
    """
    core = k8s.CoreV1Api()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=10)
    scale_up_reasons = {"TriggeredScaleUp", "ScaledUpGroup", "ScaleUp"}

    # Check recent scale-up events in kube-system
    try:
        events = core.list_namespaced_event("kube-system")
        for ev in events.items:
            if ev.reason in scale_up_reasons:
                ts = ev.last_timestamp or ev.metadata.creation_timestamp
                if ts and ts >= cutoff:
                    return True
    except Exception:
        logger.warning("Failed to check scale-up events in kube-system", exc_info=True)

    # Check for new NotReady nodes (being provisioned)
    try:
        nodes = core.list_node()
        for node in nodes.items:
            age = (now - node.metadata.creation_timestamp).total_seconds()
            if age < 600:  # younger than 10 min
                is_ready = False
                for cond in (node.status.conditions or []):
                    if cond.type == "Ready":
                        is_ready = cond.status == "True"
                        break
                if not is_ready:
                    return True
    except Exception:
        logger.warning("Failed to check new node status for scale-up detection", exc_info=True)

    return False


def _in_watched_namespace(namespace: str) -> bool:
    if namespace in config.EXCLUDE_NAMESPACES:
        return False
    if config.is_nonprod_namespace(namespace):
        return True
    if config.WATCH_ALL_NAMESPACES:
        return True
    return namespace in config.NAMESPACES


def _make_state_key(owner_key: str, reason: str) -> str:
    alias = PROBLEM_ALIASES.get(reason, reason.lower())
    return f"{owner_key}:{alias}"


async def _analyze_and_alert(title: str, resource: str, namespace: str,
                              context_fn, severity: str, event_reason: str = "",
                              node: str = "", pod_name: str = "",
                              maintenance: bool = False):
    loop = asyncio.get_running_loop()
    if maintenance:
        analysis_text = "LLM analysis skipped — maintenance mode active"
        title = f"[Maintenance] {title}"
        model = ""
    elif config.is_nonprod_namespace(namespace):
        analysis_text = title
        title = f"{config.nonprod_title_prefix(namespace)} {title}"
        model = ""
    else:
        context = await loop.run_in_executor(None, context_fn)
        res_label = f"{namespace}/{resource}"
        ar = await loop.run_in_executor(None, analyze_alert, context, res_label)
        analysis_text = format_structured_analysis(ar.parsed) if ar.parsed and not ar.parse_error else ar.raw_text
        if not analysis_text or not analysis_text.strip():
            analysis_text = "Analysis unavailable"
        model = ar.model
    # Log analysis so it's available even if Slack fails
    logger.info("Analysis for %s/%s: %s", namespace, resource, analysis_text[:300])
    node_metrics = ""
    if node:
        node_metrics = await loop.run_in_executor(None, get_node_metrics_summary, node)
    app_metrics = ""
    if pod_name:
        app_metrics = await loop.run_in_executor(None, get_app_metrics_summary, pod_name, namespace)
    webhook_url = get_webhook_for_namespace(namespace)
    await loop.run_in_executor(
        None, lambda: post_alert(title, analysis_text, severity, resource, namespace, event_reason, node, node_metrics,
                                 app_metrics, model, webhook_url=webhook_url),
    )


async def _flush_pod_event_batch(ns: str, **_kwargs):
    """Process accumulated pod Warning events for a namespace after the batch window."""
    try:
        await asyncio.sleep(_BATCH_WINDOW_SECONDS)
    except asyncio.CancelledError:
        # Clean up stale state so future flushes aren't blocked
        async with _pod_event_batch_lock:
            _pod_event_batch.pop(ns, None)
            current = _pod_event_batch_task.get(ns)
            if current is asyncio.current_task():
                _pod_event_batch_task.pop(ns, None)
        raise

    async with _pod_event_batch_lock:
        events = _pod_event_batch.pop(ns, [])
        current = _pod_event_batch_task.get(ns)
        if current is asyncio.current_task():
            _pod_event_batch_task.pop(ns, None)

    if not events:
        return

    loop = asyncio.get_running_loop()
    from src.handlers.startup import get_store
    store = get_store()

    # Read maintenance state NOW (not from snapshot at scheduling time)
    maintenance = await loop.run_in_executor(None, store.is_maintenance_active)

    # Filter: skip recovered pods and already-seen state_keys
    pending = []
    batch_seen: set[tuple[str, str]] = set()
    for pod_name, owner_key, state_key, node, display_reason in events:
        dedup_key = (owner_key, display_reason)
        if dedup_key in batch_seen:
            continue
        ready = await loop.run_in_executor(None, _is_pod_ready_now, ns, pod_name)
        if ready:
            continue
        exists = await loop.run_in_executor(None, _pod_exists, ns, pod_name)
        if not exists:
            continue
        if await loop.run_in_executor(None, store.is_seen, state_key):
            continue
        alias = PROBLEM_ALIASES.get(display_reason, display_reason.lower())
        if await loop.run_in_executor(None, store.is_suppressed, state_key, ns, alias):
            continue
        batch_seen.add(dedup_key)
        pending.append((pod_name, owner_key, state_key, node, display_reason))

    if not pending:
        return

    if len(pending) < _BATCH_THRESHOLD:
        # Few events — process individually (existing flow)
        for pod_name, owner_key, state_key, node, display_reason in pending:
            collector = _get_collector()
            alias = PROBLEM_ALIASES.get(display_reason, display_reason.lower())
            sev = "critical" if display_reason == "OOMKilled" else "warning"
            _dm = config.NON_PROD_DEBOUNCE_MULTIPLIER if config.is_nonprod_namespace(ns) else 1
            await loop.run_in_executor(
                None, lambda sk=state_key, a=alias, s=sev, dm=_dm: store.mark_seen(sk, issue_type=a, severity=s, debounce_multiplier=dm),
            )
            context_fn = lambda n=pod_name, nsp=ns, a=alias: collector.collect_pod_context_with_diagnostics(n, nsp, a)
            await _analyze_and_alert(
                title=f"Warning: {display_reason}",
                resource=f"Pod/{pod_name}",
                namespace=ns,
                context_fn=context_fn,
                severity="critical" if display_reason == "OOMKilled" else "warning",
                event_reason=display_reason,
                node=node,
                pod_name=pod_name,
                maintenance=maintenance,
            )
        return

    # Batch mode: single combined alert
    _dm = config.NON_PROD_DEBOUNCE_MULTIPLIER if config.is_nonprod_namespace(ns) else 1
    for _, _, state_key, _, dr in pending:
        alias = PROBLEM_ALIASES.get(dr, dr.lower())
        sev = "critical" if dr == "OOMKilled" else "warning"
        await loop.run_in_executor(
            None, lambda sk=state_key, a=alias, s=sev, dm=_dm: store.mark_seen(sk, issue_type=a, severity=s, debounce_multiplier=dm),
        )

    # Build combined context
    collector = _get_collector()
    svc_names = [owner_key.split("/")[-1] for _, owner_key, _, _, _ in pending]
    reasons = sorted({dr for _, _, _, _, dr in pending})
    nodes = {p[3] for p in pending if p[3]}

    reasons_str = ", ".join(reasons)
    context_lines = [f"Multiple pod issues detected: {len(pending)} events in {ns}"]
    context_lines.append(f"Affected: {', '.join(svc_names)}")
    context_lines.append(f"Reasons: {reasons_str}")
    node_name = ""
    node_metrics = ""
    for n in sorted(nodes):
        nm = await loop.run_in_executor(None, get_node_metrics_summary, n)
        if nm:
            context_lines.append(f"Node ({n}): {nm}")
            if not node_name:
                node_name = n
                node_metrics = nm
    context_lines.append("Multiple warnings fired within a short window — likely related to the same underlying issue or a rolling update.")
    context = "\n".join(context_lines)

    batch_title = f"Warning: {len(pending)} pod issues — {reasons_str}"
    if maintenance:
        analysis_text = "LLM analysis skipped — maintenance mode active"
        batch_title = f"[Maintenance] {batch_title}"
    elif config.is_nonprod_namespace(ns):
        analysis_text = context
        batch_title = f"{config.nonprod_title_prefix(ns)} {batch_title}"
    else:
        res_label = f"{ns}/BatchAlert({len(pending)} events)"
        ar = await loop.run_in_executor(None, analyze_alert, {"raw": context}, res_label)
        analysis_text = format_structured_analysis(ar.parsed) if ar.parsed and not ar.parse_error else ar.raw_text

    svc_summary = ", ".join(svc_names[:5])
    if len(svc_names) > 5:
        svc_summary += f"… +{len(svc_names) - 5} more"

    batch_severity = "critical" if any(dr == "OOMKilled" for _, _, _, _, dr in pending) else "warning"
    webhook_url = get_webhook_for_namespace(ns)
    await loop.run_in_executor(
        None, lambda: post_alert(
            batch_title,
            analysis_text, batch_severity,
            f"BatchAlert/{len(pending)}-events", ns,
            f"{reasons_str}: {svc_summary}",
            node_name, node_metrics, "",
            webhook_url=webhook_url,
        ),
    )


@kopf.on.event("events")
async def on_warning_event(event, logger, **kwargs):
    if event.get("type") is None:
        return
    if time.time() - _startup_time < _STARTUP_GRACE_SECONDS:
        return

    obj = event.get("object")
    if not obj:
        return

    if obj.get("type") != "Warning":
        return

    metadata = obj.get("metadata", {})
    namespace = metadata.get("namespace", "")

    if not _in_watched_namespace(namespace):
        return

    # Check maintenance mode — still process events for dedup, but skip LLM
    from src.handlers.startup import get_store as _get_store
    _loop = asyncio.get_running_loop()
    _maintenance_active = await _loop.run_in_executor(None, _get_store().is_maintenance_active)

    reason = obj.get("reason") or ""
    if reason not in IMPORTANT_EVENT_REASONS:
        return

    involved = obj.get("involvedObject", {})
    obj_name = involved.get("name", "unknown")
    obj_kind = involved.get("kind", "unknown")

    # Import store lazily to avoid circular imports
    from src.handlers.startup import get_store

    store = get_store()

    # Unhealthy grace: derive wait from probe config, skip if pod recovered
    if reason in ("Unhealthy", "ProbeWarning") and obj_kind == "Pod":
        grace, is_young = await asyncio.get_running_loop().run_in_executor(
            None, _get_probe_grace_seconds, namespace, obj_name,
        )
        if is_young:
            # Pod is within probe startup window — wait for probes to settle
            logger.debug("Unhealthy for %s/%s — pod is young, waiting %ds (probe grace)", namespace, obj_name, grace)
            await asyncio.sleep(grace)
            ready_now = await asyncio.get_running_loop().run_in_executor(
                None, _is_pod_ready_now, namespace, obj_name,
            )
            if ready_now:
                logger.info("Skipping Unhealthy for %s/%s — recovered within probe grace (%ds)", namespace, obj_name, grace)
                return

        # After grace: add to batch instead of immediate alert
        owner_key = await asyncio.get_running_loop().run_in_executor(
            None, resolve_owner_key_by_name, namespace, obj_name,
        )
        message = obj.get("message", "")
        pod_status_reason, node_name = await asyncio.get_running_loop().run_in_executor(
            None, _get_pod_status_reason, namespace, obj_name,
        )
        display_reason = pod_status_reason if pod_status_reason else reason
        state_key = _make_state_key(owner_key, display_reason)

        async with _pod_event_batch_lock:
            _pod_event_batch.setdefault(namespace, []).append(
                (obj_name, owner_key, state_key, node_name, display_reason)
            )
            if namespace not in _pod_event_batch_task:
                _pod_event_batch_task[namespace] = asyncio.create_task(
                    _flush_pod_event_batch(namespace, maintenance=_maintenance_active)
                )
        return  # Don't fall through to individual alert flow

    if reason == "FailedScheduling" and obj_kind == "Pod":
        await asyncio.sleep(300)
        still_pending = await asyncio.get_running_loop().run_in_executor(
            None, _is_pod_still_pending, namespace, obj_name,
        )
        if not still_pending:
            logger.debug("Skipping FailedScheduling for %s/%s \u2014 pod no longer Pending", namespace, obj_name)
            return

        # Extended grace period when autoscaler is actively scaling up
        scaling_up = await asyncio.get_running_loop().run_in_executor(
            None, _is_autoscaler_scaling_up,
        )
        if scaling_up:
            logger.info("FailedScheduling for %s/%s \u2014 autoscaler scaling up, waiting another 300s", namespace, obj_name)
            await asyncio.sleep(300)
            still_pending = await asyncio.get_running_loop().run_in_executor(
                None, _is_pod_still_pending, namespace, obj_name,
            )
            if not still_pending:
                logger.info("FailedScheduling for %s/%s \u2014 pod scheduled after autoscaler grace period", namespace, obj_name)
                return

    if reason == "NodeNotReady" and obj_kind == "Pod":
        loop = asyncio.get_running_loop()
        # Resolve node name
        try:
            core = k8s.CoreV1Api()
            pod = await loop.run_in_executor(
                None, core.read_namespaced_pod, obj_name, namespace,
            )
            nr_node = pod.spec.node_name or ""
        except Exception:
            nr_node = ""
        if not nr_node:
            return

        # Node-level dedup (not per-pod)
        node_state_key = f"Node:{nr_node}:notready"
        if node_state_key in _in_flight:
            return
        _in_flight.add(node_state_key)
        try:
            if await loop.run_in_executor(None, store.is_seen, node_state_key):
                return

            # Determine wait time
            try:
                node_obj = await loop.run_in_executor(None, core.read_node, nr_node)
                age = (datetime.now(timezone.utc) - node_obj.metadata.creation_timestamp).total_seconds()
            except Exception:
                age = None

            if age is not None and age < config.NODE_READY_GRACE_SECONDS:
                wait = config.NODE_READY_GRACE_SECONDS - age
                logger.info("NodeNotReady on new node %s (age=%ds), waiting %ds", nr_node, int(age), int(wait))
            else:
                wait = config.NODE_NOT_READY_GRACE_SECONDS
                logger.info("NodeNotReady on node %s, waiting %ds for potential autoscaler deletion", nr_node, int(wait))
            await asyncio.sleep(wait)

            # Post-wait checks
            exists, is_ready, marked = await loop.run_in_executor(None, _is_node_being_deleted, nr_node)
            if not exists:
                logger.info("Node %s deleted (autoscaler scale-down), skipping alert", nr_node)
                return
            if marked:
                logger.info("Node %s marked for deletion, skipping alert", nr_node)
                return
            if is_ready:
                logger.info("Node %s is now Ready, skipping alert", nr_node)
                return

            # Still NotReady — alert
            _nr_dm = config.NON_PROD_DEBOUNCE_MULTIPLIER if config.is_nonprod_namespace(namespace) else 1
            await loop.run_in_executor(
                None, lambda dm=_nr_dm: store.mark_seen(node_state_key, issue_type="notready", debounce_multiplier=dm),
            )
        finally:
            _in_flight.discard(node_state_key)

        logger.info("Warning event: %s — NodeNotReady (%s/%s)", node_state_key, namespace, obj_name)
        collector = _get_collector()
        context_fn = lambda n=obj_name, ns=namespace: collector.collect_pod_context_with_diagnostics(n, ns, "notready")
        await _analyze_and_alert(
            title="Warning: NodeNotReady",
            resource=f"Node/{nr_node}",
            namespace=namespace,
            context_fn=context_fn,
            severity="warning",
            event_reason=f"NodeNotReady: Node {nr_node} is not ready",
            node=nr_node,
            pod_name=obj_name,
            maintenance=_maintenance_active,
        )
        return  # early return — skip the general flow

    # Transient infra errors (sandbox, CNI): grace period, then check if resolved
    if reason in _TRANSIENT_INFRA_REASONS and obj_kind == "Pod":
        logger.debug("Transient %s for %s/%s — waiting %ds", reason, namespace, obj_name, _TRANSIENT_GRACE_SECONDS)
        await asyncio.sleep(_TRANSIENT_GRACE_SECONDS)
        exists = await asyncio.get_running_loop().run_in_executor(
            None, _pod_exists, namespace, obj_name,
        )
        if not exists:
            logger.info("Skipping %s for %s/%s — pod no longer exists (transient)", reason, namespace, obj_name)
            return
        ready = await asyncio.get_running_loop().run_in_executor(
            None, _is_pod_ready_now, namespace, obj_name,
        )
        if ready:
            logger.info("Skipping %s for %s/%s — pod recovered within grace period", reason, namespace, obj_name)
            return
        # Still failing — fall through to normal pod batch flow

    # Flux source-level resources: skip entirely — they don't impact running workloads
    if obj_kind in _FLUX_SOURCE_KINDS:
        logger.debug("Skipping source-level Flux %s/%s event — informational only", obj_kind, obj_name)
        return

    # Flux deployment resources: grace period for transient failures
    if obj_kind in _FLUX_KINDS:
        await asyncio.sleep(120)  # wait 2 min for Flux retry
        # Re-check if resource is now healthy
        try:
            api = k8s.CustomObjectsApi()
            _flux_group_version = {
                "HelmRelease": ("helm.toolkit.fluxcd.io", "v2"),
                "Kustomization": ("kustomize.toolkit.fluxcd.io", "v1"),
            }
            group, version = _flux_group_version.get(obj_kind, ("", ""))
            if group:
                plural = obj_kind.lower() + "s"
                flux_obj = await asyncio.get_running_loop().run_in_executor(
                    None, api.get_namespaced_custom_object, group, version, namespace, plural, obj_name,
                )
                conditions = flux_obj.get("status", {}).get("conditions", [])
                ready_cond = next((c for c in conditions if c.get("type") == "Ready"), None)
                if ready_cond and ready_cond.get("status") == "True":
                    logger.info("Skipping Flux %s/%s alert — recovered after grace period", obj_kind, obj_name)
                    return
        except Exception:
            logger.debug("Failed to check Flux %s/%s status after grace", obj_kind, obj_name)

    # --- Pod events: batch to prevent duplicate alerts for the same pod/deployment ---
    if obj_kind == "Pod":
        owner_key = await asyncio.get_running_loop().run_in_executor(
            None, resolve_owner_key_by_name, namespace, obj_name,
        )
        exists = await asyncio.get_running_loop().run_in_executor(
            None, _pod_exists, namespace, obj_name,
        )
        if not exists:
            logger.info("Skipping alert for deleted pod %s/%s (reason=%s)", namespace, obj_name, reason)
            return

        message = obj.get("message", "")
        pod_status_reason, node_name = await asyncio.get_running_loop().run_in_executor(
            None, _get_pod_status_reason, namespace, obj_name,
        )
        display_reason = pod_status_reason if pod_status_reason else reason
        if not pod_status_reason and display_reason in ("BackOff", "Failed"):
            display_reason = _infer_reason_from_message(reason, message)

        state_key = _make_state_key(owner_key, display_reason)

        async with _pod_event_batch_lock:
            _pod_event_batch.setdefault(namespace, []).append(
                (obj_name, owner_key, state_key, node_name, display_reason)
            )
            if namespace not in _pod_event_batch_task:
                _pod_event_batch_task[namespace] = asyncio.create_task(
                    _flush_pod_event_batch(namespace, maintenance=_maintenance_active)
                )
        return

    # --- Non-pod events: immediate alert flow ---
    owner_key = f"{obj_kind}:{namespace}/{obj_name}"
    message = obj.get("message", "")
    display_reason = reason

    alias = PROBLEM_ALIASES.get(display_reason, PROBLEM_ALIASES.get(reason, reason.lower()))
    state_key = _make_state_key(owner_key, display_reason)

    if state_key in _in_flight:
        return
    _in_flight.add(state_key)

    try:
        if await asyncio.get_running_loop().run_in_executor(None, store.is_seen, state_key):
            return
        if await asyncio.get_running_loop().run_in_executor(None, store.is_suppressed, state_key, namespace, alias):
            return
        _ev_dm = config.NON_PROD_DEBOUNCE_MULTIPLIER if config.is_nonprod_namespace(namespace) else 1
        await asyncio.get_running_loop().run_in_executor(
            None, lambda dm=_ev_dm: store.mark_seen(state_key, issue_type=alias, debounce_multiplier=dm),
        )
    finally:
        _in_flight.discard(state_key)

    logger.info("Warning event: %s \u2014 %s (%s)", state_key, reason, obj_name)
    collector = _get_collector()

    context_fn = lambda: {
        "event": {
            "kind": obj_kind,
            "name": obj_name,
            "namespace": namespace,
            "reason": reason,
            "message": message,
        }
    }

    short_message = message.split("\n")[0][:80] if message else ""
    event_label = f"{display_reason}: {short_message}" if short_message else display_reason

    await _analyze_and_alert(
        title=f"Warning: {display_reason}",
        resource=f"{obj_kind}/{obj_name}",
        namespace=namespace,
        context_fn=context_fn,
        severity="warning",
        event_reason=event_label,
        maintenance=_maintenance_active,
    )
