"""Pod scanner — extracted from main._scan_pods."""
import logging

from kubernetes import client as k8s

from src import config
from src.engine.constants import PROBLEM_STATES, POD_PROBLEM_REASONS, PROBLEM_ALIASES
from src.engine.owner import resolve_owner_key
from src.scanners._base import ScanResult

logger = logging.getLogger(__name__)


class PodScanner:
    name = "pod"
    startup_delay = 0  # uses interval_seconds as initial delay

    @property
    def enabled(self):
        return config.SCANNER_POD_ENABLED

    @property
    def interval_seconds(self):
        return config.SCANNER_INTERVAL_SECONDS

    def scan(self) -> list[ScanResult]:
        core = k8s.CoreV1Api()
        results = []
        problem_keys: set[str] = set()

        for ns in config.get_namespaces():
            try:
                pods = core.list_namespaced_pod(ns)
            except Exception:
                logger.exception("Pod scanner: failed to list pods in %s", ns)
                continue

            for pod in pods.items:
                # Pod-level reasons
                pod_reason = (pod.status.reason or "") if pod.status else ""
                if pod_reason in POD_PROBLEM_REASONS:
                    severity = "critical" if pod_reason in ("OutOfmemory", "OutOfcpu") else "warning"
                    owner_key = resolve_owner_key(pod)
                    alias = PROBLEM_ALIASES.get(pod_reason, pod_reason.lower())
                    state_key = f"{owner_key}:{alias}"
                    problem_keys.add(state_key)
                    results.append(ScanResult(
                        state_key=state_key,
                        title=f"Pod Issue: {pod_reason}",
                        severity=severity,
                        resource=pod.metadata.name,
                        namespace=ns,
                        issue_type=alias,
                        pod_name=pod.metadata.name,
                        node_name=pod.spec.node_name or "",
                    ))
                    continue

                # Container-level states
                for cs in (pod.status.container_statuses or []):
                    reason = None
                    severity = "warning"

                    if cs.state and cs.state.waiting and cs.state.waiting.reason in PROBLEM_STATES:
                        reason = cs.state.waiting.reason
                    if not reason and cs.state and cs.state.terminated:
                        if cs.state.terminated.reason == "OOMKilled":
                            reason = "OOMKilled"
                            severity = "critical"
                    if not reason and cs.last_state and cs.last_state.terminated:
                        if cs.last_state.terminated.reason == "OOMKilled":
                            reason = "OOMKilled"
                            severity = "critical"

                    if not reason:
                        continue

                    owner_key = resolve_owner_key(pod)
                    alias = PROBLEM_ALIASES.get(reason, reason.lower())
                    state_key = f"{owner_key}:{alias}"
                    problem_keys.add(state_key)
                    results.append(ScanResult(
                        state_key=state_key,
                        title=f"Pod Issue: {reason}",
                        severity=severity,
                        resource=pod.metadata.name,
                        namespace=ns,
                        issue_type=alias,
                        pod_name=pod.metadata.name,
                        node_name=pod.spec.node_name or "",
                    ))

        # Auto-resolve: check active pod/deployment incidents against current state
        try:
            from src.handlers.startup import get_store
            store = get_store()
            active = store.get_active_incidents_by_prefix(
                ["Deployment:", "StatefulSet:", "DaemonSet:", "Job:", "Pod:"]
            )
            for incident in active:
                if incident.state_key in problem_keys:
                    continue
                # Parse namespace from state_key (format: "Kind:namespace/name:alias")
                parts = incident.state_key.split(":")
                ns_resource = parts[1] if len(parts) > 1 else ""
                ns_part = ns_resource.split("/")[0] if "/" in ns_resource else ""
                results.append(ScanResult(
                    state_key=incident.state_key,
                    title=f"Resolved: {incident.state_key}",
                    severity="info",
                    resource=ns_resource,
                    namespace=ns_part,
                    issue_type=incident.issue_type,
                    auto_resolve=True,
                ))
            resolved_count = len(results) - len(problem_keys)
            if resolved_count > 0:
                logger.info("Pod scanner: %d incidents auto-resolved", resolved_count)
        except Exception:
            logger.warning("Pod scanner: failed to check for auto-resolve", exc_info=True)

        issues = [r for r in results if not r.auto_resolve]
        if issues:
            details = "; ".join(f"{r.namespace}/{r.resource}: {r.issue_type}" for r in issues)
            logger.info("Pod scanner: %d issues found: %s", len(issues), details)
        else:
            logger.info("Pod scanner: no issues found")
        return results

    def collect_daily_data(self) -> str | None:
        return None  # daily pod data handled by collectors/daily.py
