"""FailedScheduling diagnostic plugin."""
import logging
from datetime import datetime, timedelta, timezone

from kubernetes import client as k8s

from src.diagnostics._helpers import (
    diag_hpa, diag_pdb,
)

logger = logging.getLogger(__name__)


class SchedulingDiagnostic:
    issue_type = "scheduling"

    def diagnose(self, core: k8s.CoreV1Api, pod) -> dict:
        data = {}

        # Pod scheduling constraints
        constraints = {}
        if pod.spec.node_selector:
            constraints["node_selector"] = dict(pod.spec.node_selector)
        if pod.spec.tolerations:
            tols = []
            for t in pod.spec.tolerations:
                if t.key:
                    tols.append({"key": t.key, "value": t.value, "effect": t.effect or "any"})
            if tols:
                constraints["tolerations"] = tols
        if pod.spec.affinity:
            constraints["affinity"] = True
        tsc_list = pod.spec.topology_spread_constraints or []
        if tsc_list:
            constraints["topology_spread"] = [
                {"max_skew": tsc.max_skew, "key": tsc.topology_key}
                for tsc in tsc_list
            ]
        if constraints:
            data["scheduling_constraints"] = constraints

        # Nodes
        try:
            nodes = core.list_node()

            # Cordoned nodes
            cordoned = []
            for n in nodes.items:
                if n.spec.unschedulable:
                    cordoned.append(n.metadata.name)
            if cordoned:
                data["cordoned_nodes"] = cordoned

            # Node capacity summary
            pod_node_selector = pod.spec.node_selector or {}
            node_capacity = []
            for node in nodes.items:
                name = node.metadata.name
                alloc = node.status.allocatable or {}
                ready = "Unknown"
                for cond in (node.status.conditions or []):
                    if cond.type == "Ready":
                        ready = cond.status
                entry = {
                    "name": name,
                    "ready": ready,
                    "schedulable": not node.spec.unschedulable,
                    "cpu": alloc.get("cpu", "?"),
                    "memory": alloc.get("memory", "?"),
                }
                if pod_node_selector:
                    labels = node.metadata.labels or {}
                    entry["selector_match"] = all(labels.get(k) == v for k, v in pod_node_selector.items())
                node_capacity.append(entry)
            if node_capacity:
                data["node_capacity"] = node_capacity

        except Exception:
            logger.debug("Failed to list nodes for scheduling diagnostics")

        hpa = diag_hpa(core, pod)
        if hpa:
            data["hpa"] = hpa

        pdb = diag_pdb(core, pod)
        if pdb:
            data["pdb"] = pdb

        autoscaler_ctx = self._collect_autoscaler_context(core)
        if autoscaler_ctx:
            data["cluster_autoscaler"] = autoscaler_ctx

        return data

    def _collect_autoscaler_context(self, core: k8s.CoreV1Api) -> dict | None:
        """Collect cluster autoscaler context for LLM analysis."""
        context = {}
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=30)
        autoscaler_reasons = {
            "TriggeredScaleUp", "ScaledUpGroup", "ScaleUp",
            "NotTriggerScaleUp", "ScaleDown", "ScaleDownEmpty",
        }

        # Recent autoscaler events from kube-system
        try:
            events = core.list_namespaced_event("kube-system")
            recent = []
            for ev in events.items:
                if ev.reason not in autoscaler_reasons:
                    continue
                ts = ev.last_timestamp or ev.metadata.creation_timestamp
                if ts and ts >= cutoff:
                    msg = (ev.message or "")[:200]
                    recent.append({"reason": ev.reason, "message": msg, "time": ts.isoformat()})
            if recent:
                context["recent_events"] = recent[:10]
        except Exception:
            logger.debug("Failed to collect autoscaler events")

        # Provisioning nodes (NotReady, younger than 15 min)
        try:
            nodes = core.list_node()
            provisioning = []
            for node in nodes.items:
                age_s = (now - node.metadata.creation_timestamp).total_seconds()
                if age_s < 900:  # younger than 15 min
                    is_ready = False
                    for cond in (node.status.conditions or []):
                        if cond.type == "Ready":
                            is_ready = cond.status == "True"
                            break
                    if not is_ready:
                        provisioning.append({
                            "name": node.metadata.name,
                            "age_seconds": int(age_s),
                        })
            if provisioning:
                context["provisioning_nodes"] = provisioning
        except Exception:
            logger.debug("Failed to collect provisioning nodes")

        # cluster-autoscaler-status ConfigMap
        try:
            cm = core.read_namespaced_config_map("cluster-autoscaler-status", "kube-system")
            status = (cm.data or {}).get("status", "")
            if status:
                context["autoscaler_status"] = status[:500]
        except Exception:
            logger.debug("Failed to read cluster-autoscaler-status ConfigMap")

        return context if context else None
