"""Shared diagnostic helpers — extracted from diagnostics.py."""
import logging

from kubernetes import client as k8s

from src.engine.owner import resolve_owner

logger = logging.getLogger(__name__)


def diag_current_usage(pod) -> list[dict]:
    """Fetch current CPU/memory usage from Metrics API. Returns list of container dicts."""
    custom = k8s.CustomObjectsApi()
    ns = pod.metadata.namespace
    name = pod.metadata.name

    result = []
    try:
        pm = custom.get_namespaced_custom_object(
            "metrics.k8s.io", "v1beta1", ns, "pods", name,
        )
        for c in pm.get("containers", []):
            usage = c.get("usage", {})
            cpu_raw = usage.get("cpu", "0")
            mem_raw = usage.get("memory", "0")
            result.append({
                "container": c["name"],
                "cpu": _parse_cpu(cpu_raw),
                "mem": _parse_memory(mem_raw),
            })
    except Exception:
        logger.debug("Metrics API unavailable for pod %s/%s", ns, name)

    return result


def diag_previous_logs(core: k8s.CoreV1Api, pod) -> list[str]:
    """Fetch previous container logs for containers that have restarted. Returns list of strings."""
    result = []
    for cs in (pod.status.container_statuses or []):
        if cs.restart_count > 0:
            try:
                prev_logs = core.read_namespaced_pod_log(
                    pod.metadata.name, pod.metadata.namespace,
                    container=cs.name,
                    previous=True,
                    tail_lines=20,
                )
                if prev_logs.strip():
                    for line in prev_logs.strip().split("\n"):
                        # Strip timestamps
                        parts = line.split(" ", 1)
                        if len(parts) == 2 and len(parts[0]) > 18 and "T" in parts[0]:
                            result.append(parts[1][:500])
                        else:
                            result.append(line[:500])
            except Exception:
                logger.debug("Failed to get previous logs for %s/%s", cs.name, pod.metadata.name)
    return result


def diag_resource_limits(pod) -> list[dict]:
    """Extract resource requests and limits from pod spec. Returns list of dicts."""
    result = []
    for c in (pod.spec.containers or []):
        resources = c.resources
        entry = {"container": c.name}
        if not resources:
            entry["note"] = "no resource requests/limits set"
        else:
            req = resources.requests or {}
            lim = resources.limits or {}
            if req.get("cpu"):
                entry["cpu_req"] = req["cpu"]
            if req.get("memory"):
                entry["mem_req"] = req["memory"]
            if lim.get("cpu"):
                entry["cpu_lim"] = lim["cpu"]
            if lim.get("memory"):
                entry["mem_lim"] = lim["memory"]
        result.append(entry)
    return result


def diag_restart_history(pod) -> list[dict]:
    """Show restart count and last termination exit codes. Returns list of dicts."""
    result = []
    for cs in (pod.status.container_statuses or []):
        if cs.restart_count > 0:
            entry = {"container": cs.name, "restarts": cs.restart_count}
            if cs.last_state and cs.last_state.terminated:
                t = cs.last_state.terminated
                entry["exit_code"] = t.exit_code
                entry["reason"] = t.reason or ""
                if t.signal:
                    entry["signal"] = t.signal
            result.append(entry)
    return result


def diag_probe_config(pod) -> list[dict]:
    """Show liveness/readiness/startup probe configuration. Returns list of dicts."""
    result = []
    for c in (pod.spec.containers or []):
        for probe_name, probe in [("liveness", c.liveness_probe), ("readiness", c.readiness_probe), ("startup", c.startup_probe)]:
            entry = {"container": c.name, "probe": probe_name}
            if probe:
                if probe.http_get:
                    entry["type"] = "httpGet"
                    entry["path"] = f"{probe.http_get.path}:{probe.http_get.port}"
                elif probe.tcp_socket:
                    entry["type"] = "tcpSocket"
                    entry["port"] = probe.tcp_socket.port
                elif probe._exec:
                    entry["type"] = "exec"
                entry["period"] = probe.period_seconds
                entry["timeout"] = probe.timeout_seconds
                entry["failures"] = probe.failure_threshold
            else:
                entry["configured"] = False
            result.append(entry)
    return result


def diag_node_resources(core: k8s.CoreV1Api, pod, focus: str = "memory") -> dict | None:
    """Show node resource info for the node running this pod. Returns dict or None."""
    node_name = pod.spec.node_name
    if not node_name:
        return None
    try:
        node = core.read_node(node_name)
        cap = node.status.capacity or {}
        alloc = node.status.allocatable or {}

        if focus == "memory":
            return {
                "node": node_name,
                "capacity": cap.get("memory", "?"),
                "allocatable": alloc.get("memory", "?"),
            }
        elif focus == "pressure":
            pressures = []
            for cond in (node.status.conditions or []):
                if cond.type in ("MemoryPressure", "DiskPressure", "PIDPressure"):
                    pressures.append({"type": cond.type, "status": cond.status, "message": cond.message})
            return {"node": node_name, "pressures": pressures} if pressures else None
    except Exception:
        logger.debug("Failed to read node %s for diagnostics", node_name)
    return None


def diag_hpa(core: k8s.CoreV1Api, pod) -> dict | None:
    """Check if pod's owner has an HPA and whether it's at capacity. Returns dict or None."""
    ns = pod.metadata.namespace
    owner_kind, owner_name = resolve_owner(pod)
    if not owner_kind or owner_kind == "Job":
        return None
    try:
        autoscaling = k8s.AutoscalingV2Api()
        hpas = autoscaling.list_namespaced_horizontal_pod_autoscaler(ns)
        for hpa in hpas.items:
            ref = hpa.spec.scale_target_ref
            if ref.kind == owner_kind and ref.name == owner_name:
                hs = hpa.status
                result = {
                    "name": hpa.metadata.name,
                    "target": f"{ref.kind}/{ref.name}",
                    "current": hs.current_replicas,
                    "max": hpa.spec.max_replicas,
                    "min": hpa.spec.min_replicas,
                    "desired": hs.desired_replicas,
                }
                if hs.current_replicas and hs.current_replicas >= hpa.spec.max_replicas:
                    result["at_max"] = True
                return result
    except Exception:
        logger.debug("Failed to check HPA for %s/%s", ns, pod.metadata.name)
    return None


def diag_pdb(core: k8s.CoreV1Api, pod) -> dict | None:
    """Check PodDisruptionBudgets that match this pod's workload. Returns dict or None."""
    ns = pod.metadata.namespace
    owner_kind, owner_name = resolve_owner(pod)
    template_labels = get_workload_template_labels(owner_kind, owner_name, ns)
    match_labels = template_labels if template_labels else (pod.metadata.labels or {})

    try:
        policy = k8s.PolicyV1Api()
        pdbs = policy.list_namespaced_pod_disruption_budget(ns)
        for pdb in pdbs.items:
            selector = pdb.spec.selector
            if not selector or not selector.match_labels:
                continue
            if all(match_labels.get(k) == v for k, v in selector.match_labels.items()):
                s = pdb.status
                result = {
                    "name": pdb.metadata.name,
                    "min_available": pdb.spec.min_available,
                    "max_unavailable": pdb.spec.max_unavailable,
                    "current_healthy": s.current_healthy,
                    "desired_healthy": s.desired_healthy,
                    "disruptions_allowed": s.disruptions_allowed,
                }
                if s.disruptions_allowed == 0:
                    result["blocking"] = True
                return result
    except Exception:
        logger.debug("Failed to check PDBs for %s/%s", ns, pod.metadata.name)
    return None


def get_workload_template_labels(kind: str, name: str, namespace: str) -> dict:
    """Get pod template labels from the workload spec."""
    try:
        apps = k8s.AppsV1Api()
        if kind == "Deployment":
            obj = apps.read_namespaced_deployment(name, namespace)
        elif kind == "StatefulSet":
            obj = apps.read_namespaced_stateful_set(name, namespace)
        elif kind == "DaemonSet":
            obj = apps.read_namespaced_daemon_set(name, namespace)
        else:
            return {}
        return dict(obj.spec.template.metadata.labels or {})
    except Exception:
        return {}


# --- internal parsers (diagnostics-specific format) ---

def _parse_cpu(val: str) -> str:
    if val.endswith("n"):
        return f"{int(val[:-1]) // 1_000_000}m"
    if val.endswith("m"):
        return val
    try:
        return f"{int(float(val) * 1000)}m"
    except ValueError:
        return val


def _parse_memory(val: str) -> str:
    if val.endswith("Ki"):
        return f"{int(val[:-2]) // 1024}Mi"
    if val.endswith("Mi"):
        return val
    if val.endswith("Gi"):
        return f"{int(float(val[:-2]) * 1024)}Mi"
    try:
        return f"{int(val) // (1024 * 1024)}Mi"
    except ValueError:
        return val


def _parse_cpu_millicores(val: str) -> float:
    if not val:
        return 0.0
    try:
        if val.endswith("n"):
            return float(val[:-1]) / 1_000_000
        if val.endswith("m"):
            return float(val[:-1])
        return float(val) * 1000
    except (ValueError, TypeError):
        return 0.0


def _parse_mem_mi(val: str) -> float:
    if not val:
        return 0.0
    try:
        if val.endswith("Ki"):
            return float(val[:-2]) / 1024
        if val.endswith("Mi"):
            return float(val[:-2])
        if val.endswith("Gi"):
            return float(val[:-2]) * 1024
        return float(val) / (1024 * 1024)
    except (ValueError, TypeError):
        return 0.0
