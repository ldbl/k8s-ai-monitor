"""Reconciliation scanner — auto-resolves stale active incidents.

Periodically checks active incidents in the store against current K8s state
and emits auto_resolve ScanResults for incidents whose underlying issue is gone.

Covers:
- Node:* incidents — checks if node is Ready
- Flux:* incidents — checks Ready condition on Flux resources
- HelmRepository/HelmChart/GitRepository/OCIRepository event incidents
"""
import logging
import re

from kubernetes import client as k8s

from src import config
from src.scanners._base import ScanResult

logger = logging.getLogger(__name__)

# Flux CRD group/version/plural mappings
_FLUX_RESOURCES: dict[str, tuple[str, str, str]] = {
    "HelmRelease": ("helm.toolkit.fluxcd.io", "v2", "helmreleases"),
    "Kustomization": ("kustomize.toolkit.fluxcd.io", "v1", "kustomizations"),
    "HelmRepository": ("source.toolkit.fluxcd.io", "v1", "helmrepositories"),
    "HelmChart": ("source.toolkit.fluxcd.io", "v1", "helmcharts"),
    "GitRepository": ("source.toolkit.fluxcd.io", "v1", "gitrepositories"),
    "OCIRepository": ("source.toolkit.fluxcd.io", "v1", "ocirepositories"),
}


class ReconcileScanner:
    """Periodic reconciliation: auto-resolve stale active incidents."""

    name = "reconcile"
    startup_delay = 120  # let other scanners run first

    @property
    def enabled(self) -> bool:
        return True  # always enabled — core correctness feature

    @property
    def interval_seconds(self) -> int:
        return config.SCANNER_INTERVAL_SECONDS

    def scan(self) -> list[ScanResult]:
        from src.handlers.startup import get_store
        store = get_store()
        results: list[ScanResult] = []

        results.extend(self._reconcile_nodes(store))
        results.extend(self._reconcile_flux(store))

        if results:
            logger.info("Reconcile scanner: %d incidents auto-resolved", len(results))
        else:
            logger.debug("Reconcile scanner: nothing to resolve")
        return results

    def _reconcile_nodes(self, store) -> list[ScanResult]:
        """Auto-resolve Node:* incidents where the node is now Ready."""
        results: list[ScanResult] = []
        active = store.get_active_incidents_by_prefix(["Node:"])
        if not active:
            return results

        core = k8s.CoreV1Api()
        for incident in active:
            # Parse node name from state_key "Node:{node_name}:notready"
            match = re.match(r"^Node:([^:]+):", incident.state_key)
            if not match:
                continue
            node_name = match.group(1)

            try:
                node = core.read_node(node_name)
                is_ready = any(
                    c.type == "Ready" and c.status == "True"
                    for c in (node.status.conditions or [])
                )
                if not is_ready:
                    continue
            except k8s.ApiException as e:
                if e.status == 404:
                    logger.debug("Reconcile: node %s not found (deleted), resolving", node_name)
                else:
                    logger.warning("Reconcile: API error checking node %s: %s", node_name, e.reason)
                    continue
            except Exception:
                logger.debug("Reconcile: failed to check node %s", node_name, exc_info=True)
                continue

            results.append(ScanResult(
                state_key=incident.state_key,
                title=f"Resolved: Node {node_name} is Ready",
                severity="info",
                resource=f"Node/{node_name}",
                namespace="",
                issue_type=incident.issue_type,
                auto_resolve=True,
            ))

        return results

    def _reconcile_flux(self, store) -> list[ScanResult]:
        """Auto-resolve Flux resource incidents where the resource is now Ready."""
        results: list[ScanResult] = []

        # Flux handler incidents: "Flux:{Kind}:{ns}/{name}:stalled"
        flux_handler_active = store.get_active_incidents_by_prefix(
            ["Flux:HelmRelease:", "Flux:Kustomization:"]
        )
        for incident in flux_handler_active:
            match = re.match(r"^Flux:(\w+):([^/]+)/([^:]+):", incident.state_key)
            if not match:
                continue
            kind, ns, name = match.group(1), match.group(2), match.group(3)
            if self._is_flux_resource_ready(kind, ns, name):
                results.append(ScanResult(
                    state_key=incident.state_key,
                    title=f"Resolved: Flux {kind} {ns}/{name}",
                    severity="info",
                    resource=f"{kind}/{name}",
                    namespace=ns,
                    issue_type=incident.issue_type,
                    auto_resolve=True,
                ))

        # Event handler incidents: "{Kind}:{ns}/{name}:{alias}"
        event_flux_active = store.get_active_incidents_by_prefix(
            ["HelmRepository:", "HelmChart:", "GitRepository:", "OCIRepository:",
             "HelmRelease:", "Kustomization:"]
        )
        for incident in event_flux_active:
            match = re.match(r"^(\w+):([^/]+)/([^:]+):", incident.state_key)
            if not match:
                continue
            kind, ns, name = match.group(1), match.group(2), match.group(3)
            if kind not in _FLUX_RESOURCES:
                continue
            if self._is_flux_resource_ready(kind, ns, name):
                results.append(ScanResult(
                    state_key=incident.state_key,
                    title=f"Resolved: {kind} {ns}/{name}",
                    severity="info",
                    resource=f"{kind}/{name}",
                    namespace=ns,
                    issue_type=incident.issue_type,
                    auto_resolve=True,
                ))

        return results

    def _is_flux_resource_ready(self, kind: str, namespace: str, name: str) -> bool:
        """Check if a Flux resource has Ready=True condition."""
        spec = _FLUX_RESOURCES.get(kind)
        if not spec:
            return False
        group, version, plural = spec

        try:
            api = k8s.CustomObjectsApi()
            obj = api.get_namespaced_custom_object(group, version, namespace, plural, name)
            conditions = obj.get("status", {}).get("conditions", [])
            ready = next((c for c in conditions if c.get("type") == "Ready"), None)
            return ready is not None and ready.get("status") == "True"
        except k8s.ApiException as e:
            if e.status == 404:
                logger.debug("Reconcile: %s %s/%s not found (deleted), resolving", kind, namespace, name)
                return True  # resource deleted — resolve the incident
            logger.warning("Reconcile: API error checking %s %s/%s: %s", kind, namespace, name, e.reason)
            return False
        except Exception:
            logger.debug("Reconcile: failed to check %s %s/%s", kind, namespace, name, exc_info=True)
            return False

    def collect_daily_data(self) -> str | None:
        return None
