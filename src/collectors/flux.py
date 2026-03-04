"""Flux context collection — extracted from Collector."""
import logging

from kubernetes import client as k8s

logger = logging.getLogger(__name__)


def collect_flux_context(name: str, namespace: str, kind: str) -> dict:
    """Collect Flux resource context as structured dict.

    Only reads status fields (conditions, history, failure counts).
    Never reads spec to avoid leaking secrets embedded in HelmRelease values.
    """
    custom = k8s.CustomObjectsApi()
    group = "kustomize.toolkit.fluxcd.io" if kind == "Kustomization" else "helm.toolkit.fluxcd.io"
    version = "v1"
    plural = "kustomizations" if kind == "Kustomization" else "helmreleases"

    try:
        obj = custom.get_namespaced_custom_object(group, version, namespace, plural, name)
    except Exception as exc:
        status = getattr(exc, "status", None)
        if status == 404:
            logger.debug("Failed to get Flux %s %s/%s: %s", kind, namespace, name, exc)
        else:
            logger.warning("Failed to get Flux %s %s/%s: %s", kind, namespace, name, exc)
        return {
            "flux_resource": {"kind": kind, "name": name, "namespace": namespace},
            "error": f"Kubernetes API error fetching {kind} {namespace}/{name}: {exc}",
        }

    status = obj.get("status", {})
    conditions = status.get("conditions", [])

    result: dict = {
        "flux_resource": {"kind": kind, "name": name, "namespace": namespace},
        "conditions": [
            {"type": c["type"], "status": c["status"], "message": c.get("message", "")}
            for c in conditions
        ],
    }

    # HelmRelease-specific: include failure counts and release history
    if kind == "HelmRelease":
        failures = status.get("failures", 0)
        upgrade_failures = status.get("upgradeFailures", 0)
        install_failures = status.get("installFailures", 0)
        if failures or upgrade_failures or install_failures:
            result["failure_counts"] = {
                "total": failures,
                "upgrade": upgrade_failures,
                "install": install_failures,
            }

        last_action = status.get("lastAttemptedReleaseAction", "")
        last_revision = status.get("lastAttemptedRevision", "")
        if last_action or last_revision:
            result["last_attempt"] = {
                "action": last_action,
                "revision": last_revision,
            }

        # Include recent release history (status only, no config/values)
        history = status.get("history", [])
        if history:
            result["release_history"] = [
                {
                    "version": h.get("version"),
                    "status": h.get("status", ""),
                    "chartVersion": h.get("chartVersion", ""),
                    "appVersion": h.get("appVersion", ""),
                }
                for h in history[:5]
            ]

    return result
