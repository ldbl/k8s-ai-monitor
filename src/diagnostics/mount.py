"""FailedMount diagnostic plugin."""
import logging

from kubernetes import client as k8s

logger = logging.getLogger(__name__)


class MountDiagnostic:
    issue_type = "mount"

    def diagnose(self, core: k8s.CoreV1Api, pod) -> dict:
        ns = pod.metadata.namespace
        pvcs = []
        for vol in (pod.spec.volumes or []):
            pvc = vol.persistent_volume_claim
            if not pvc:
                continue
            try:
                claim = core.read_namespaced_persistent_volume_claim(pvc.claim_name, ns)
                phase = claim.status.phase if claim.status else "Unknown"
                entry = {"name": pvc.claim_name, "phase": phase}
                if phase != "Bound":
                    events = []
                    try:
                        ev_list = core.list_namespaced_event(
                            ns, field_selector=f"involvedObject.name={pvc.claim_name},involvedObject.kind=PersistentVolumeClaim"
                        )
                        for e in (ev_list.items or [])[-5:]:
                            events.append({"type": e.type, "reason": e.reason, "message": e.message})
                    except Exception:
                        logger.debug("Failed to list events for PVC %s in %s", pvc.claim_name, ns, exc_info=True)
                    if events:
                        entry["events"] = events
                pvcs.append(entry)
            except k8s.ApiException as e:
                if e.status == 404:
                    pvcs.append({"name": pvc.claim_name, "phase": "NOT_FOUND"})
                else:
                    logger.debug("PVC check failed for %s: %s", pvc.claim_name, e)
                    pvcs.append({"name": pvc.claim_name, "phase": "CHECK_FAILED"})

        return {"pvcs": pvcs} if pvcs else {}
