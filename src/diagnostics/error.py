"""Error state diagnostic plugin."""
from kubernetes import client as k8s

from src.diagnostics._helpers import diag_restart_history


class ErrorDiagnostic:
    issue_type = "error"

    def diagnose(self, core: k8s.CoreV1Api, pod) -> dict:
        restart_history = diag_restart_history(pod)
        return {"restart_history": restart_history} if restart_history else {}
