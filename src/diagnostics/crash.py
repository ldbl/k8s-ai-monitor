"""CrashLoopBackOff diagnostic plugin."""
from kubernetes import client as k8s

from src.diagnostics._helpers import diag_restart_history, diag_probe_config, diag_hpa


class CrashDiagnostic:
    issue_type = "crash"

    def diagnose(self, core: k8s.CoreV1Api, pod) -> dict:
        data = {}
        restart_history = diag_restart_history(pod)
        if restart_history:
            data["restart_history"] = restart_history
        probe_config = diag_probe_config(pod)
        if probe_config:
            data["probe_config"] = probe_config
        hpa = diag_hpa(core, pod)
        if hpa:
            data["hpa"] = hpa
        return data
