"""Unhealthy probe diagnostic plugin."""
from kubernetes import client as k8s

from src.diagnostics._helpers import diag_probe_config


class UnhealthyDiagnostic:
    issue_type = "unhealthy"

    def diagnose(self, core: k8s.CoreV1Api, pod) -> dict:
        probe_config = diag_probe_config(pod)
        return {"probe_config": probe_config} if probe_config else {}
