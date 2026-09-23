"""Evicted diagnostic plugin."""
from kubernetes import client as k8s

from src.diagnostics._helpers import diag_node_resources, diag_pdb


class EvictedDiagnostic:
    issue_type = "evicted"

    def diagnose(self, core: k8s.CoreV1Api, pod) -> dict:
        data = {}
        node_info = diag_node_resources(core, pod, focus="pressure")
        if node_info:
            data["node_pressure"] = node_info
        pdb = diag_pdb(core, pod)
        if pdb:
            data["pdb"] = pdb
        return data
