"""OOM diagnostic plugin."""
from kubernetes import client as k8s

from src.diagnostics._helpers import diag_node_resources


class OomDiagnostic:
    issue_type = "oom"

    def diagnose(self, core: k8s.CoreV1Api, pod) -> dict:
        data = {}
        oom_details = []
        for cs in (pod.status.container_statuses or []):
            term = None
            if cs.last_state and cs.last_state.terminated:
                term = cs.last_state.terminated
            elif cs.state and cs.state.terminated:
                term = cs.state.terminated
            if term and term.reason == "OOMKilled":
                detail = {
                    "container": cs.name,
                    "exit_code": term.exit_code,
                }
                if term.signal:
                    detail["signal"] = term.signal
                oom_details.append(detail)
        if oom_details:
            data["oom_details"] = oom_details

        node_mem = diag_node_resources(core, pod, focus="memory")
        if node_mem:
            data["node_memory"] = node_mem

        return data
