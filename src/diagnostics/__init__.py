"""Diagnostics registry and dispatcher."""
import logging

from kubernetes import client as k8s

from src.diagnostics._base import DiagnosticPlugin
from src.diagnostics._helpers import diag_current_usage, diag_previous_logs, diag_resource_limits
from src.diagnostics.oom import OomDiagnostic
from src.diagnostics.crash import CrashDiagnostic
from src.diagnostics.image_pull import ImagePullDiagnostic
from src.diagnostics.scheduling import SchedulingDiagnostic
from src.diagnostics.mount import MountDiagnostic
from src.diagnostics.error import ErrorDiagnostic
from src.diagnostics.unhealthy import UnhealthyDiagnostic
from src.diagnostics.evicted import EvictedDiagnostic

logger = logging.getLogger(__name__)

ALL_PLUGINS: list[DiagnosticPlugin] = [
    OomDiagnostic(),
    CrashDiagnostic(),
    ImagePullDiagnostic(),
    SchedulingDiagnostic(),
    MountDiagnostic(),
    ErrorDiagnostic(),
    UnhealthyDiagnostic(),
    EvictedDiagnostic(),
]

_PLUGIN_MAP = {p.issue_type: p for p in ALL_PLUGINS}


def collect_diagnostics(pod_name: str, namespace: str, issue_type: str) -> dict:
    """Run diagnostics for a pod based on issue type. Returns structured dict."""
    core = k8s.CoreV1Api()

    try:
        pod = core.read_namespaced_pod(pod_name, namespace)
    except Exception:
        logger.debug("Diagnostics: failed to read pod %s/%s", namespace, pod_name)
        return {}

    data = {}

    # Common diagnostics
    current_usage = diag_current_usage(pod)
    if current_usage:
        data["current_usage"] = current_usage

    previous_logs = diag_previous_logs(core, pod)
    if previous_logs:
        data["previous_logs"] = previous_logs

    resource_limits = diag_resource_limits(pod)
    if resource_limits:
        data["resource_limits"] = resource_limits

    # Issue-specific
    plugin = _PLUGIN_MAP.get(issue_type)
    if plugin:
        plugin_data = plugin.diagnose(core, pod)
        if plugin_data:
            data.update(plugin_data)

    return data
