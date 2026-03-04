"""DiagnosticPlugin protocol."""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from kubernetes import client as k8s


@runtime_checkable
class DiagnosticPlugin(Protocol):
    issue_type: str

    def diagnose(self, core: k8s.CoreV1Api, pod) -> dict: ...
