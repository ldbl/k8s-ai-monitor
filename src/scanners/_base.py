"""Scanner protocol and ScanResult dataclass."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass
class ScanResult:
    state_key: str          # dedup key: "Deployment:ns/name:oom"
    title: str              # "Pod Issue: OOMKilled"
    severity: str           # "critical" | "warning"
    resource: str           # "Pod/my-pod"
    namespace: str
    issue_type: str         # diagnostic alias: "oom", "crash", etc.
    pod_name: str = ""
    node_name: str = ""
    context_override: str = ""  # scanners that build own context
    event_reason: str = ""
    auto_resolve: bool = False  # clear state when healthy
    skip_llm: bool = False      # post to Slack without LLM analysis
    metadata: dict = field(default_factory=dict)


@runtime_checkable
class Scanner(Protocol):
    name: str
    enabled: bool
    interval_seconds: int
    startup_delay: int

    def scan(self) -> list[ScanResult]: ...
    def collect_daily_data(self) -> str | None: ...
