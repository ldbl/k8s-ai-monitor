"""Incident store — SQLite backend.

Provides the Incident dataclass and SqliteStore for full incident lifecycle
with occurrences, escalation, suppressions, LLM cost tracking, and daily reports.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Incident:
    id: int
    state_key: str
    fingerprint: str
    issue_type: str
    severity: str
    owner_ref: str
    first_seen_at: float
    last_seen_at: float
    occurrence_count: int
    cooldown_until: float | None
    last_slack_ts: str
    status: str  # "active", "acknowledged", "resolved"
