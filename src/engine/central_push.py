"""Push data to central ClickHouse via HTTP interface."""
import json
import logging
from datetime import datetime, timezone

import requests

from src import config

logger = logging.getLogger(__name__)

# Fields that contain Unix timestamps and need conversion to ISO format for DateTime64
_TIMESTAMP_FIELDS = {"first_seen_at", "last_seen_at", "resolved_at", "called_at", "created_at"}


def _convert_timestamps(row: dict) -> dict:
    """Convert float Unix timestamps to ISO 8601 strings for ClickHouse DateTime64."""
    for key in _TIMESTAMP_FIELDS:
        val = row.get(key)
        if isinstance(val, (int, float)) and val > 0:
            row[key] = datetime.fromtimestamp(val, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]  # trim to milliseconds
    return row


def _push(table: str, rows: list[dict]) -> None:
    if not config.CENTRAL_AGGREGATE or not rows:
        return
    rows = [_convert_timestamps(r) for r in rows]
    body = "\n".join(json.dumps(r, default=str) for r in rows)
    try:
        resp = requests.post(
            f"{config.CENTRAL_CH_URL}/",
            params={
                "query": f"INSERT INTO {config.CENTRAL_CH_DATABASE}.{table} FORMAT JSONEachRow",
                "async_insert": "1",
                "wait_for_async_insert": "0",
            },
            data=body.encode(),
            auth=(config.CENTRAL_CH_USER, config.CENTRAL_CH_PASSWORD),
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning("CH push %s failed: %s %s", table, resp.status_code, resp.text[:200])
        else:
            logger.debug("CH push %s OK: %d rows", table, len(rows))
    except (requests.RequestException, ValueError, TypeError, KeyError):
        logger.warning("CH push %s error", table, exc_info=True)


def push_incident(data: dict) -> None:
    data["cluster"] = config.CLUSTER_NAME
    _push("incidents", [data])


def push_report(data: dict) -> None:
    data["cluster"] = config.CLUSTER_NAME
    _push("daily_reports", [data])


def push_llm_usage(data: dict) -> None:
    data["cluster"] = config.CLUSTER_NAME
    _push("llm_usage", [data])
