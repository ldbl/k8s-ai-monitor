"""Prometheus range query helper."""
import logging
import time

import requests

from src import config

logger = logging.getLogger(__name__)


def prom_range_query(
    query: str,
    since_minutes: int = 30,
    step: str = "60s",
) -> list[dict] | None:
    """Execute a Prometheus range query.

    Returns list of result dicts (each with metric labels and values array),
    or None if Prometheus is unreachable.
    """
    if not config.PROMETHEUS_URL:
        return None

    try:
        resp = requests.get(
            f"{config.PROMETHEUS_URL}/api/v1/query_range",
            params={
                "query": query,
                "start": str(int(time.time()) - since_minutes * 60),
                "end": str(int(time.time())),
                "step": step,
            },
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "success" and data["data"]["result"]:
            return data["data"]["result"]
        if data.get("status") != "success":
            logger.warning("Prometheus range query error: status=%s, error=%s, query=%s",
                           data.get("status"), data.get("error", ""), query[:120])
    except (requests.ConnectionError, requests.Timeout) as exc:
        logger.warning("Prometheus unreachable: %s — %s", query[:120], exc)
    except Exception:
        logger.warning("Prometheus range query failed: %s", query[:120], exc_info=True)
    return None
