"""Prometheus query helpers — extracted from Collector."""
import logging

import requests

from src import config

logger = logging.getLogger(__name__)


def prom_scalar(query: str) -> float | None:
    result = prom_query(query)
    if result:
        return float(result[0]["value"][1])
    return None


def prom_query(query: str) -> list | None:
    if not config.PROMETHEUS_URL:
        return None
    try:
        resp = requests.get(
            f"{config.PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=5,
        )
        data = resp.json()
        if data.get("status") == "success" and data["data"]["result"]:
            return data["data"]["result"]
        if data.get("status") != "success":
            logger.warning("Prometheus query error: status=%s, error=%s, query=%s",
                           data.get("status"), data.get("error", ""), query[:120])
    except (requests.ConnectionError, requests.Timeout) as exc:
        logger.warning("Prometheus unreachable: %s — %s", query[:120], exc)
    except Exception:
        logger.warning("Prometheus query failed: %s", query[:120], exc_info=True)
    return None
