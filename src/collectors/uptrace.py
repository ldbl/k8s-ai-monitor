"""Uptrace REST API client — span search and service stats."""
import logging

import requests

from src import config

logger = logging.getLogger(__name__)

_TIMEOUT = 10


def _is_configured() -> bool:
    return bool(config.UPTRACE_API_URL and config.UPTRACE_API_TOKEN)


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {config.UPTRACE_API_TOKEN}",
        "Content-Type": "application/json",
    }


def _base_url() -> str:
    url = config.UPTRACE_API_URL.rstrip("/")
    return f"{url}/api/v1/tracing/{config.UPTRACE_PROJECT_ID}"


def search_spans(
    service_name: str,
    since_minutes: int = 30,
    limit: int = 50,
    status_code: str | None = None,
) -> list[dict] | None:
    """Search spans by service name, optionally filtered by status code.

    Args:
        service_name: OTel service.name to filter by.
        since_minutes: Look back window.
        limit: Max spans to return.
        status_code: If "error", filter for error spans only.

    Returns list of span dicts or None if Uptrace is not configured/unreachable.
    """
    if not _is_configured():
        return None

    parts = [f"where service.name = '{service_name}'"]
    if status_code == "error":
        parts.append("where status_code = 'error'")
    query = " | ".join(parts)

    params: dict[str, str | int] = {
        "query": query,
        "time_gte": f"now-{since_minutes}m",
        "time_lt": "now",
        "limit": limit,
        "order_by": "time desc",
    }

    try:
        resp = requests.get(
            f"{_base_url()}/spans",
            params=params,  # type: ignore[arg-type]
            headers=_headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        spans = data.get("spans", data.get("data", []))
        results = []
        for span in spans:
            results.append({
                "trace_id": span.get("traceId", span.get("trace_id", "")),
                "span_id": span.get("spanId", span.get("span_id", "")),
                "name": span.get("name", ""),
                "service": span.get("serviceName", span.get("service.name", service_name)),
                "duration_ms": span.get("durationMs", span.get("duration_ms", 0)),
                "status_code": span.get("statusCode", span.get("status_code", "")),
                "time": span.get("time", ""),
                "attrs": span.get("attrs", {}),
            })
        return results
    except requests.ConnectionError as exc:
        logger.warning("Uptrace unreachable: %s", exc)
    except requests.HTTPError as exc:
        logger.warning("Uptrace HTTP error: %s", exc)
    except Exception:
        logger.warning("Uptrace span search failed", exc_info=True)
    return None


def search_slow_spans(
    service_name: str,
    since_minutes: int = 30,
    min_duration_ms: int = 1000,
    limit: int = 50,
) -> list[dict] | None:
    """Search for slow spans (duration >= min_duration_ms)."""
    if not _is_configured():
        return None

    query = (
        f"where service.name = '{service_name}' "
        f"| where duration >= {min_duration_ms}ms"
    )

    params: dict[str, str | int] = {
        "query": query,
        "time_gte": f"now-{since_minutes}m",
        "time_lt": "now",
        "limit": limit,
        "order_by": "duration desc",
    }

    try:
        resp = requests.get(
            f"{_base_url()}/spans",
            params=params,  # type: ignore[arg-type]
            headers=_headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        spans = data.get("spans", data.get("data", []))
        results = []
        for span in spans:
            results.append({
                "trace_id": span.get("traceId", span.get("trace_id", "")),
                "span_id": span.get("spanId", span.get("span_id", "")),
                "name": span.get("name", ""),
                "service": span.get("serviceName", span.get("service.name", service_name)),
                "duration_ms": span.get("durationMs", span.get("duration_ms", 0)),
                "status_code": span.get("statusCode", span.get("status_code", "")),
                "time": span.get("time", ""),
                "attrs": span.get("attrs", {}),
            })
        return results
    except requests.ConnectionError as exc:
        logger.warning("Uptrace unreachable: %s", exc)
    except requests.HTTPError as exc:
        logger.warning("Uptrace HTTP error: %s", exc)
    except Exception:
        logger.warning("Uptrace slow span search failed", exc_info=True)
    return None


def get_service_stats(
    service_name: str,
    since_minutes: int = 30,
) -> dict | None:
    """Get aggregated stats for a service: span count, error rate, avg duration.

    Returns dict with keys: span_count, error_count, error_rate, avg_duration_ms,
    p50_duration_ms, p99_duration_ms, or None if unavailable.
    """
    if not _is_configured():
        return None

    query = (
        f"where service.name = '{service_name}' "
        f"| group by service.name "
        f"| count() as span_count "
        f"| countIf(status_code = 'error') as error_count "
        f"| avg(duration) as avg_duration "
        f"| p50(duration) as p50_duration "
        f"| p99(duration) as p99_duration"
    )

    params = {
        "query": query,
        "time_gte": f"now-{since_minutes}m",
        "time_lt": "now",
    }

    try:
        resp = requests.get(
            f"{_base_url()}/groups",
            params=params,
            headers=_headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        groups = data.get("groups", data.get("data", []))
        if not groups:
            return {
                "span_count": 0, "error_count": 0, "error_rate": 0.0,
                "avg_duration_ms": 0, "p50_duration_ms": 0, "p99_duration_ms": 0,
            }
        g = groups[0]
        span_count = g.get("span_count", g.get("count", 0))
        error_count = g.get("error_count", 0)
        error_rate = (error_count / span_count * 100) if span_count else 0.0
        return {
            "span_count": span_count,
            "error_count": error_count,
            "error_rate": round(error_rate, 2),
            "avg_duration_ms": g.get("avg_duration", g.get("avg_duration_ms", 0)),
            "p50_duration_ms": g.get("p50_duration", g.get("p50_duration_ms", 0)),
            "p99_duration_ms": g.get("p99_duration", g.get("p99_duration_ms", 0)),
        }
    except requests.ConnectionError as exc:
        logger.warning("Uptrace unreachable: %s", exc)
    except requests.HTTPError as exc:
        logger.warning("Uptrace HTTP error: %s", exc)
    except Exception:
        logger.warning("Uptrace service stats failed", exc_info=True)
    return None
