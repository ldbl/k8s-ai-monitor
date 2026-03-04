"""Elasticsearch log search — stateless, uses requests."""
import logging

import requests

from src import config

logger = logging.getLogger(__name__)

_TIMEOUT = 10


def _get_index() -> str:
    prefix = config.ELASTICSEARCH_INDEX_PREFIX or config.CLUSTER_NAME
    return f"{prefix}-logs"


def _make_auth() -> tuple[str, str] | None:
    if config.ELASTICSEARCH_USER and config.ELASTICSEARCH_PASSWORD:
        return (config.ELASTICSEARCH_USER, config.ELASTICSEARCH_PASSWORD)
    return None


def _is_configured() -> bool:
    return bool(config.ELASTICSEARCH_URL)


def search_logs(
    namespace: str,
    pod_pattern: str = "",
    query_string: str = "",
    since_minutes: int = 30,
    limit: int = 100,
) -> list[dict] | None:
    """Search ES logs by namespace, optional pod pattern, and query string.

    Returns list of hit dicts with timestamp, pod, container, message fields,
    or None if ES is not configured / unreachable.
    """
    if not _is_configured():
        return None

    must: list[dict] = [
        {"range": {"@timestamp": {"gte": f"now-{since_minutes}m", "lte": "now"}}},
        {"term": {"kubernetes.namespace_name": namespace}},
    ]
    if pod_pattern:
        must.append({"wildcard": {"kubernetes.pod_name": pod_pattern}})
    if query_string:
        must.append({"query_string": {"query": query_string, "default_field": "message",
                                       "analyze_wildcard": True}})

    body = {
        "size": limit,
        "sort": [{"@timestamp": "desc"}],
        "query": {"bool": {"must": must}},
        "_source": ["@timestamp", "message", "log", "stream",
                     "kubernetes.pod_name", "kubernetes.container_name",
                     "level", "severity"],
    }

    try:
        resp = requests.post(
            f"{config.ELASTICSEARCH_URL}/{_get_index()}/_search",
            json=body,
            auth=_make_auth(),
            timeout=_TIMEOUT,
            verify=True,
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        results = []
        for hit in hits:
            src = hit.get("_source", {})
            k8s = src.get("kubernetes", {})
            results.append({
                "timestamp": src.get("@timestamp", ""),
                "pod": k8s.get("pod_name", ""),
                "container": k8s.get("container_name", ""),
                "message": src.get("message") or src.get("log", ""),
                "stream": src.get("stream", ""),
                "level": src.get("level") or src.get("severity", ""),
            })
        return results
    except requests.ConnectionError as exc:
        logger.warning("Elasticsearch unreachable: %s", exc)
    except requests.HTTPError as exc:
        logger.warning("Elasticsearch HTTP error: %s", exc)
    except Exception:
        logger.warning("Elasticsearch search failed", exc_info=True)
    return None


def search_error_logs(
    namespace: str,
    since_minutes: int = 60,
    limit: int = 100,
) -> list[dict] | None:
    """Search for error-level logs in a namespace.

    Looks for stderr stream OR error/critical severity levels.
    """
    if not _is_configured():
        return None

    must: list[dict] = [
        {"range": {"@timestamp": {"gte": f"now-{since_minutes}m", "lte": "now"}}},
        {"term": {"kubernetes.namespace_name": namespace}},
    ]
    should: list[dict] = [
        {"term": {"stream": "stderr"}},
        {"terms": {"level": ["error", "ERROR", "fatal", "FATAL", "critical", "CRITICAL"]}},
        {"terms": {"severity": ["error", "ERROR", "fatal", "FATAL", "critical", "CRITICAL"]}},
    ]

    body = {
        "size": limit,
        "sort": [{"@timestamp": "desc"}],
        "query": {"bool": {"must": must, "should": should, "minimum_should_match": 1}},
        "_source": ["@timestamp", "message", "log", "stream",
                     "kubernetes.pod_name", "kubernetes.container_name",
                     "level", "severity"],
    }

    try:
        resp = requests.post(
            f"{config.ELASTICSEARCH_URL}/{_get_index()}/_search",
            json=body,
            auth=_make_auth(),
            timeout=_TIMEOUT,
            verify=True,
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        results = []
        for hit in hits:
            src = hit.get("_source", {})
            k8s = src.get("kubernetes", {})
            results.append({
                "timestamp": src.get("@timestamp", ""),
                "pod": k8s.get("pod_name", ""),
                "container": k8s.get("container_name", ""),
                "message": src.get("message") or src.get("log", ""),
                "stream": src.get("stream", ""),
                "level": src.get("level") or src.get("severity", ""),
            })
        return results
    except requests.ConnectionError as exc:
        logger.warning("Elasticsearch unreachable: %s", exc)
    except requests.HTTPError as exc:
        logger.warning("Elasticsearch HTTP error: %s", exc)
    except Exception:
        logger.warning("Elasticsearch error search failed", exc_info=True)
    return None
