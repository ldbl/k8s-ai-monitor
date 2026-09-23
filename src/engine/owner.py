"""Consolidated owner resolution — single source for all 3 previous copies.

Provides:
- resolve_owner(pod) -> (kind, name)   — from diagnostics.py
- resolve_owner_key(pod) -> str        — from main.py
- resolve_owner_key_by_name(ns, pod_name) -> str  — from handlers.py (cached)
"""
import logging
import time

from kubernetes import client as k8s

logger = logging.getLogger(__name__)

# In-memory cache: (ns, pod_name) -> (owner_key, timestamp)
_owner_cache: dict[tuple[str, str], tuple[str, float]] = {}
_OWNER_CACHE_TTL = 300  # 5 minutes


def resolve_owner(pod) -> tuple[str, str]:
    """Return (owner_kind, owner_name) for pod. Resolves ReplicaSet -> Deployment."""
    try:
        apps = k8s.AppsV1Api()
        for owner in (pod.metadata.owner_references or []):
            if owner.kind == "ReplicaSet":
                rs = apps.read_namespaced_replica_set(owner.name, pod.metadata.namespace)
                for rs_owner in (rs.metadata.owner_references or []):
                    if rs_owner.kind == "Deployment":
                        return "Deployment", rs_owner.name
                return "ReplicaSet", owner.name
            elif owner.kind in ("StatefulSet", "DaemonSet", "Job"):
                return owner.kind, owner.name
    except Exception:
        logger.debug("Failed to resolve owner for %s/%s", pod.metadata.namespace, pod.metadata.name, exc_info=True)
    return "", ""


def resolve_owner_key(pod) -> str:
    """Return owner key string like 'Deployment:ns/name' for a pod object."""
    namespace = pod.metadata.namespace
    try:
        apps = k8s.AppsV1Api()
        for owner in (pod.metadata.owner_references or []):
            if owner.kind == "ReplicaSet":
                rs = apps.read_namespaced_replica_set(owner.name, namespace)
                for rs_owner in (rs.metadata.owner_references or []):
                    if rs_owner.kind == "Deployment":
                        return f"Deployment:{namespace}/{rs_owner.name}"
                return f"ReplicaSet:{namespace}/{owner.name}"
            elif owner.kind in ("StatefulSet", "DaemonSet", "Job"):
                return f"{owner.kind}:{namespace}/{owner.name}"
    except Exception:
        logger.debug("Failed to resolve owner for %s/%s", namespace, pod.metadata.name)
    return f"Pod:{namespace}/{pod.metadata.name}"


def resolve_owner_key_by_name(namespace: str, pod_name: str) -> str:
    """Return owner key by looking up the pod by name. Uses 5min cache."""
    now = time.time()
    cache_key = (namespace, pod_name)
    cached = _owner_cache.get(cache_key)
    if cached and (now - cached[1]) < _OWNER_CACHE_TTL:
        return cached[0]

    try:
        core = k8s.CoreV1Api()
        apps = k8s.AppsV1Api()
        pod = core.read_namespaced_pod(pod_name, namespace)
        for owner in (pod.metadata.owner_references or []):
            if owner.kind == "ReplicaSet":
                rs = apps.read_namespaced_replica_set(owner.name, namespace)
                for rs_owner in (rs.metadata.owner_references or []):
                    if rs_owner.kind == "Deployment":
                        key = f"Deployment:{namespace}/{rs_owner.name}"
                        _owner_cache[cache_key] = (key, now)
                        return key
                key = f"ReplicaSet:{namespace}/{owner.name}"
                _owner_cache[cache_key] = (key, now)
                return key
            elif owner.kind in ("StatefulSet", "DaemonSet", "Job"):
                key = f"{owner.kind}:{namespace}/{owner.name}"
                _owner_cache[cache_key] = (key, now)
                return key
    except Exception:
        logger.debug("Failed to resolve owner for %s/%s", namespace, pod_name)

    key = f"Pod:{namespace}/{pod_name}"
    _owner_cache[cache_key] = (key, now)
    return key
