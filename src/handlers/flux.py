"""Flux Kustomization/HelmRelease handlers — extracted from handlers.py."""
import asyncio
import logging
import time

import kopf

from src import config
from src.engine.llm import analyze_alert
from src.engine.notifier import post_alert, format_structured_analysis, get_webhook_for_namespace
from src.collectors import Collector
from src.collectors.node import get_node_metrics_summary
from src.collectors.app_metrics import get_app_metrics_summary

logger = logging.getLogger(__name__)

_startup_time = time.time()
_STARTUP_GRACE_SECONDS = 30

# In-flight state keys to prevent duplicate concurrent alerts
_in_flight: set[str] = set()

_collector: Collector | None = None


def _get_collector() -> Collector:
    global _collector
    if _collector is None:
        _collector = Collector()
    return _collector


async def _analyze_and_alert(title: str, resource: str, namespace: str,
                              context_fn, severity: str, event_reason: str = "",
                              node: str = "", pod_name: str = ""):
    loop = asyncio.get_running_loop()
    if config.is_nonprod_namespace(namespace):
        analysis_text = title
        title = f"{config.nonprod_title_prefix(namespace)} {title}"
        model = ""
    else:
        context = await loop.run_in_executor(None, context_fn)
        res_label = f"{namespace}/{resource}"
        ar = await loop.run_in_executor(None, analyze_alert, context, res_label)
        analysis_text = format_structured_analysis(ar.parsed) if ar.parsed and not ar.parse_error else ar.raw_text
        if not analysis_text or not analysis_text.strip():
            analysis_text = "Analysis unavailable"
        model = ar.model
    node_metrics = ""
    if node:
        node_metrics = await loop.run_in_executor(None, get_node_metrics_summary, node)
    app_metrics = ""
    if pod_name:
        app_metrics = await loop.run_in_executor(None, get_app_metrics_summary, pod_name, namespace)
    webhook_url = get_webhook_for_namespace(namespace)
    await loop.run_in_executor(
        None, lambda: post_alert(title, analysis_text, severity, resource, namespace, event_reason, node, node_metrics,
                                 app_metrics, model, webhook_url=webhook_url),
    )


@kopf.on.event("kustomizations", group="kustomize.toolkit.fluxcd.io", version="v1")
async def on_kustomization_event(event, logger, **kwargs):
    await _handle_flux_event(event, "Kustomization", logger)


@kopf.on.event("helmreleases", group="helm.toolkit.fluxcd.io", version="v2")
async def on_helmrelease_event(event, logger, **kwargs):
    await _handle_flux_event(event, "HelmRelease", logger)


async def _handle_flux_event(event, kind: str, logger):
    if event.get("type") is None:
        return
    if time.time() - _startup_time < _STARTUP_GRACE_SECONDS:
        return

    obj = event.get("object")
    if not obj:
        return

    name = obj.get("metadata", {}).get("name", "")
    namespace = obj.get("metadata", {}).get("namespace", "")

    # Respect EXCLUDE_NAMESPACES / WATCH_NAMESPACES
    if namespace in config.EXCLUDE_NAMESPACES:
        return
    if not config.WATCH_ALL_NAMESPACES and namespace not in config.NAMESPACES:
        return

    conditions = obj.get("status", {}).get("conditions", [])

    cond_map = {c.get("type"): c for c in conditions}
    stalled = cond_map.get("Stalled", {})
    if stalled.get("status") != "True":
        return

    from src.handlers.startup import get_store
    store = get_store()

    state_key = f"Flux:{kind}:{namespace}/{name}:stalled"

    if state_key in _in_flight:
        return
    _in_flight.add(state_key)

    try:
        if await asyncio.get_running_loop().run_in_executor(None, store.is_seen, state_key):
            return
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: store.mark_seen(state_key, issue_type="stalled"),
        )
    finally:
        _in_flight.discard(state_key)

    message = stalled.get("message", "")
    logger.info("Flux %s stalled: %s/%s \u2014 %s", kind, namespace, name, message)
    collector = _get_collector()
    await _analyze_and_alert(
        title=f"Flux {kind} Stalled",
        resource=f"{kind}/{name}",
        namespace=namespace,
        context_fn=lambda n=name, ns=namespace, k=kind: collector.collect_flux_context(n, ns, k),
        severity="warning",
    )
