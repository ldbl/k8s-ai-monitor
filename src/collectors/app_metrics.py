"""App metrics summary for Slack context blocks — extracted from Collector."""
from src import config
from src.collectors._formatters import fmt_bytes
from src.collectors.prometheus import prom_scalar


def get_app_metrics_summary(pod_name: str, namespace: str) -> str:
    """Return a compact one-line app metrics summary for Slack context blocks."""
    if not config.PROMETHEUS_URL:
        return ""

    labels = f'namespace="{namespace}",pod="{pod_name}"'
    parts = []

    el_lag = prom_scalar(f'nodejs_eventloop_lag_p99_seconds{{{labels}}}')
    if el_lag is not None:
        parts.append(f"EL p99: {el_lag * 1000:.0f}ms")

    heap_used = prom_scalar(f'nodejs_heap_size_used_bytes{{{labels}}}')
    heap_total = prom_scalar(f'nodejs_heap_size_total_bytes{{{labels}}}')
    if heap_used is not None and heap_total is not None and heap_total > 0:
        heap_pct = heap_used / heap_total * 100
        parts.append(f"Heap: {heap_pct:.0f}% ({fmt_bytes(heap_used)}/{fmt_bytes(heap_total)})")
    elif heap_used is not None:
        parts.append(f"Heap: {fmt_bytes(heap_used)}")

    rss = prom_scalar(f'process_resident_memory_bytes{{{labels}}}')
    if rss is not None:
        parts.append(f"RSS: {fmt_bytes(rss)}")

    cpu = prom_scalar(f'rate(process_cpu_seconds_total{{{labels}}}[5m])')
    if cpu is not None:
        parts.append(f"CPU: {cpu:.2f}")

    gc = prom_scalar(f'rate(nodejs_gc_duration_seconds_sum{{{labels}}}[5m])')
    if gc is not None:
        parts.append(f"GC: {gc * 1000:.1f}ms/s")

    return " | ".join(parts)
