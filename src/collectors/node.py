"""Node metrics collection — extracted from Collector."""
import logging

from kubernetes import client as k8s

from src import config
from src.collectors._formatters import parse_cpu, parse_memory_mi, fmt_bytes, fmt_bytes_rate
from src.collectors.prometheus import prom_scalar

logger = logging.getLogger(__name__)


def get_node_metrics_summary(node_name: str) -> str:
    """Return a compact one-line metrics summary for a node (for Slack fields)."""
    if not node_name:
        return ""

    core = k8s.CoreV1Api()
    node_ip = ""
    try:
        node = core.read_node(node_name)
        for addr in (node.status.addresses or []):
            if addr.type == "InternalIP":
                node_ip = addr.address
                break
    except Exception:
        return ""

    if not node_ip or not config.PROMETHEUS_URL:
        try:
            alloc = node.status.allocatable or {}
            alloc_cpu_m = parse_cpu(alloc.get("cpu", ""))
            alloc_mem_mi = parse_memory_mi(alloc.get("memory", ""))
            metrics_api = k8s.CustomObjectsApi()
            nm = metrics_api.get_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes", node_name)
            usage = nm.get("usage", {})
            usage_cpu_m = parse_cpu(usage.get("cpu", ""))
            usage_mem_mi = parse_memory_mi(usage.get("memory", ""))
            parts = []
            if usage_cpu_m is not None and alloc_cpu_m:
                parts.append(f"CPU: {usage_cpu_m / alloc_cpu_m * 100:.1f}%")
            if usage_mem_mi is not None and alloc_mem_mi:
                parts.append(f"Mem: {usage_mem_mi / alloc_mem_mi * 100:.1f}%")
            return " | ".join(parts)
        except Exception:
            return ""

    inst = f"{node_ip}:.*"
    parts = []

    cpu_pct = prom_scalar(
        f'100 - (avg(rate(node_cpu_seconds_total{{mode="idle",instance=~"{inst}"}}[5m])) * 100)'
    )
    if cpu_pct is not None:
        parts.append(f"CPU: {cpu_pct:.1f}%")

    mem_total = prom_scalar(f'node_memory_MemTotal_bytes{{instance=~"{inst}"}}')
    mem_avail = prom_scalar(f'node_memory_MemAvailable_bytes{{instance=~"{inst}"}}')
    if mem_total and mem_avail is not None:
        mem_used = mem_total - mem_avail
        mem_pct = mem_used / mem_total * 100
        parts.append(f"Mem: {mem_pct:.1f}% ({fmt_bytes(mem_used)}/{fmt_bytes(mem_total)})")

    fs_size = prom_scalar(f'node_filesystem_size_bytes{{instance=~"{inst}",mountpoint="/",fstype!~"tmpfs|overlay"}}')
    fs_avail = prom_scalar(f'node_filesystem_avail_bytes{{instance=~"{inst}",mountpoint="/",fstype!~"tmpfs|overlay"}}')
    if fs_size and fs_avail is not None:
        fs_pct = (fs_size - fs_avail) / fs_size * 100
        parts.append(f"Disk: {fs_pct:.1f}%")
    else:
        btrfs_total = prom_scalar(f'node_btrfs_device_size_bytes{{instance=~"{inst}"}}')
        btrfs_used = prom_scalar(f'sum(node_btrfs_used_bytes{{instance=~"{inst}"}})')
        if btrfs_total and btrfs_used is not None:
            parts.append(f"Disk: {btrfs_used / btrfs_total * 100:.1f}%")

    net_rx = prom_scalar(
        f'sum(rate(node_network_receive_bytes_total{{instance=~"{inst}",device!~"lo|veth.*|cali.*|flannel.*|cni.*"}}[5m]))'
    )
    net_tx = prom_scalar(
        f'sum(rate(node_network_transmit_bytes_total{{instance=~"{inst}",device!~"lo|veth.*|cali.*|flannel.*|cni.*"}}[5m]))'
    )
    if net_rx is not None or net_tx is not None:
        parts.append(f"Net: RX {fmt_bytes_rate(net_rx)}/s TX {fmt_bytes_rate(net_tx)}/s")

    return " | ".join(parts)
