"""Shared formatting/parsing helpers for collectors."""


def parse_cpu(value: str) -> float | None:
    """Parse CPU value to millicores. '2' -> 2000, '500m' -> 500, '100u' -> 0.1."""
    if not value or value == "?":
        return None
    try:
        if value.endswith("n"):
            return float(value[:-1]) / 1_000_000
        if value.endswith("u"):
            return float(value[:-1]) / 1_000
        if value.endswith("m"):
            return float(value[:-1])
        return float(value) * 1000
    except (ValueError, TypeError):
        return None


def parse_memory_mi(value: str) -> float | None:
    """Parse memory value to MiB. '16Gi' -> 16384, '22856Mi' -> 22856, '23404224Ki' -> 22856."""
    if not value or value == "?":
        return None
    try:
        if value.endswith("Ki"):
            return float(value[:-2]) / 1024
        if value.endswith("Mi"):
            return float(value[:-2])
        if value.endswith("Gi"):
            return float(value[:-2]) * 1024
        if value.endswith("Ti"):
            return float(value[:-2]) * 1024 * 1024
        return float(value) / (1024 * 1024)
    except (ValueError, TypeError):
        return None


def fmt_cpu(millicores: float) -> str:
    """Format millicores: 37.0 -> '37m', 2500.0 -> '2500m'."""
    if millicores >= 1000:
        return f"{millicores / 1000:.1f}"
    return f"{millicores:.0f}m"


def fmt_mem(mi: float) -> str:
    """Format MiB: 6220.0 -> '6220Mi', 32768.0 -> '32Gi'."""
    if mi >= 1024:
        return f"{mi / 1024:.1f}Gi"
    return f"{mi:.0f}Mi"


def fmt_bytes(b: float | None) -> str:
    if b is None:
        return "?"
    gi = b / (1024 ** 3)
    if gi >= 1:
        return f"{gi:.1f}Gi"
    return f"{b / (1024 ** 2):.0f}Mi"


def fmt_bytes_rate(b: float | None) -> str:
    """Format bytes/second rate: 1048576.0 -> '1.0MB', 512.0 -> '512B'."""
    if b is None:
        return "?"
    if b >= 1024 ** 3:
        return f"{b / (1024 ** 3):.1f}GB"
    if b >= 1024 ** 2:
        return f"{b / (1024 ** 2):.1f}MB"
    if b >= 1024:
        return f"{b / 1024:.1f}KB"
    return f"{b:.0f}B"
