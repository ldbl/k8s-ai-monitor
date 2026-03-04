"""Stable fingerprint generation per issue type."""
import hashlib
from collections.abc import Callable


STABLE_SIGNATURES: dict[str, Callable[[dict], str]] = {
    "crash":        lambda m: f"exit={m.get('exit_code', '?')}",
    "oom":          lambda m: "OOMKilled",
    "image-pull":   lambda m: str(m.get("image") or "?")[:80],
    "scheduling":   lambda m: m.get("reason", "?"),
    "mount":        lambda m: m.get("volume", "?"),
    "error":        lambda m: f"exit={m.get('exit_code', '?')}",
    "unhealthy":    lambda m: m.get("probe_type", "?"),
    "evicted":      lambda m: m.get("reason", "Evicted"),
    "create-error": lambda m: str(m.get("message") or "?")[:80],
    "certificate":  lambda m: str(m.get("issue") or "?")[:80],
    "pvc":          lambda m: m.get("severity", "?"),
    "endpoint":     lambda m: str(m.get("url") or "?")[:80],
}


def compute_fingerprint(state_key: str, issue_type: str, metadata: dict) -> str:
    """Compute stable fingerprint: sha256(state_key + stable_signature).

    state_key is human-readable (e.g. "Deployment:ns/name:oom").
    fingerprint adds issue-specific stable signature to prevent cardinality explosion.
    """
    sig_fn = STABLE_SIGNATURES.get(issue_type, lambda m: "")
    sig = sig_fn(metadata)
    return hashlib.sha256(f"{state_key}:{sig}".encode()).hexdigest()
