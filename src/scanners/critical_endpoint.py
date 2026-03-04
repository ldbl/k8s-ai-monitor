"""Critical Endpoint scanner — Storefront CR-based deep chain probing.

Discovery: Storefront CRs (senteca.dev/v1) with active=true
Chain:     storefront-{name}:4015 → api-gateway:4000 → subgraph services:3000
"""
import logging
import re
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from kubernetes import client as k8s

from src import config
from src.collectors.app_metrics import get_app_metrics_summary
from src.collectors.node import get_node_metrics_summary
from src.collectors.pod import query_app_metrics
from src.collectors.prometheus import prom_query, prom_scalar
from src.scanners._base import ScanResult
from src.scanners._probe import ProbeResult, probe, probe_external, probe_via_cluster_ip

logger = logging.getLogger(__name__)

# Patterns for detecting upstream connection strings in env vars
_UPSTREAM_PATTERNS = re.compile(
    r"(mongodb|postgres|mysql|redis|kafka|amqp|nats|typesense)"
    r"://(?:[^@/\s]+@)?([^:/@\s]+)",
    re.IGNORECASE,
)

# Env var patterns for subgraph service discovery
_SERVICE_HOST_RE = re.compile(r"^(.+)_SERVICE_HOST$")
_SERVICE_PORT_GRAPHQL_RE = re.compile(r"^(.+)_SERVICE_PORT_GRAPHQL$")

# Services to skip in subgraph discovery (not subgraphs)
_SKIP_SERVICES = {"KUBERNETES", "API_GATEWAY"}


@dataclass
class HopResult:
    name: str
    probe: ProbeResult
    healthy: bool
    depth: int
    health_path: str


def _list_active_storefronts(ns: str) -> list[dict]:
    """List Storefront CRs with active=true via CustomObjectsApi."""
    api = k8s.CustomObjectsApi()
    try:
        sfs = api.list_namespaced_custom_object("senteca.dev", "v1", ns, "storefronts")
        return [sf for sf in sfs.get("items", []) if sf.get("spec", {}).get("active")]
    except k8s.ApiException as e:
        if e.status == 404:
            logger.debug("Storefront CRD not found in %s (404)", ns)
        else:
            logger.warning("Failed to list Storefront CRs in %s: %s", ns, e.reason)
        return []
    except Exception:
        logger.exception("Failed to list Storefront CRs in %s", ns)
        return []


def _get_readiness_probe_path(ns: str, deployment_name: str) -> tuple[int, str]:
    """Read readinessProbe from a deployment spec. Returns (port, path)."""
    apps = k8s.AppsV1Api()
    try:
        deploy = apps.read_namespaced_deployment(deployment_name, ns)
        for c in (deploy.spec.template.spec.containers or []):
            if c.readiness_probe and c.readiness_probe.http_get:
                port = c.readiness_probe.http_get.port
                path = c.readiness_probe.http_get.path or "/"
                if not path.startswith("/"):
                    path = "/" + path
                if isinstance(port, str):
                    # Named port — resolve from container ports
                    for cp in (c.ports or []):
                        if cp.name == port:
                            port = cp.container_port
                            break
                    else:
                        port = 3000  # fallback
                return int(port), path
    except Exception:
        logger.debug("Failed to read readinessProbe from %s/%s", ns, deployment_name)
    return 3000, "/probes/ready"


def _resolve_named_port(ns: str, deployment_name: str, port_name: str) -> int | None:
    """Resolve a named port to its numeric value from deployment containers."""
    try:
        apps = k8s.AppsV1Api()
        deploy = apps.read_namespaced_deployment(deployment_name, ns)
        for c in (deploy.spec.template.spec.containers or []):
            for cp in (c.ports or []):
                if cp.name == port_name:
                    return cp.container_port
    except Exception:
        logger.debug("Failed to resolve named port %s for %s/%s", port_name, ns, deployment_name)
    return None


def _get_health_path_for_service(ns: str, svc_name: str) -> str:
    """Get health check path reachable via the k8s service port.

    Matches service targetPort to readinessProbe port. If they match,
    uses the probe path. Otherwise falls back to /health.
    """
    target_port = None
    try:
        core = k8s.CoreV1Api()
        svc = core.read_namespaced_service(svc_name, ns)
        if svc.spec.ports:
            tp = svc.spec.ports[0].target_port
            if tp is not None:
                try:
                    target_port = int(tp)
                except (ValueError, TypeError):
                    # Named port — resolve from deployment containers
                    target_port = _resolve_named_port(ns, svc_name, str(tp))
    except Exception:
        logger.debug("Failed to read service %s/%s for health path", ns, svc_name)

    probe_port, probe_path = _get_readiness_probe_path(ns, svc_name)

    if target_port is not None and target_port == probe_port:
        return probe_path

    # targetPort unknown or differs from probe port.
    # If service port == probe port, the probe path likely works too.
    try:
        core = k8s.CoreV1Api()
        svc = core.read_namespaced_service(svc_name, ns)
        if svc.spec.ports and svc.spec.ports[0].port == probe_port:
            return probe_path
    except Exception:
        logger.debug("Failed to check service port for %s/%s", ns, svc_name)

    return "/health"


def _discover_subgraph_services(ns: str) -> list[tuple[str, int, str]]:
    """Discover subgraph services from api-gateway pod env vars.

    The api-gateway discovers subgraphs via k8s-injected env vars:
    {SERVICE_NAME}_SERVICE_HOST and {SERVICE_NAME}_SERVICE_PORT_GRAPHQL.

    Returns: [(service_name, port, health_path), ...]
    """
    core = k8s.CoreV1Api()
    try:
        pods = core.list_namespaced_pod(
            ns, label_selector="app=api-gateway", limit=1,
        )
        if not pods.items:
            # Try alternative label
            pods = core.list_namespaced_pod(
                ns, label_selector="app.kubernetes.io/name=api-gateway", limit=1,
            )
        if not pods.items:
            logger.debug("No api-gateway pod found in %s", ns)
            return []
    except Exception:
        logger.debug("Failed to list api-gateway pods in %s", ns)
        return []

    pod = pods.items[0]

    # Collect env vars from all containers
    env_map: dict[str, str] = {}
    for container in (pod.spec.containers or []):
        for env in (container.env or []):
            if env.value:
                env_map[env.name] = env.value

    # Find services that have both _SERVICE_HOST and _SERVICE_PORT_GRAPHQL
    host_services: dict[str, str] = {}  # normalized_name -> host
    port_services: dict[str, str] = {}  # normalized_name -> port

    for name, value in env_map.items():
        m = _SERVICE_HOST_RE.match(name)
        if m:
            host_services[m.group(1)] = value
            continue
        m = _SERVICE_PORT_GRAPHQL_RE.match(name)
        if m:
            port_services[m.group(1)] = value

    # Services with GRAPHQL port are subgraphs
    subgraphs = []
    for svc_key in sorted(port_services.keys()):
        if svc_key in _SKIP_SERVICES:
            continue
        if svc_key not in host_services:
            continue

        # Convert env var name to k8s service name: CORE_SERVICE → core-service
        svc_name = svc_key.lower().replace("_", "-")

        # Get readinessProbe from deployment
        port, health_path = _get_readiness_probe_path(ns, svc_name)

        subgraphs.append((svc_name, port, health_path))

    return subgraphs


def _get_service_port(ns: str, svc_name: str) -> int:
    """Get the first port of a k8s service. Returns 80 as fallback."""
    try:
        core = k8s.CoreV1Api()
        svc = core.read_namespaced_service(svc_name, ns)
        if svc.spec.ports:
            return svc.spec.ports[0].port
    except Exception:
        pass
    return 80


def _probe_chain(
    sf_name: str,
    sf_address: str,
    ns: str,
    chain_depth: int,
) -> tuple[ProbeResult | None, ProbeResult | None, list[HopResult], bool]:
    """Probe the entire chain for a storefront.

    Returns: (external_probe, cluster_ip_probe, hops, all_healthy)
    """
    timeout = config.ENDPOINT_SCAN_TIMEOUT
    all_healthy = True
    hops: list[HopResult] = []

    # Dual probes via ingress (if ingress service configured)
    external_probe = None
    cluster_ip_probe = None
    if config.ENDPOINT_INGRESS_SERVICE:
        # External probe: via LoadBalancer IP (real external path)
        external_probe = probe_external(
            host=sf_address,
            path="/",
            ingress_svc=config.ENDPOINT_INGRESS_SERVICE,
            timeout=timeout,
        )
        ext_healthy = external_probe.status_code is not None and 200 <= external_probe.status_code <= 399
        if not ext_healthy:
            all_healthy = False

        # Internal probe: via ClusterIP (in-cluster path)
        cluster_ip_probe = probe_via_cluster_ip(
            host=sf_address,
            path="/",
            ingress_svc=config.ENDPOINT_INGRESS_SERVICE,
            timeout=timeout,
        )
        cip_healthy = cluster_ip_probe.status_code is not None and 200 <= cluster_ip_probe.status_code <= 399
        if not cip_healthy:
            all_healthy = False

    # Hop 0: Storefront — uses service port from k8s
    sf_service = f"storefront-{sf_name}"
    sf_port = _get_service_port(ns, sf_service)
    sf_health_path = _get_health_path_for_service(ns, sf_service)
    sf_url = f"http://{sf_service}.{ns}.svc.cluster.local:{sf_port}{sf_health_path}"
    sf_probe = probe(sf_url, timeout=timeout)
    sf_healthy = sf_probe.status_code is not None and 200 <= sf_probe.status_code <= 399
    hops.append(HopResult(
        name=f"{sf_service}:{sf_port}",
        probe=sf_probe,
        healthy=sf_healthy,
        depth=0,
        health_path=sf_health_path,
    ))
    if not sf_healthy:
        all_healthy = False

    if chain_depth < 1:
        return external_probe, cluster_ip_probe, hops, all_healthy

    # Hop 1: api-gateway — resolve port and health path from k8s
    gw_port = _get_service_port(ns, "api-gateway")
    gw_health_path = _get_health_path_for_service(ns, "api-gateway")
    gw_url = f"http://api-gateway.{ns}.svc.cluster.local:{gw_port}{gw_health_path}"
    gw_probe = probe(gw_url, timeout=timeout)
    gw_healthy = gw_probe.status_code is not None and 200 <= gw_probe.status_code <= 399
    hops.append(HopResult(
        name=f"api-gateway:{gw_port}",
        probe=gw_probe,
        healthy=gw_healthy,
        depth=1,
        health_path=gw_health_path,
    ))
    if not gw_healthy:
        all_healthy = False

    if chain_depth < 2:
        return external_probe, cluster_ip_probe, hops, all_healthy

    # Hop 2+: Subgraph services — use service port, not container port
    subgraphs = _discover_subgraph_services(ns)
    for svc_name, _container_port, _probe_health_path in subgraphs:
        svc_port = _get_service_port(ns, svc_name)
        health_path = _get_health_path_for_service(ns, svc_name)
        svc_url = f"http://{svc_name}.{ns}.svc.cluster.local:{svc_port}{health_path}"
        svc_probe = probe(svc_url, timeout=timeout)
        svc_healthy = svc_probe.status_code is not None and 200 <= svc_probe.status_code <= 399
        hops.append(HopResult(
            name=f"{svc_name}:{svc_port}",
            probe=svc_probe,
            healthy=svc_healthy,
            depth=2,
            health_path=health_path,
        ))
        if not svc_healthy:
            all_healthy = False

    return external_probe, cluster_ip_probe, hops, all_healthy


def _resolve_dns(hostname: str) -> str:
    """Resolve a hostname and return status string."""
    try:
        addrs = socket.getaddrinfo(hostname, None)
        ips = sorted({str(addr[4][0]) for addr in addrs})
        return f"OK → {', '.join(ips)}"
    except socket.gaierror as e:
        return f"FAILED — {e}"


def _collect_upstream_deps(pod_spec) -> list[tuple[str, str]]:
    """Extract upstream service hostnames from env vars in pod spec."""
    hosts: set[tuple[str, str]] = set()
    for container in (pod_spec.containers or []):
        for env in (container.env or []):
            if env.value:
                for match in _UPSTREAM_PATTERNS.finditer(env.value):
                    hosts.add((match.group(1).lower(), match.group(2)))
    return list(hosts)


def _check_upstream_service(protocol: str, host: str) -> str:
    """Check if an upstream service is reachable (TCP connect)."""
    default_ports = {
        "mongodb": 27017, "postgres": 5432, "mysql": 3306,
        "redis": 6379, "kafka": 9092, "amqp": 5672,
        "nats": 4222, "typesense": 8108,
    }
    port = default_ports.get(protocol, 80)
    if ":" in host:
        parts = host.rsplit(":", 1)
        host = parts[0]
        try:
            port = int(parts[1])
        except ValueError:
            pass
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.close()
        return f"OK (TCP connect to {host}:{port})"
    except Exception as e:
        return f"FAILED (TCP connect to {host}:{port}: {e})"


def _probe_spec_str(probe_spec) -> str:
    """Format a probe spec to a short string."""
    if probe_spec.http_get:
        port = probe_spec.http_get.port
        path = probe_spec.http_get.path or "/"
        return f"HTTP GET :{port}{path}"
    if probe_spec.tcp_socket:
        return f"TCP :{probe_spec.tcp_socket.port}"
    if probe_spec.exec:
        cmd = " ".join(probe_spec.exec.command or [])
        return f"exec({cmd[:40]})"
    return "unknown"


def _fmt_ms(seconds: float) -> str:
    """Format seconds as ms string."""
    if seconds <= 0:
        return "—"
    return f"{seconds * 1000:.0f}ms"


def _collect_pods_for_hop(svc_name: str, ns: str) -> list:
    """Find pods for a service by common label selectors."""
    core = k8s.CoreV1Api()
    for label in (f"app={svc_name}", f"app.kubernetes.io/name={svc_name}"):
        try:
            pods = core.list_namespaced_pod(ns, label_selector=label)
            if pods.items:
                return pods.items
        except Exception:
            logger.debug("Failed to list pods for %s in %s (label=%s)", svc_name, ns, label)
    return []


def _collect_apollo_metrics(ns: str) -> str | None:
    """Collect Apollo Router metrics from Prometheus. Returns markdown or None."""
    if not config.PROMETHEUS_URL:
        return None

    # Discovery: try known metric name patterns
    for prefix in ("apollo_router", "router"):
        test = prom_query(f'{prefix}_http_requests_total{{namespace="{ns}"}}')
        if not test:
            continue

        labels = f'namespace="{ns}"'
        parts = []

        req_rate = prom_scalar(f'sum(rate({prefix}_http_requests_total{{{labels}}}[5m]))')
        if req_rate is not None:
            parts.append(f"Request rate: {req_rate:.1f} req/s")

        p99 = prom_scalar(
            f'histogram_quantile(0.99, sum(rate({prefix}_http_request_duration_seconds_bucket{{{labels}}}[5m])) by (le))'
        )
        if p99 is not None:
            parts.append(f"p99 latency: {p99 * 1000:.0f}ms")

        err_rate = prom_scalar(
            f'sum(rate({prefix}_http_requests_total{{{labels},status=~"5.."}}[5m]))'
        )
        if err_rate is not None:
            err_pct = f" ({err_rate / req_rate * 100:.1f}%)" if req_rate else ""
            parts.append(f"Error rate: {err_rate:.1f} req/s{err_pct}")

        sessions = prom_scalar(f'{prefix}_session_count_active{{{labels}}}')
        if sessions is not None:
            parts.append(f"Active sessions: {int(sessions)}")

        if parts:
            return " | ".join(parts)

    return None


def _collect_ingress_context(
    sf_name: str,
    sf_address: str,
    ns: str,
    core: k8s.CoreV1Api,
) -> str | None:
    """Collect ingress and nginx controller context when external fails but pods are healthy.

    This scenario means nginx is not routing to the backend — collect ingress rules,
    nginx controller status, and relevant nginx logs.
    """
    from src.engine.sanitizer import redact_response_body

    net = k8s.NetworkingV1Api()
    lines = ["### Ingress & Nginx Context (external-only failure)"]
    has_content = False

    # --- Ingress resources for this host ---
    try:
        ingresses = net.list_namespaced_ingress(ns)
        matching = []
        for ing in ingresses.items:
            for rule in (ing.spec.rules or []):
                if rule.host and (rule.host == sf_address or sf_address in rule.host):
                    matching.append(ing)
                    break

        if matching:
            lines.append("#### Matching Ingresses")
            for ing in matching:
                name = ing.metadata.name
                annotations = ing.metadata.annotations or {}
                ann_str = ", ".join(
                    f"{k}={v}" for k, v in sorted(annotations.items())
                    if not k.startswith("kubectl.kubernetes.io")
                    and not k.startswith("meta.helm.sh")
                )
                lines.append(f"  **{name}**:")
                if ann_str:
                    lines.append(f"    Annotations: {ann_str}")
                tls_hosts = []
                for tls in (ing.spec.tls or []):
                    tls_hosts.extend(tls.hosts or [])
                    if tls.secret_name:
                        lines.append(f"    TLS Secret: {tls.secret_name}")
                if tls_hosts:
                    lines.append(f"    TLS Hosts: {', '.join(tls_hosts)}")
                for rule in (ing.spec.rules or []):
                    if rule.http:
                        for path in (rule.http.paths or []):
                            backend = path.backend
                            svc_info = ""
                            if backend.service:
                                port = backend.service.port
                                port_str = str(port.number or port.name) if port else "?"
                                svc_info = f"{backend.service.name}:{port_str}"
                            lines.append(
                                f"    Rule: {rule.host}{path.path or '/'} "
                                f"({path.path_type or '?'}) → {svc_info}"
                            )
            has_content = True
        else:
            lines.append(f"  No ingress found for host `{sf_address}` in namespace `{ns}`")
            has_content = True
    except Exception:
        logger.debug("Failed to list ingresses in %s", ns)

    # --- TLS secret status ---
    try:
        for ing in matching:
            for tls in (ing.spec.tls or []):
                if tls.secret_name:
                    try:
                        secret = core.read_namespaced_secret(tls.secret_name, ns)
                        has_cert = "tls.crt" in (secret.data or {})
                        has_key = "tls.key" in (secret.data or {})
                        lines.append(f"    Secret `{tls.secret_name}`: "
                                     f"{'exists' if has_cert and has_key else 'MISSING cert/key'}")
                    except k8s.ApiException as e:
                        if e.status == 404:
                            lines.append(f"    Secret `{tls.secret_name}`: NOT FOUND")
                        else:
                            lines.append(f"    Secret `{tls.secret_name}`: error ({e.reason})")
                    has_content = True
    except Exception:
        logger.warning("Failed to collect ingress context for %s in %s", sf_name, ns, exc_info=True)

    # --- Nginx ingress controller pod status & logs ---
    ingress_ns = None
    for try_ns in ("senteca-system", "ingress-nginx", "kube-system"):
        try:
            pods = core.list_namespaced_pod(
                try_ns, label_selector="app.kubernetes.io/name=ingress-nginx",
            )
            if pods.items:
                ingress_ns = try_ns
                break
        except Exception:
            logger.debug("Failed to find ingress controller in namespace %s", try_ns)
            continue

    if ingress_ns:
        try:
            pods = core.list_namespaced_pod(
                ingress_ns, label_selector="app.kubernetes.io/name=ingress-nginx",
            )
            ctrl_pods = [p for p in pods.items if "controller" in p.metadata.name]
            if ctrl_pods:
                pod = ctrl_pods[0]
                phase = pod.status.phase if pod.status else "Unknown"
                restarts = sum(
                    cs.restart_count for cs in (pod.status.container_statuses or [])
                )
                lines.append(f"#### Nginx Controller: {pod.metadata.name}")
                lines.append(f"  Phase={phase}, Restarts={restarts}, Node={pod.spec.node_name}")
                has_content = True

                # Fetch nginx logs filtered for the storefront address
                try:
                    logs = core.read_namespaced_pod_log(
                        pod.metadata.name, ingress_ns,
                        tail_lines=100, timestamps=True,
                    )
                    if logs.strip():
                        # Filter for relevant lines (the host or error lines)
                        all_lines = logs.strip().split("\n")
                        relevant = [
                            l for l in all_lines
                            if sf_address in l
                            or re.search(r"(?i)(error|no server|no upstream|backend|503|502|404)", l)
                        ]
                        if relevant:
                            display = redact_response_body("\n".join(relevant[-20:]))
                            lines.append(f"#### Nginx Logs (filtered)\n```\n{display}\n```")
                            has_content = True
                except Exception:
                    logger.debug("Failed to read nginx controller logs")
        except Exception:
            logger.debug("Failed to list nginx controller pods in %s", ingress_ns)

    # --- Storefront service endpoint status ---
    sf_service = f"storefront-{sf_name}"
    try:
        endpoints = core.read_namespaced_endpoints(sf_service, ns)
        ready_addrs = []
        not_ready_addrs = []
        for subset in (endpoints.subsets or []):
            ready_addrs.extend(subset.addresses or [])
            not_ready_addrs.extend(subset.not_ready_addresses or [])
        lines.append(f"#### Service Endpoints: {sf_service}")
        lines.append(f"  Ready: {len(ready_addrs)}, NotReady: {len(not_ready_addrs)}")
        for addr in ready_addrs[:5]:
            target = addr.target_ref
            pod_name = target.name if target else "?"
            lines.append(f"    {addr.ip} → {pod_name}")
        has_content = True
    except Exception:
        logger.debug("Failed to read endpoints for %s/%s", ns, sf_service)

    return "\n".join(lines) if has_content else None


def _collect_nginx_ingress_metrics(ns: str, host: str) -> str | None:
    """Collect nginx ingress controller metrics from Prometheus."""
    if not config.PROMETHEUS_URL:
        return None

    labels = f'namespace="{ns}",host="{host}"'
    parts = []

    # Request rate
    req_rate = prom_scalar(
        f'sum(rate(nginx_ingress_controller_requests{{{labels}}}[5m]))'
    )
    if req_rate is not None:
        parts.append(f"Request rate: {req_rate:.1f} req/s")

        # 5xx rate
        err_rate = prom_scalar(
            f'sum(rate(nginx_ingress_controller_requests{{{labels},status=~"5.."}}[5m]))'
        )
        if err_rate is not None:
            err_pct = f" ({err_rate / req_rate * 100:.1f}%)" if req_rate > 0 else ""
            parts.append(f"5xx rate: {err_rate:.2f} req/s{err_pct}")

        # 4xx rate
        err4_rate = prom_scalar(
            f'sum(rate(nginx_ingress_controller_requests{{{labels},status=~"4.."}}[5m]))'
        )
        if err4_rate is not None:
            err4_pct = f" ({err4_rate / req_rate * 100:.1f}%)" if req_rate > 0 else ""
            parts.append(f"4xx rate: {err4_rate:.2f} req/s{err4_pct}")

    # Upstream latency p50, p99
    for pct, label in [(0.50, "p50"), (0.99, "p99")]:
        val = prom_scalar(
            f'histogram_quantile({pct}, sum(rate('
            f'nginx_ingress_controller_response_duration_seconds_bucket{{{labels}}}[5m]'
            f')) by (le))'
        )
        if val is not None:
            parts.append(f"Upstream {label}: {val * 1000:.0f}ms")

    # Active connections (global, not per-host)
    conns = prom_scalar(
        'sum(nginx_ingress_controller_nginx_process_connections{state="active"})'
    )
    if conns is not None:
        parts.append(f"Active connections: {int(conns)}")

    if not parts:
        return None
    return " | ".join(parts)


def _discover_redis_in_namespace(ns: str, sf_name: str) -> list[tuple[str, int]]:
    """Discover Redis instances in the storefront namespace.

    Strategy:
    1. Look for well-known redis-master service (Bitnami helm chart convention)
    2. Fall back to parsing redis:// URLs from storefront pod env vars
    Returns list of (host, port) tuples.
    """
    core = k8s.CoreV1Api()

    # Strategy 1: well-known service names (Bitnami redis chart → redis-master)
    for svc_candidate in ("redis-master", "redis"):
        try:
            svc = core.read_namespaced_service(svc_candidate, ns)
            if svc.spec.ports:
                port = svc.spec.ports[0].port
                host = f"{svc_candidate}.{ns}.svc.cluster.local"
                return [(host, port)]
        except k8s.ApiException as e:
            if e.status != 404:
                logger.debug("Error checking redis service %s/%s: %s", ns, svc_candidate, e.reason)
        except Exception:
            logger.debug("Failed to check redis service %s/%s", ns, svc_candidate, exc_info=True)

    # Strategy 2: parse redis:// URLs from storefront pod env vars
    sf_svc_name = f"storefront-{sf_name}"
    pods = _collect_pods_for_hop(sf_svc_name, ns)
    if not pods:
        return []

    found: set[tuple[str, int]] = set()
    pod = pods[0]
    for container in (pod.spec.containers or []):
        for env in (container.env or []):
            if not env.value:
                continue
            for m in re.finditer(r'redis://(?:[^@/\s]*@)?([^:/@\s]+)(?::(\d+))?', env.value, re.IGNORECASE):
                host = m.group(1)
                port = int(m.group(2)) if m.group(2) else 6379
                found.add((host, port))

    return list(found)


def _check_redis_connectivity(host: str, port: int) -> dict:
    """Check Redis connectivity via raw TCP + RESP PING. No redis-py dependency."""
    result: dict = {"address": f"{host}:{port}", "status": "unknown", "connect_ms": 0.0, "ping_ms": 0.0}

    # TCP connect
    t0 = time.perf_counter()
    try:
        sock = socket.create_connection((host, port), timeout=5)
    except Exception as e:
        result["connect_ms"] = (time.perf_counter() - t0) * 1000
        result["status"] = f"connect_failed: {e}"
        return result

    result["connect_ms"] = (time.perf_counter() - t0) * 1000

    # RESP PING
    t1 = time.perf_counter()
    try:
        sock.sendall(b"*1\r\n$4\r\nPING\r\n")
        sock.settimeout(3)
        data = sock.recv(64)
        result["ping_ms"] = (time.perf_counter() - t1) * 1000
        if b"+PONG" in data:
            result["status"] = "ok"
        elif b"-NOAUTH" in data:
            # Redis requires auth — connection works but can't PING without password
            result["status"] = "ok (auth required, TCP reachable)"
        else:
            result["status"] = f"unexpected_response: {data[:32].decode(errors='replace')}"
    except Exception as e:
        result["ping_ms"] = (time.perf_counter() - t1) * 1000
        result["status"] = f"ping_failed: {e}"
    finally:
        sock.close()

    return result


def _collect_redis_metrics(ns: str) -> str | None:
    """Collect Redis metrics from Prometheus (redis-metrics exporter in namespace)."""
    if not config.PROMETHEUS_URL:
        return None

    labels = f'namespace="{ns}"'
    parts = []

    # Check if redis exporter is alive
    up = prom_scalar(f'redis_up{{{labels}}}')
    if up is not None and up < 1:
        parts.append("Exporter: DOWN")

    # Connected clients
    clients = prom_scalar(f'sum(redis_connected_clients{{{labels}}})')
    if clients is not None:
        parts.append(f"Clients: {int(clients)}")

    # Used memory + max memory
    mem = prom_scalar(f'sum(redis_memory_used_bytes{{{labels}}})')
    max_mem = prom_scalar(f'sum(redis_memory_max_bytes{{{labels}}})')
    if mem is not None:
        mem_mb = mem / (1024 * 1024)
        if max_mem and max_mem > 0:
            pct = mem / max_mem * 100
            parts.append(f"Memory: {mem_mb:.0f}MB ({pct:.0f}%)")
        else:
            parts.append(f"Memory: {mem_mb:.0f}MB")

    # Ops/sec
    ops = prom_scalar(f'sum(rate(redis_commands_processed_total{{{labels}}}[5m]))')
    if ops is not None:
        parts.append(f"Ops/s: {ops:.0f}")

    # Keyspace hit rate
    hits = prom_scalar(f'sum(rate(redis_keyspace_hits_total{{{labels}}}[5m]))')
    misses = prom_scalar(f'sum(rate(redis_keyspace_misses_total{{{labels}}}[5m]))')
    if hits is not None and misses is not None and (hits + misses) > 0:
        hit_rate = hits / (hits + misses) * 100
        parts.append(f"Hit rate: {hit_rate:.0f}%")

    # Blocked clients (potential issue indicator)
    blocked = prom_scalar(f'sum(redis_blocked_clients{{{labels}}})')
    if blocked is not None and blocked > 0:
        parts.append(f"Blocked: {int(blocked)}")

    # Evicted keys (memory pressure indicator)
    evicted = prom_scalar(f'sum(rate(redis_evicted_keys_total{{{labels}}}[5m]))')
    if evicted is not None and evicted > 0:
        parts.append(f"Evictions: {evicted:.1f}/s")

    # Rejected connections
    rejected = prom_scalar(f'sum(rate(redis_rejected_connections_total{{{labels}}}[5m]))')
    if rejected is not None and rejected > 0:
        parts.append(f"Rejected conns: {rejected:.1f}/s")

    if not parts:
        return None
    return " | ".join(parts)


def _collect_probe_correlation(
    probe_result: ProbeResult,
    core: k8s.CoreV1Api,
) -> str | None:
    """Correlate a probe with nginx access log entries using probe_id."""
    if not probe_result.probe_id:
        return None

    # Find nginx ingress controller pods
    ingress_ns = None
    for try_ns in ("senteca-system", "ingress-nginx", "kube-system"):
        try:
            pods = core.list_namespaced_pod(
                try_ns, label_selector="app.kubernetes.io/name=ingress-nginx",
            )
            if pods.items:
                ingress_ns = try_ns
                break
        except Exception:
            logger.debug("Failed to find ingress controller in namespace %s", try_ns)
            continue

    if not ingress_ns:
        return None

    try:
        pods = core.list_namespaced_pod(
            ingress_ns, label_selector="app.kubernetes.io/name=ingress-nginx",
        )
        ctrl_pods = [p for p in pods.items if "controller" in p.metadata.name]
        if not ctrl_pods:
            return None
    except Exception as e:
        logger.debug("Failed to list nginx controller pods for probe correlation: %s", e)
        return None

    pod = ctrl_pods[0]
    probe_id = probe_result.probe_id
    lines_parts = []

    try:
        logs = core.read_namespaced_pod_log(
            pod.metadata.name, ingress_ns,
            since_seconds=30, timestamps=True,
        )
        if not logs or not logs.strip():
            lines_parts.append(f"Probe ID: {probe_id}")
            lines_parts.append("Log Entry: (no logs in last 30s)")
            return "\n".join(lines_parts)

        all_lines = logs.strip().split("\n")
        matched = [l for l in all_lines if probe_id in l]

        lines_parts.append(f"Probe ID: {probe_id}")
        if matched:
            from src.engine.sanitizer import redact_response_body

            # Parse the matched log line for diagnostic info
            log_line = matched[-1]  # Last match (most recent)
            sanitized = redact_response_body(log_line[:300])
            lines_parts.append(f"Log Entry: {sanitized}")

            # Diagnostic interpretation
            if probe_result.status_code is None:
                lines_parts.append("→ Request reached nginx but probe timed out — likely slow backend/large payload")
            elif probe_result.status_code and probe_result.status_code >= 500:
                lines_parts.append("→ Request reached nginx, upstream returned error")
        else:
            lines_parts.append("Log Entry: (not found — request may not have reached nginx)")
            if probe_result.status_code is None:
                lines_parts.append("→ No log entry + timeout = request likely didn't reach nginx (LB/network issue)")
    except Exception as e:
        logger.debug("Failed to read nginx logs for probe correlation: %s", e)
        lines_parts.append(f"Probe ID: {probe_id}")
        lines_parts.append(f"Log Entry: (error reading logs: {e})")

    return "\n".join(lines_parts) if lines_parts else None


def _collect_nginx_health(core: k8s.CoreV1Api) -> str | None:
    """Collect general nginx health: error log entries and 5xx count."""
    # Find nginx ingress controller pods
    ingress_ns = None
    for try_ns in ("senteca-system", "ingress-nginx", "kube-system"):
        try:
            pods = core.list_namespaced_pod(
                try_ns, label_selector="app.kubernetes.io/name=ingress-nginx",
            )
            if pods.items:
                ingress_ns = try_ns
                break
        except Exception:
            logger.debug("Failed to find ingress controller in namespace %s", try_ns)
            continue

    if not ingress_ns:
        return None

    try:
        pods = core.list_namespaced_pod(
            ingress_ns, label_selector="app.kubernetes.io/name=ingress-nginx",
        )
        ctrl_pods = [p for p in pods.items if "controller" in p.metadata.name]
        if not ctrl_pods:
            return None
    except Exception as e:
        logger.debug("Failed to list nginx controller pods for health summary: %s", e)
        return None

    pod = ctrl_pods[0]
    parts = []

    try:
        logs = core.read_namespaced_pod_log(
            pod.metadata.name, ingress_ns,
            since_seconds=60, timestamps=True,
        )
        if not logs or not logs.strip():
            return None

        all_lines = logs.strip().split("\n")
        total = len(all_lines)

        # Count error/crit lines
        error_lines = [
            l for l in all_lines
            if re.search(r'\[error\]|\[crit\]|\[emerg\]', l)
        ]
        parts.append(f"Error log entries: {len(error_lines)}")

        # Count 5xx responses
        fivexx = [l for l in all_lines if re.search(r'" 5\d{2} ', l)]
        parts.append(f"5xx responses: {len(fivexx)}/{total} requests ({len(fivexx) / total * 100:.1f}%)" if total > 0 else "5xx responses: 0")

        # Count slow requests (>3s)
        slow_count = 0
        for l in all_lines:
            m = re.search(r'upstream_response_time="?(\d+\.?\d*)"?', l)
            if m:
                try:
                    if float(m.group(1)) > 3.0:
                        slow_count += 1
                except ValueError:
                    pass
        if slow_count > 0:
            parts.append(f"Slow requests (>3s): {slow_count}/{total} ({slow_count / total * 100:.2f}%)")

    except Exception as e:
        logger.debug("Failed to read nginx logs for health summary: %s", e)
        return None

    return " | ".join(parts) if parts else None


def _build_chain_context(
    sf_name: str,
    sf_address: str,
    external_probe: ProbeResult | None,
    cluster_ip_probe: ProbeResult | None,
    hops: list[HopResult],
    ns: str,
) -> str:
    """Build full markdown context for a failing chain."""
    from src.engine.sanitizer import redact_response_body

    core = k8s.CoreV1Api()
    apps = k8s.AppsV1Api()
    sections = []

    sections.append(f"## Critical Endpoint Chain: {sf_address} (storefront: {sf_name})")

    # Explanation note
    sections.append(
        "> **External Probe** connects to the LoadBalancer IP (real user path). "
        "**Internal Ingress Probe** connects to the nginx ClusterIP (in-cluster only). "
        "Comparing both reveals whether the issue is in the external network path or inside the cluster."
    )

    # --- External Probe (via LoadBalancer) ---
    if external_probe:
        lb_label = external_probe.ip_address or "?"
        ext_lines = [f"### External Probe (via LoadBalancer {lb_label})"]
        ext_lines.append(f"URL: {external_probe.url}")
        if external_probe.status_code:
            ext_lines.append(f"Status: {external_probe.status_code} {external_probe.reason}")
        else:
            ext_lines.append(f"Error: {external_probe.reason}")
        ext_lines.append(
            f"DNS: {_fmt_ms(external_probe.dns_time)} → {external_probe.ip_address or '?'}"
        )
        ext_lines.append(
            f"TCP: {_fmt_ms(external_probe.connect_time)} | "
            f"TLS: {_fmt_ms(external_probe.tls_time)} | "
            f"TTFB: {_fmt_ms(external_probe.ttfb)} | "
            f"Total: {_fmt_ms(external_probe.total_time)}"
        )
        if external_probe.tls_cert:
            cert = external_probe.tls_cert
            sans_str = ", ".join(cert.get("sans", [])[:5])
            ext_lines.append(
                f"TLS Cert: valid until {cert.get('expiry', '?')} "
                f"({cert.get('days_left', '?')}d), "
                f"issuer={cert.get('issuer', '?')}, SANs=[{sans_str}]"
            )
        if external_probe.headers:
            hdr_parts = [f"{k}={v}" for k, v in external_probe.headers.items()]
            ext_lines.append(f"Headers: {', '.join(hdr_parts)}")
        if external_probe.body:
            ext_lines.append(f"Body:\n```\n{redact_response_body(external_probe.body)}\n```")
        sections.append("\n".join(ext_lines))

    # --- Internal Ingress Probe (via ClusterIP) ---
    if cluster_ip_probe:
        cip_lines = [f"### Internal Ingress Probe (via ClusterIP {cluster_ip_probe.ip_address or '?'})"]
        cip_lines.append(f"URL: {cluster_ip_probe.url}")
        if cluster_ip_probe.status_code:
            cip_lines.append(f"Status: {cluster_ip_probe.status_code} {cluster_ip_probe.reason}")
        else:
            cip_lines.append(f"Error: {cluster_ip_probe.reason}")
        cip_lines.append(
            f"TCP: {_fmt_ms(cluster_ip_probe.connect_time)} | "
            f"TLS: {_fmt_ms(cluster_ip_probe.tls_time)} | "
            f"TTFB: {_fmt_ms(cluster_ip_probe.ttfb)} | "
            f"Total: {_fmt_ms(cluster_ip_probe.total_time)}"
        )
        sections.append("\n".join(cip_lines))

    # --- Probe Comparison ---
    if external_probe and cluster_ip_probe:
        cmp_lines = ["### Probe Comparison"]
        cmp_lines.append("| Metric | External (LB) | Internal (ClusterIP) | Delta |")
        cmp_lines.append("|--------|---------------|----------------------|-------|")

        ext_status = str(external_probe.status_code or external_probe.reason)
        cip_status = str(cluster_ip_probe.status_code or cluster_ip_probe.reason)
        status_match = "same" if ext_status == cip_status else "DIFFERENT"
        cmp_lines.append(f"| Status | {ext_status} | {cip_status} | {status_match} |")

        for metric, ext_val, cip_val in [
            ("TCP connect", external_probe.connect_time, cluster_ip_probe.connect_time),
            ("TLS handshake", external_probe.tls_time, cluster_ip_probe.tls_time),
            ("TTFB", external_probe.ttfb, cluster_ip_probe.ttfb),
            ("Total", external_probe.total_time, cluster_ip_probe.total_time),
        ]:
            if ext_val > 0 and cip_val > 0:
                ratio = ext_val / cip_val
                delta = f"{ratio:.1f}x" if ratio > 1.1 else "~same"
            else:
                delta = "—"
            cmp_lines.append(f"| {metric} | {_fmt_ms(ext_val)} | {_fmt_ms(cip_val)} | {delta} |")

        sections.append("\n".join(cmp_lines))

    # --- Probe Correlation (nginx access log) ---
    for probe_result, label in [
        (external_probe, "External"),
        (cluster_ip_probe, "Internal"),
    ]:
        if probe_result:
            correlation = _collect_probe_correlation(probe_result, core)
            if correlation:
                sections.append(f"#### Probe Correlation — {label}\n{correlation}")

    # --- Nginx Health (last 60s) ---
    nginx_health = _collect_nginx_health(core)
    if nginx_health:
        sections.append(f"#### Nginx Health (last 60s)\n{nginx_health}")

    # --- Chain Health Table ---
    table_lines = ["### Chain Health"]
    table_lines.append("| # | Service | Health Path | Status | TTFB | Details |")
    table_lines.append("|---|---------|-------------|--------|------|---------|")
    for i, hop in enumerate(hops):
        if hop.healthy:
            status = f"✓ {hop.probe.status_code}"
        elif hop.probe.status_code:
            status = f"✗ {hop.probe.status_code}"
        else:
            status = "✗ ERR"
        ttfb = _fmt_ms(hop.probe.ttfb)
        details = "" if hop.healthy else hop.probe.reason
        table_lines.append(f"| {i} | {hop.name} | {hop.health_path} | {status} | {ttfb} | {details} |")
    sections.append("\n".join(table_lines))

    # --- Collect pods for ALL hops → node + app metrics ---
    hop_pods: dict[str, list] = {}  # svc_name -> pods
    node_names: dict[str, str] = {}  # node_name -> first svc that runs on it
    for hop in hops:
        svc_name = hop.name.split(":")[0]
        pods_list = _collect_pods_for_hop(svc_name, ns)
        hop_pods[svc_name] = pods_list
        for pod in pods_list:
            if pod.spec.node_name and pod.spec.node_name not in node_names:
                node_names[pod.spec.node_name] = svc_name

    # --- Node Utilization ---
    if node_names:
        node_lines = ["### Node Utilization", "| Node | Metrics |", "|------|---------|"]
        for node_name in sorted(node_names):
            summary = get_node_metrics_summary(node_name)
            if summary:
                node_lines.append(f"| {node_name} | {summary} |")
        if len(node_lines) > 3:  # has at least one data row
            sections.append("\n".join(node_lines))

    # --- Application Metrics (Node.js) ---
    app_metric_lines = [
        "### Application Metrics (Node.js)",
        "| Service | Pod | Metrics |",
        "|---------|-----|---------|",
    ]
    has_app_metrics = False
    for hop in hops:
        svc_name = hop.name.split(":")[0]
        pods_list = hop_pods.get(svc_name, [])
        if not pods_list:
            continue
        pod = pods_list[0]
        summary = get_app_metrics_summary(pod.metadata.name, ns)
        if summary:
            app_metric_lines.append(f"| {svc_name} | {pod.metadata.name} | {summary} |")
            has_app_metrics = True
    if has_app_metrics:
        sections.append("\n".join(app_metric_lines))

    # --- Apollo Router Metrics ---
    apollo_summary = _collect_apollo_metrics(ns)
    if apollo_summary:
        sections.append(f"### Apollo Router Metrics\n{apollo_summary}")

    # --- Nginx Ingress Metrics (from Prometheus) ---
    nginx_metrics = _collect_nginx_ingress_metrics(ns, sf_address)
    if nginx_metrics:
        sections.append(f"### Nginx Ingress Metrics\n{nginx_metrics}")

    # --- Redis (storefront redirects) ---
    # Auto-discover Redis from storefront pod env vars
    redis_instances = _discover_redis_in_namespace(ns, sf_name)
    # Fallback to explicit config if no auto-discovery
    if not redis_instances and config.ENDPOINT_REDIS_SERVICE:
        addr = config.ENDPOINT_REDIS_SERVICE
        if ":" in addr:
            rh, rp_str = addr.rsplit(":", 1)
            try:
                redis_instances = [(rh, int(rp_str))]
            except ValueError:
                redis_instances = [(addr, 6379)]
        else:
            redis_instances = [(addr, 6379)]

    if redis_instances:
        redis_lines = ["### Redis (storefront redirects)"]
        for rhost, rport in redis_instances:
            result = _check_redis_connectivity(rhost, rport)
            redis_lines.append(
                f"  {result['address']}: {result['status']} "
                f"(connect={result['connect_ms']:.1f}ms, ping={result['ping_ms']:.1f}ms)"
            )
        # Prometheus metrics for Redis in this namespace
        redis_metrics = _collect_redis_metrics(ns)
        if redis_metrics:
            redis_lines.append(f"  Metrics: {redis_metrics}")
        sections.append("\n".join(redis_lines))

    # TODO: Postgres metrics collection (pg_stat_activity, connection pool, replication lag)
    # Will be added as a separate phase — requires discovering PG pods and their metrics endpoints

    # --- External-only failure: collect ingress/nginx context ---
    ext_failed = (
        external_probe is not None
        and (external_probe.status_code is None or external_probe.status_code >= 400)
    )
    cip_failed = (
        cluster_ip_probe is not None
        and (cluster_ip_probe.status_code is None or cluster_ip_probe.status_code >= 400)
    )
    internal_all_ok = all(h.healthy for h in hops) and not cip_failed
    if ext_failed and internal_all_ok:
        ingress_sections = _collect_ingress_context(sf_name, sf_address, ns, core)
        if ingress_sections:
            sections.append(ingress_sections)

    # --- All hops: collect k8s context (full for failing, lightweight for healthy) ---
    for hop in hops:
        is_failing = not hop.healthy
        header = f"### Failing: {hop.name}" if is_failing else f"### Hop: {hop.name} (healthy)"
        hop_sections = [header]
        svc_name = hop.name.split(":")[0]

        # Pods (reuse already-collected pods)
        pods_list = hop_pods.get(svc_name, [])
        try:
            if pods_list:
                pod_lines = ["#### Pods"]
                for pod in pods_list:
                    phase = pod.status.phase if pod.status else "Unknown"
                    statuses = pod.status.container_statuses or []
                    ready_count = sum(1 for cs in statuses if cs.ready)
                    total = len(statuses)
                    restarts = sum(cs.restart_count for cs in statuses)
                    pod_lines.append(
                        f"  {pod.metadata.name}: Phase={phase}, "
                        f"Ready={ready_count}/{total}, Restarts={restarts}"
                    )
                    if is_failing:
                        for cond in (pod.status.conditions or []):
                            if cond.type == "Ready" and cond.status != "True":
                                pod_lines.append(f"    NOT READY: {cond.reason}")
                        # Probes (only for failing hops)
                        for c in (pod.spec.containers or []):
                            probes = []
                            if c.readiness_probe:
                                probes.append(f"readiness={_probe_spec_str(c.readiness_probe)}")
                            if c.liveness_probe:
                                probes.append(f"liveness={_probe_spec_str(c.liveness_probe)}")
                            if probes:
                                pod_lines.append(f"    Probes: {', '.join(probes)}")
                hop_sections.append("\n".join(pod_lines))
        except Exception:
            logger.debug("Failed to list pods for %s in %s", svc_name, ns)

        # Deployment (only for failing hops)
        if is_failing:
            try:
                deploy = apps.read_namespaced_deployment(svc_name, ns)
                status = deploy.status
                spec_replicas = deploy.spec.replicas or 1
                ready_replicas = status.ready_replicas or 0
                updated_replicas = status.updated_replicas or 0
                unavailable = status.unavailable_replicas or 0
                deploy_lines = [
                    "#### Deployment",
                    f"  Replicas: {spec_replicas} desired, {ready_replicas} ready, "
                    f"{updated_replicas} updated, {unavailable} unavailable",
                ]
                for c in (deploy.spec.template.spec.containers or []):
                    deploy_lines.append(f"  Image: {c.image}")
                hop_sections.append("\n".join(deploy_lines))
            except Exception:
                logger.debug("Failed to read deployment %s in %s", svc_name, ns)

        # Events
        try:
            events = core.list_namespaced_event(
                ns, field_selector=f"involvedObject.name={svc_name}",
            )
            # Also get events for pods
            pod_events = []
            if pods_list:
                for p in pods_list[:3]:
                    pe = core.list_namespaced_event(
                        ns, field_selector=f"involvedObject.name={p.metadata.name}",
                    )
                    pod_events.extend(pe.items)

            all_events = events.items + pod_events
            if not is_failing:
                # Healthy hops: only Warning/Error events
                all_events = [e for e in all_events if e.type in ("Warning", "Error")]
            if all_events:
                all_events.sort(
                    key=lambda e: e.last_timestamp or e.event_time or datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True,
                )
                ev_lines = ["#### Events"]
                for e in all_events[:10]:
                    ev_lines.append(f"  [{e.type}] {e.reason}: {e.message}")
                hop_sections.append("\n".join(ev_lines))
        except Exception:
            logger.debug("Failed to list events for %s in %s", svc_name, ns)

        # Logs (error-prioritized)
        if pods_list:
            for pod in pods_list[:2]:
                try:
                    logs = core.read_namespaced_pod_log(
                        pod.metadata.name, ns,
                        tail_lines=30, timestamps=True,
                    )
                    if logs.strip():
                        lines = logs.strip().split("\n")
                        error_lines = [
                            l for l in lines
                            if re.search(r"(?i)(error|exception|fatal|panic|traceback)", l)
                        ]
                        if error_lines:
                            display = redact_response_body("\n".join(error_lines[-15:]))
                            hop_sections.append(
                                f"#### Logs (errors) — {pod.metadata.name}\n```\n{display}\n```"
                            )
                        elif is_failing:
                            # Full logs only for failing hops
                            display = redact_response_body("\n".join(lines[-15:]))
                            hop_sections.append(
                                f"#### Logs — {pod.metadata.name}\n```\n{display}\n```"
                            )
                except Exception as e:
                    logger.warning("Failed to collect logs for pod %s: %s", pod.metadata.name, e)

        # Upstream Dependencies (only for failing hops)
        if is_failing and pods_list:
            upstream_lines = []
            svc_fqdn = f"{svc_name}.{ns}.svc.cluster.local"
            dns_result = _resolve_dns(svc_fqdn)
            upstream_lines.append(f"Service DNS ({svc_fqdn}): {dns_result}")

            for pod in pods_list[:1]:
                deps = _collect_upstream_deps(pod.spec)
                checked: set[str] = set()
                for protocol, host in deps:
                    if host not in checked:
                        checked.add(host)
                        result = _check_upstream_service(protocol, host)
                        upstream_lines.append(f"{protocol}://{host}: {result}")

            if upstream_lines:
                hop_sections.append("#### Upstream Dependencies\n" + "\n".join(upstream_lines))

        # Rich application metrics for failing hops (heap trend, active resources)
        if is_failing and pods_list:
            try:
                rich_metrics = query_app_metrics(pods_list[0].metadata.name, ns)
                if rich_metrics:
                    # Strip the "## Application Metrics\n" header from query_app_metrics
                    metrics_body = rich_metrics.split("\n", 1)[1] if "\n" in rich_metrics else rich_metrics
                    hop_sections.append(f"#### Application Metrics (detailed)\n{metrics_body}")
            except Exception as e:
                logger.warning("Failed to collect app metrics for %s in %s: %s", svc_name, ns, e)

        # Only include healthy hops if they have meaningful context beyond just the header + pods
        if is_failing or len(hop_sections) > 2:
            sections.append("\n".join(hop_sections))

    return "\n\n".join(sections)


class CriticalEndpointScanner:
    name = "critical_endpoint"
    startup_delay = 15

    @property
    def enabled(self):
        return config.SCANNER_CRITICAL_ENDPOINT_ENABLED

    @property
    def interval_seconds(self):
        return config.CRITICAL_ENDPOINT_INTERVAL_SECONDS

    def scan(self) -> list[ScanResult]:
        results = []
        probed = 0
        chain_depth = config.CRITICAL_ENDPOINT_CHAIN_DEPTH

        for ns in config.get_namespaces():
            storefronts = _list_active_storefronts(ns)
            if not storefronts:
                continue

            sf_names = [sf.get("metadata", {}).get("name", "?") for sf in storefronts]
            logger.info(
                "Critical endpoint scanner: found %d active storefronts in %s: %s",
                len(storefronts), ns, ", ".join(sf_names),
            )

            for sf in storefronts:
                spec = sf.get("spec", {})
                sf_cr_name = sf.get("metadata", {}).get("name", "")
                sf_address = spec.get("address", "")

                if not sf_cr_name or not sf_address:
                    logger.debug("Skipping Storefront CR with missing name/address: %s", sf)
                    continue

                if sf_cr_name in config.CRITICAL_ENDPOINT_EXCLUDE_NAMES:
                    logger.debug("Skipping excluded storefront: %s", sf_cr_name)
                    continue

                probed += 1
                state_key = f"CriticalEndpoint:{ns}/storefront-{sf_cr_name}:{sf_address}"

                try:
                    external_probe, cluster_ip_probe, hops, all_healthy = _probe_chain(
                        sf_cr_name, sf_address, ns, chain_depth,
                    )
                except Exception:
                    logger.exception(
                        "Critical endpoint scanner: failed to probe chain for %s/%s",
                        ns, sf_cr_name,
                    )
                    continue

                # Log chain results for debugging
                hop_summary = ", ".join(
                    f"{h.name}={'OK' if h.healthy else h.probe.status_code or h.probe.reason}"
                    for h in hops
                )
                ext_status = ""
                if external_probe:
                    ext_ok_flag = external_probe.status_code is not None and 200 <= external_probe.status_code <= 399
                    ext_status = f"external={'OK' if ext_ok_flag else external_probe.status_code or external_probe.reason}, "
                logger.info("Chain %s/%s (%s): %s%s", ns, sf_cr_name, sf_address, ext_status, hop_summary)

                if all_healthy:
                    # Also check external and cluster IP probes
                    ext_ok = (
                        external_probe is None
                        or (external_probe.status_code is not None and 200 <= external_probe.status_code <= 399)
                    )
                    cip_ok = (
                        cluster_ip_probe is None
                        or (cluster_ip_probe.status_code is not None and 200 <= cluster_ip_probe.status_code <= 399)
                    )
                    if ext_ok and cip_ok:
                        results.append(ScanResult(
                            state_key=state_key,
                            title=f"Critical Endpoint Recovered: {sf_address}",
                            severity="info",
                            resource=f"Storefront/{sf_cr_name}",
                            namespace=ns,
                            issue_type="critical_endpoint",
                            auto_resolve=True,
                        ))
                        continue

                # Something is unhealthy — build context
                failing_names = [h.name for h in hops if not h.healthy]
                if external_probe and (external_probe.status_code is None or external_probe.status_code >= 400):
                    failing_names.insert(0, "external-lb")
                if cluster_ip_probe and (cluster_ip_probe.status_code is None or cluster_ip_probe.status_code >= 400):
                    failing_names.insert(0, "internal-clusterip")

                event_reason = ", ".join(
                    f"{h.name}: {h.probe.reason}" for h in hops if not h.healthy
                )
                if not event_reason and external_probe:
                    event_reason = f"external: {external_probe.reason}"

                full_chain_context = _build_chain_context(
                    sf_name=sf_cr_name,
                    sf_address=sf_address,
                    external_probe=external_probe,
                    cluster_ip_probe=cluster_ip_probe,
                    hops=hops,
                    ns=ns,
                )

                results.append(ScanResult(
                    state_key=state_key,
                    title=f"Critical Endpoint Down: {sf_address}",
                    severity="critical",
                    resource=f"Storefront/{sf_cr_name}",
                    namespace=ns,
                    issue_type="critical_endpoint",
                    context_override=full_chain_context,
                    event_reason=event_reason,
                ))

        unhealthy = [r for r in results if not r.auto_resolve]
        if unhealthy:
            issue_details = "; ".join(f"{r.resource} ({r.event_reason})" for r in unhealthy)
            logger.info("Critical endpoint scanner: probed %d storefronts, %d issues: %s", probed, len(unhealthy), issue_details)
        else:
            logger.info("Critical endpoint scanner: probed %d storefronts, all healthy", probed)
        return results

    def collect_daily_data(self) -> str | None:
        return None
