"""Certificate scanner — extracted from main._scan_certificates."""
import logging
from datetime import datetime, timezone

from kubernetes import client as k8s

from src import config
from src.scanners._base import ScanResult

logger = logging.getLogger(__name__)


class CertificateScanner:
    name = "certificate"
    startup_delay = 90

    @property
    def enabled(self):
        return config.SCANNER_CERT_ENABLED

    @property
    def interval_seconds(self):
        return config.CERT_SCAN_INTERVAL_SECONDS

    def scan(self) -> list[ScanResult]:
        custom = k8s.CustomObjectsApi()

        try:
            certs = custom.list_cluster_custom_object("cert-manager.io", "v1", "certificates")
        except k8s.ApiException as e:
            if e.status == 404:
                logger.debug("cert-manager CRDs not available, skipping")
            else:
                logger.warning("Failed to list certificates: %s", e.reason)
            return []
        except Exception:
            logger.debug("cert-manager not available, skipping")
            return []

        now = datetime.now(timezone.utc)
        results = []

        for cert in certs.get("items", []):
            ns = cert["metadata"]["namespace"]
            if ns in config.EXCLUDE_NAMESPACES:
                continue
            name = cert["metadata"]["name"]
            status = cert.get("status", {})
            conditions = status.get("conditions", [])
            ready = next((c for c in conditions if c["type"] == "Ready"), None)
            not_after_str = status.get("notAfter", "")
            renewal_time_str = status.get("renewalTime", "")

            issue = None
            severity = "warning"

            if ready and ready["status"] != "True":
                # Grace period: cert-manager retries ACME errors automatically.
                # Skip if not-Ready for less than 10 minutes to avoid false positives.
                transition_str = ready.get("lastTransitionTime", "")
                if transition_str:
                    try:
                        transition = datetime.fromisoformat(transition_str.replace("Z", "+00:00"))
                        not_ready_minutes = (now - transition).total_seconds() / 60
                        if not_ready_minutes < 10:
                            logger.debug(
                                "Certificate %s/%s not-Ready for %.0fm (<10m grace), skipping",
                                ns, name, not_ready_minutes,
                            )
                            continue
                    except (ValueError, TypeError):
                        pass
                issue = f"Certificate not ready: {ready.get('reason', '?')} \u2014 {ready.get('message', '')}"
                severity = "critical"
            elif not_after_str:
                try:
                    not_after = datetime.fromisoformat(not_after_str.replace("Z", "+00:00"))
                    days_left = (not_after - now).days
                    if days_left <= config.CERT_EXPIRY_WARNING_DAYS:
                        issue = f"expires in {days_left} days ({not_after_str})"
                        severity = "critical" if days_left <= 7 else "warning"
                except (ValueError, TypeError):
                    pass

            if not issue and renewal_time_str:
                try:
                    renewal_time = datetime.fromisoformat(renewal_time_str.replace("Z", "+00:00"))
                    if renewal_time < now:
                        hours_overdue = (now - renewal_time).total_seconds() / 3600
                        if hours_overdue > 24:
                            issue = f"Renewal overdue by {hours_overdue:.0f}h"
                            severity = "warning"
                except (ValueError, TypeError):
                    pass

            if not issue:
                results.append(ScanResult(
                    state_key=f"Certificate:{ns}/{name}",
                    title=f"Certificate OK: {name}",
                    severity="info",
                    resource=f"Certificate/{name}",
                    namespace=ns,
                    issue_type="certificate",
                    auto_resolve=True,
                ))
                continue

            # Build context
            context = (
                f"## Certificate Alert\n"
                f"Certificate: {ns}/{name}\n"
                f"Secret: {cert['spec'].get('secretName', '?')}\n"
                f"Issuer: {cert['spec'].get('issuerRef', {}).get('name', '?')} "
                f"(kind={cert['spec'].get('issuerRef', {}).get('kind', 'Issuer')})\n"
                f"DNS Names: {cert['spec'].get('dnsNames', [])}\n"
                f"Issue: {issue}\n"
                f"notAfter: {not_after_str or '?'}, renewalTime: {renewal_time_str or '?'}\n"
                f"Conditions:\n"
            )
            for c in conditions:
                context += f"  {c['type']}: {c['status']} \u2014 {c.get('message', '')}\n"

            # CertificateRequest events on failure
            if ready and ready["status"] != "True":
                try:
                    crs = custom.list_namespaced_custom_object("cert-manager.io", "v1", ns, "certificaterequests")
                    for cr in crs.get("items", []):
                        owners = cr.get("metadata", {}).get("ownerReferences", [])
                        if any(o.get("name") == name for o in owners):
                            cr_name = cr["metadata"]["name"]
                            cr_conds = cr.get("status", {}).get("conditions", [])
                            context += f"\nCertificateRequest: {cr_name}\n"
                            for cc in cr_conds:
                                context += f"  {cc['type']}: {cc['status']} \u2014 {cc.get('message', '')}\n"
                except Exception:
                    logger.debug("Failed to list CertificateRequests for %s/%s", ns, name)

            results.append(ScanResult(
                state_key=f"Certificate:{ns}/{name}",
                title=f"Certificate: {issue}",
                severity=severity,
                resource=f"Certificate/{name}",
                namespace=ns,
                issue_type="certificate",
                context_override=context,
            ))

        issues = [r for r in results if not r.auto_resolve]
        ok_count = len(results) - len(issues)
        if issues:
            details = "; ".join(f"{r.namespace}/{r.resource}: {r.title}" for r in issues)
            logger.info("Certificate scanner: %d issues found, %d OK: %s", len(issues), ok_count, details)
        else:
            logger.info("Certificate scanner: all %d certificates OK", ok_count)
        return results

    def collect_daily_data(self) -> str | None:
        return None  # daily cert data handled by collectors/daily.py
