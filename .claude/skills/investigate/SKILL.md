---
name: investigate
description: Deep investigation of a specific issue, service, or namespace. Use when the user asks to investigate, debug, or troubleshoot something specific.
argument-hint: "[namespace] [pod/service]"
---

# Deep Investigation

When the user asks to investigate, debug, or troubleshoot a specific issue, follow this playbook using the k8s-monitor MCP tools.

## Step 1: Determine Scope
Parse the user's request for:
- **Namespace** (required): e.g. "production", "staging"
- **Pod/Service** (optional): specific workload to focus on
- **Time window** (optional): default 30 minutes, extend to 60+ for intermittent issues

## Step 2: Multi-Source Data Gathering
Call `investigate(namespace=<ns>, pod=<pod>, since_minutes=<window>)` to get:
- Pod statuses and container states
- Warning events
- Related incidents
- CPU/memory/restart metrics
- Error logs
- Trace statistics

## Step 3: Deep Dive
Based on initial findings, drill deeper:

**For log errors:**
- `search_logs(namespace=<ns>, pod_pattern="*<pod>*", query="<error pattern>")` — search for specific error messages
- `search_logs(namespace=<ns>, errors_only=true, since_minutes=60)` — broader error scan

**For performance issues:**
- `query_metrics(namespace=<ns>, pod=<pod>, since_minutes=60)` — extended metrics window
- `query_metrics(query="<custom PromQL>")` — custom metric queries
- `search_traces(service=<name>, slow=true, min_duration_ms=500)` — find slow operations

**For service errors:**
- `get_service_stats(service=<name>)` — error rate and latency percentiles
- `search_traces(service=<name>, errors_only=true)` — trace-level error details

**For recurring issues:**
- `list_incidents(status="all")` — check incident history
- `get_incident(incident_id=<id>)` — read past analysis

## Step 4: Build Timeline
Correlate events across sources:
1. When did the problem start? (first error log, first warning event)
2. What changed? (new deployment, config change, traffic spike)
3. What's the blast radius? (one pod, one service, entire namespace)
4. Is it getting worse or stabilizing?

## Step 5: Root Cause Patterns
Look for common patterns:
- **OOMKilled**: high memory + terminated with exit code 137 → increase memory limits
- **CrashLoopBackOff**: repeated restarts + error logs → check logs for startup failures
- **Dependency failure**: trace errors pointing to external service → check that service
- **Resource exhaustion**: high CPU/memory across pods → scale up or optimize
- **Config error**: recent deployment + immediate failures → check recent changes
- **Network issues**: connection timeouts in logs + trace errors → check network policies

## Step 6: Present Findings
Structure the response as:
1. **Issue Summary**: one-line description
2. **Root Cause**: what's causing the problem (confirmed or hypothesis)
3. **Timeline**: when it started, key events
4. **Impact**: what's affected, user-facing impact
5. **Evidence**: key log lines, metrics, trace data
6. **Actions**: prioritized remediation steps
   - **P1** (immediate): restart pod, rollback deployment
   - **P2** (short-term): fix config, adjust resources
   - **P3** (long-term): add monitoring, improve resilience

Be specific. Quote actual log messages and metric values. Don't speculate without evidence.
