---
name: cluster-status
description: Check cluster health, investigate issues, show what's happening. Use when the user asks about cluster status, problems, health, or says "what's happening".
---

# Cluster Status Check

When the user asks about cluster health, status, or "what's happening", follow this playbook using the k8s-monitor MCP tools.

## Step 1: Overview
Call `cluster_health()` to get the full picture:
- Node status (Ready/NotReady)
- Pod issues across namespaces
- Active incidents from the incident store
- Recent warning events
- Backup job status
- Latest daily report summary

## Step 2: Drill Down (if issues found)
For each namespace with problems, call `investigate(namespace=<ns>)` to get:
- Detailed pod states and container statuses
- Warning events with context
- Prometheus metrics (CPU, memory, restarts)
- Error logs from Elasticsearch
- Trace stats from Uptrace

## Step 3: Targeted Investigation (if specific pod errors)
- `search_logs(namespace=<ns>, pod_pattern="*<pod>*", errors_only=true)` — get error logs for failing pods
- `get_service_stats(service=<name>)` — check error rate and latency for degraded services
- `query_metrics(namespace=<ns>, pod=<pod>)` — get CPU/memory/restart metrics

## Step 4: Cross-Reference
Correlate findings across data sources:
- Log errors + metric spikes at the same time → likely root cause
- Trace errors + pod restarts → service instability
- High memory + OOMKilled → resource limits too low
- Warning events + incident history → recurring issue

## Step 5: Report
Present a structured summary:
1. **Overall Status**: healthy / degraded / critical
2. **Active Issues**: list each with severity and namespace
3. **Key Metrics**: notable CPU/memory/error rate numbers
4. **Recommended Actions**: prioritized next steps (P1/P2/P3)
5. **Blind Spots**: note any data sources showing "not_configured"

Keep it concise. Lead with what matters most. Use tables for multi-pod comparisons.
