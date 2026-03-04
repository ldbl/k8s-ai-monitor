import json
import logging
import time

from src import config
from src.collectors import Collector
from src.engine import central_push
from src.engine.llm import analyze_daily_report, analyze_weekly_report
from src.engine.sanitizer import sanitize_dict
from src.engine.notifier import post_daily_report, post_weekly_report

logger = logging.getLogger(__name__)


def _save_report(collector_data, result, report_type: str = "daily"):
    """Best-effort save to SQLite. Never raises."""
    try:
        from src.handlers.startup import get_store
        store = get_store()
        rid = store.save_daily_report(
            cluster=config.CLUSTER_NAME,
            collector_data=collector_data,
            result=result,
            report_type=report_type,
        )
        logger.info("%s report saved (id=%d)", report_type.capitalize(), rid)
    except Exception:
        logger.debug("Failed to persist %s report", report_type, exc_info=True)


def run_daily_report():
    logger.info("Generating daily report")
    collector = Collector()
    data = collector.collect_daily_data()
    result = analyze_daily_report(data)
    _save_report(data, result)
    post_daily_report(result)
    central_push.push_report({
        "created_at": time.time(),
        "overall_status": result.parsed.get("overall_status", "") if result.parsed else "",
        "summary": result.parsed.get("summary", "") if result.parsed else "",
        "analysis_json": json.dumps(result.parsed) if result.parsed else "",
        "collector_data_json": json.dumps(sanitize_dict(data), default=str),
        "llm_model": result.model,
        "cost_usd": result.cost_usd or 0.0,
    })
    logger.info("Daily report sent (status=%s)",
                result.parsed.get("overall_status", "?") if result.parsed else "error")


def run_weekly_report():
    logger.info("Generating weekly report")
    from src.collectors.daily import collect_weekly_data
    data = collect_weekly_data()
    result = analyze_weekly_report(data)
    _save_report(data, result, report_type="weekly")
    post_weekly_report(result)
    central_push.push_report({
        "created_at": time.time(),
        "report_type": "weekly",
        "overall_status": result.parsed.get("overall_trend", "") if result.parsed else "",
        "summary": result.parsed.get("summary", "") if result.parsed else "",
        "analysis_json": json.dumps(result.parsed) if result.parsed else "",
        "collector_data_json": json.dumps(sanitize_dict(data), default=str),
        "llm_model": result.model,
        "cost_usd": result.cost_usd or 0.0,
    })
    logger.info("Weekly report sent (trend=%s)",
                result.parsed.get("overall_trend", "?") if result.parsed else "error")
