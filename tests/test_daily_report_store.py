"""Tests for daily report persistence in SqliteStore."""
import json
import time
import unittest
from dataclasses import dataclass

from src.engine.store.sqlite import SqliteStore


@dataclass
class FakeAnalysisResult:
    raw_text: str
    parsed: dict | None
    parse_error: bool
    model: str
    tokens_in: int
    tokens_out: int
    cost_usd: float | None
    latency_ms: float = 0.0


def _make_result(*, parsed=None, raw_text="raw llm output", parse_error=False,
                 model="claude-haiku-3", tokens_in=500, tokens_out=200,
                 cost_usd=0.001, latency_ms=450.0):
    if parsed is None and not parse_error:
        parsed = {
            "overall_status": "healthy",
            "confidence": 0.9,
            "summary": "All systems operational",
            "issues": [],
            "trends": [],
            "recommendations": [],
        }
    return FakeAnalysisResult(
        raw_text=raw_text, parsed=parsed, parse_error=parse_error,
        model=model, tokens_in=tokens_in, tokens_out=tokens_out,
        cost_usd=cost_usd, latency_ms=latency_ms,
    )


def _make_collector_data():
    return {
        "pods": [{"name": "web-1", "status": "Running"}],
        "nodes": [{"name": "node-1", "ready": True}],
        "events": [],
    }


class TestSaveDailyReport(unittest.TestCase):
    """Save + retrieve round-trip."""

    def setUp(self):
        self.store = SqliteStore(":memory:")

    def test_save_returns_id(self):
        rid = self.store.save_daily_report(
            cluster="test-cluster",
            collector_data=_make_collector_data(),
            result=_make_result(),
        )
        self.assertIsInstance(rid, int)
        self.assertGreater(rid, 0)

    def test_round_trip(self):
        data = _make_collector_data()
        result = _make_result()
        rid = self.store.save_daily_report(
            cluster="test-cluster", collector_data=data, result=result,
        )
        report = self.store.get_daily_report(rid)
        self.assertIsNotNone(report)
        self.assertEqual(report["id"], rid)
        self.assertEqual(report["cluster"], "test-cluster")
        self.assertEqual(report["overall_status"], "healthy")
        self.assertEqual(report["model"], "claude-haiku-3")
        self.assertEqual(report["tokens_in"], 500)
        self.assertEqual(report["tokens_out"], 200)
        self.assertAlmostEqual(report["cost_usd"], 0.001)
        self.assertAlmostEqual(report["latency_ms"], 450.0)
        self.assertFalse(report["parse_error"])
        self.assertEqual(report["collector_data"], data)
        self.assertEqual(report["analysis"]["overall_status"], "healthy")
        self.assertEqual(report["summary"], "All systems operational")
        self.assertEqual(report["analysis_raw"], "raw llm output")


class TestListDailyReports(unittest.TestCase):
    """List returns summaries without heavy fields, ordered DESC."""

    def setUp(self):
        self.store = SqliteStore(":memory:")

    def test_list_ordered_desc(self):
        for i in range(3):
            self.store.save_daily_report(
                cluster="c", collector_data=_make_collector_data(),
                result=_make_result(),
            )
        reports = self.store.list_daily_reports()
        self.assertEqual(len(reports), 3)
        # Newest first
        self.assertGreaterEqual(reports[0]["created_at"], reports[1]["created_at"])
        self.assertGreaterEqual(reports[1]["created_at"], reports[2]["created_at"])

    def test_list_respects_limit(self):
        for _ in range(5):
            self.store.save_daily_report(
                cluster="c", collector_data=_make_collector_data(),
                result=_make_result(),
            )
        reports = self.store.list_daily_reports(limit=2)
        self.assertEqual(len(reports), 2)

    def test_list_has_summary_no_heavy_fields(self):
        self.store.save_daily_report(
            cluster="c", collector_data=_make_collector_data(),
            result=_make_result(),
        )
        reports = self.store.list_daily_reports()
        r = reports[0]
        # Should have summary fields
        self.assertIn("id", r)
        self.assertIn("summary", r)
        self.assertIn("overall_status", r)
        self.assertIn("model", r)
        self.assertIn("cost_usd", r)
        self.assertIn("parse_error", r)
        # Should NOT have heavy fields
        self.assertNotIn("collector_data", r)
        self.assertNotIn("analysis_raw", r)
        self.assertNotIn("collector_data_json", r)


class TestGetDailyReport(unittest.TestCase):
    """Full detail with parsed JSON fields."""

    def setUp(self):
        self.store = SqliteStore(":memory:")

    def test_get_returns_all_fields(self):
        rid = self.store.save_daily_report(
            cluster="prod", collector_data=_make_collector_data(),
            result=_make_result(),
        )
        report = self.store.get_daily_report(rid)
        expected_keys = {
            "id", "created_at", "cluster", "overall_status", "summary",
            "model", "cost_usd", "parse_error", "report_type", "collector_data",
            "analysis", "analysis_raw", "tokens_in", "tokens_out", "latency_ms",
            "context_bytes", "truncated",
        }
        self.assertEqual(set(report.keys()), expected_keys)

    def test_get_nonexistent_returns_none(self):
        self.assertIsNone(self.store.get_daily_report(999))

    def test_collector_data_is_parsed_dict(self):
        data = _make_collector_data()
        rid = self.store.save_daily_report(
            cluster="c", collector_data=data, result=_make_result(),
        )
        report = self.store.get_daily_report(rid)
        self.assertIsInstance(report["collector_data"], dict)
        self.assertEqual(report["collector_data"]["pods"][0]["name"], "web-1")

    def test_context_bytes_set(self):
        data = _make_collector_data()
        rid = self.store.save_daily_report(
            cluster="c", collector_data=data, result=_make_result(),
        )
        report = self.store.get_daily_report(rid)
        expected = len(json.dumps(data, default=str).encode("utf-8"))
        self.assertEqual(report["context_bytes"], expected)


class TestCleanupDailyReports(unittest.TestCase):
    """Old records deleted, recent kept."""

    def setUp(self):
        self.store = SqliteStore(":memory:")

    def test_cleanup_removes_old(self):
        # Insert a record with old timestamp
        conn = self.store._get_conn()
        old_ts = time.time() - (31 * 86400)  # 31 days ago
        conn.execute(
            """INSERT INTO daily_reports
               (created_at, cluster, collector_data_json, overall_status,
                analysis_json, analysis_raw, parse_error,
                llm_model, tokens_in, tokens_out, cost_usd,
                latency_ms, context_bytes, truncated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (old_ts, "c", "{}", "healthy", "{}", "raw", False,
             "m", 0, 0, 0, 0, 0, False),
        )
        conn.commit()

        # Insert a recent record
        self.store.save_daily_report(
            cluster="c", collector_data=_make_collector_data(),
            result=_make_result(),
        )

        deleted = self.store.cleanup_daily_reports()
        self.assertEqual(deleted, 1)
        reports = self.store.list_daily_reports()
        self.assertEqual(len(reports), 1)

    def test_cleanup_keeps_recent(self):
        self.store.save_daily_report(
            cluster="c", collector_data=_make_collector_data(),
            result=_make_result(),
        )
        deleted = self.store.cleanup_daily_reports()
        self.assertEqual(deleted, 0)
        reports = self.store.list_daily_reports()
        self.assertEqual(len(reports), 1)


class TestSaveWithParseError(unittest.TestCase):
    """AnalysisResult with parsed=None, parse_error=True."""

    def setUp(self):
        self.store = SqliteStore(":memory:")

    def test_save_parse_error(self):
        result = _make_result(
            parsed=None, parse_error=True,
            raw_text="invalid json from LLM",
        )
        rid = self.store.save_daily_report(
            cluster="c", collector_data=_make_collector_data(),
            result=result,
        )
        report = self.store.get_daily_report(rid)
        self.assertTrue(report["parse_error"])
        self.assertIsNone(report["overall_status"])
        self.assertIsNone(report["analysis"])
        self.assertEqual(report["analysis_raw"], "invalid json from LLM")

    def test_list_shows_parse_error(self):
        result = _make_result(parsed=None, parse_error=True)
        self.store.save_daily_report(
            cluster="c", collector_data=_make_collector_data(),
            result=result,
        )
        reports = self.store.list_daily_reports()
        self.assertTrue(reports[0]["parse_error"])
        self.assertIsNone(reports[0]["summary"])


if __name__ == "__main__":
    unittest.main()
