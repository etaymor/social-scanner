"""Tests for daily_report.py — daily analytics orchestration and report generation."""

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest

from pipeline import db
from pipeline.intelligence import DIMENSIONS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts(hours_ago: float = 0) -> str:
    """Return a UTC timestamp string *hours_ago* in the past."""
    dt = datetime.now(UTC) - timedelta(hours=hours_ago)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _insert_slideshow(
    conn: sqlite3.Connection,
    city_id: int,
    *,
    hook_text: str = "Top 5 hidden gems",
    category: str = "food_and_drink",
    fmt: str = "listicle",
    posted_hours_ago: float = 72,
    postiz_post_id: str | None = "post-abc",
    tiktok_release_id: str | None = "rel-123",
    publish_status: str = "published",
    visual_style: str | None = None,
    cta_text: str | None = None,
) -> int:
    """Insert a slideshow and return its ID."""
    posted_at = _ts(posted_hours_ago)
    cur = conn.execute(
        """INSERT INTO slideshows
           (city_id, category, format, hook_text, slide_count, output_dir,
            posted_at, postiz_post_id, tiktok_release_id, publish_status,
            visual_style, cta_text)
           VALUES (?, ?, ?, ?, 5, '/tmp/out', ?, ?, ?, ?, ?, ?)""",
        (
            city_id, category, fmt, hook_text, posted_at,
            postiz_post_id, tiktok_release_id, publish_status,
            visual_style, cta_text,
        ),
    )
    return cur.lastrowid


def _insert_analytics(
    conn: sqlite3.Connection,
    slideshow_id: int,
    views: int = 5000,
    *,
    likes: int = 100,
    comments: int = 10,
    shares: int = 5,
    saves: int = 20,
    views_estimated: bool = False,
) -> None:
    """Insert a slideshow_analytics row."""
    conn.execute(
        """INSERT INTO slideshow_analytics
           (slideshow_id, fetched_at, views, likes, comments, shares, saves, views_estimated)
           VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)""",
        (slideshow_id, views, likes, comments, shares, saves, views_estimated),
    )


def _insert_platform_stats(
    conn: sqlite3.Connection,
    *,
    followers: int = 1000,
    total_views: int = 50000,
    total_likes: int = 3000,
    videos: int = 20,
    hours_ago: float = 0,
) -> None:
    """Insert a platform_stats row."""
    fetched_at = _ts(hours_ago)
    conn.execute(
        """INSERT INTO platform_stats
           (fetched_at, followers, total_views, total_likes, recent_comments, recent_shares, videos)
           VALUES (?, ?, ?, ?, 0, 0, ?)""",
        (fetched_at, followers, total_views, total_likes, videos),
    )


def _insert_rc_snapshot(
    conn: sqlite3.Connection,
    *,
    mrr: float = 100.0,
    active_trials: int = 5,
    active_subscriptions: int = 10,
    revenue: float = 200.0,
    hours_ago: float = 0,
) -> None:
    """Insert an rc_snapshots row."""
    fetched_at = _ts(hours_ago)
    conn.execute(
        """INSERT INTO rc_snapshots
           (fetched_at, mrr, active_trials, active_subscriptions, active_users, new_customers, revenue)
           VALUES (?, ?, ?, ?, 50, 3, ?)""",
        (fetched_at, mrr, active_trials, active_subscriptions, revenue),
    )


def _insert_performance(
    conn: sqlite3.Connection,
    slideshow_id: int,
    *,
    views_at_48h: int = 5000,
    conversions: int = 0,
    composite_score: float = 1.0,
    decision_tag: str = "keep",
) -> None:
    """Insert a slideshow_performance row."""
    conn.execute(
        """INSERT INTO slideshow_performance
           (slideshow_id, evaluated_at, views_at_48h, views_latest,
            conversions, conversion_rate, composite_score, decision_tag)
           VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, 0.0, ?, ?)""",
        (slideshow_id, views_at_48h, views_at_48h, conversions, composite_score, decision_tag),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def report_date():
    return datetime.now(UTC).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Test: Full report generation with mocked API responses
# ---------------------------------------------------------------------------


class TestFullReport:
    """Full report generates correctly with mocked API responses."""

    def test_full_report_with_all_phases(self, conn, city_id, tmp_path, report_date):
        """All three phases execute and produce a complete markdown report."""
        # Seed some data
        sid = _insert_slideshow(conn, city_id, hook_text="Top 5 spots in TestCity")
        _insert_analytics(conn, sid, views=15000, likes=300, comments=20, shares=10)
        _insert_platform_stats(conn, followers=1000, total_views=50000, hours_ago=24)
        _insert_platform_stats(conn, followers=1050, total_views=55000)
        conn.commit()

        from daily_report import (
            generate_report,
            run_phase1,
            run_phase2,
            run_phase3,
        )

        # Mock external API calls in Phase 1
        with mock.patch("daily_report.analytics") as mock_analytics:
            mock_analytics.fetch_posts.return_value = []
            mock_analytics.connect_release_ids.return_value = 0
            mock_analytics.fetch_post_analytics.return_value = 1
            mock_analytics.fetch_platform_stats.return_value = {"followers": 1050}
            mock_analytics.detect_stale_drafts.return_value = 0

            phase1 = run_phase1(conn, days=3)

        # Phase 2 — RC not configured
        with mock.patch("daily_report.config") as mock_config:
            mock_config.REVENUECAT_V2_SECRET_KEY = ""
            mock_config.ANALYTICS_LOOKBACK_DAYS = 3
            phase2 = run_phase2(conn, days=3)

        # Phase 3 — mock intelligence
        with mock.patch("daily_report.intelligence") as mock_intel:
            mock_intel.DIMENSIONS = DIMENSIONS
            mock_intel.evaluate_slideshows.return_value = 1
            mock_intel.check_circuit_breaker.return_value = False
            mock_intel.read_weights.return_value = {d: {} for d in DIMENSIONS}
            mock_intel.compute_dimension_weights.return_value = {d: {} for d in DIMENSIONS}
            mock_intel.write_weights.return_value = None

            phase3 = run_phase3(conn)

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "# Atlasi Daily Analytics Report" in report
        assert report_date in report
        assert "Slideshow Performance" in report
        assert "Platform Growth" in report
        assert "Weight Changes" in report
        assert "Hook Recommendations" in report
        assert "Run Summary" in report

    def test_report_includes_slideshow_data(self, conn, city_id, report_date):
        """The performance table includes slideshow rows."""
        sid = _insert_slideshow(conn, city_id, hook_text="Hidden cafes in TestCity")
        _insert_analytics(conn, sid, views=12000, likes=250)
        conn.commit()

        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 1, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 1, "previous_weights": {}, "new_weights": {}, "evaluated": 1}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "Hidden cafes in TestCity" in report
        assert "12,000" in report


# ---------------------------------------------------------------------------
# Test: Postiz-only mode (RC not configured)
# ---------------------------------------------------------------------------


class TestPostizOnlyMode:
    """Report generates correctly when RevenueCat is not configured."""

    def test_no_rc_sections_when_not_configured(self, conn, city_id, report_date):
        """RC sections are omitted when RC is not configured."""
        sid = _insert_slideshow(conn, city_id)
        _insert_analytics(conn, sid)
        conn.commit()

        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 1, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 5, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "RevenueCat Summary" not in report
        assert "Conversion Attribution" not in report

    def test_run_phase2_skips_when_not_configured(self, conn, city_id):
        """Phase 2 runner returns rc_configured=False when key is empty."""
        from daily_report import run_phase2

        with mock.patch("daily_report.config") as mock_config:
            mock_config.REVENUECAT_V2_SECRET_KEY = ""
            result = run_phase2(conn, days=3)

        assert result["rc_configured"] is False
        assert result["rc_snapshot"] is None
        assert result["conversions_attributed"] == 0


# ---------------------------------------------------------------------------
# Test: Cold-start notice
# ---------------------------------------------------------------------------


class TestColdStart:
    """Report shows cold-start notice when < 10 published posts."""

    def test_cold_start_with_few_posts(self, conn, city_id, report_date):
        """Cold-start section appears when post_count < 10."""
        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 3, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "Cold Start" in report
        assert "3 published post(s)" in report
        assert "Weights not adjusted" in report

    def test_no_cold_start_with_enough_posts(self, conn, city_id, report_date):
        """Cold-start section does NOT appear when post_count >= 10."""
        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 15, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "Cold Start" not in report


# ---------------------------------------------------------------------------
# Test: Report file saved to correct path
# ---------------------------------------------------------------------------


class TestReportFileSave:
    """Report file is saved to reports/YYYY-MM-DD.md."""

    def test_save_report_creates_file(self, tmp_path, report_date):
        from daily_report import save_report

        # Temporarily change working directory to tmp_path
        import os
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            path = save_report("# Test report content", report_date)
            assert path.exists()
            assert path.name == f"{report_date}.md"
            assert path.parent.name == "reports"
            assert path.read_text() == "# Test report content"
        finally:
            os.chdir(original_cwd)

    def test_save_report_creates_reports_dir(self, tmp_path, report_date):
        from daily_report import save_report

        import os
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            assert not (tmp_path / "reports").exists()
            save_report("# Test", report_date)
            assert (tmp_path / "reports").exists()
            assert (tmp_path / "reports").is_dir()
        finally:
            os.chdir(original_cwd)


# ---------------------------------------------------------------------------
# Test: API failure handling (partial reports)
# ---------------------------------------------------------------------------


class TestPartialReport:
    """Phase failures produce partial reports instead of crashing."""

    def test_phase1_error_captured(self, conn, city_id):
        """Phase 1 error is captured and doesn't crash the runner."""
        from daily_report import run_phase1

        with mock.patch("daily_report.analytics") as mock_analytics:
            mock_analytics.fetch_posts.side_effect = RuntimeError("Postiz timeout")
            result = run_phase1(conn, days=3)

        assert result["error"] is not None
        assert "Postiz timeout" in result["error"]

    def test_phase2_error_captured(self, conn, city_id):
        """Phase 2 error is captured and doesn't crash the runner."""
        from daily_report import run_phase2

        with mock.patch("daily_report.config") as mock_config, \
             mock.patch("daily_report.conversions") as mock_conv:
            mock_config.REVENUECAT_V2_SECRET_KEY = "sk_test"
            mock_conv.fetch_rc_snapshot.side_effect = RuntimeError("RC timeout")
            result = run_phase2(conn, days=3)

        assert result["error"] is not None
        assert "RC timeout" in result["error"]

    def test_phase3_error_captured(self, conn, city_id):
        """Phase 3 error is captured and doesn't crash the runner."""
        from daily_report import run_phase3

        phase2 = {"rc_configured": False}
        with mock.patch("daily_report.intelligence") as mock_intel:
            mock_intel.evaluate_slideshows.side_effect = RuntimeError("DB locked")
            result = run_phase3(conn)

        assert result["error"] is not None
        assert "DB locked" in result["error"]

    def test_report_with_phase1_error_still_generates(self, conn, city_id, report_date):
        """Report generates even when Phase 1 had errors."""
        from daily_report import generate_report

        phase1 = {
            "release_ids_connected": 0,
            "analytics_upserted": 0,
            "stale_marked": 0,
            "error": "Postiz API timed out",
        }
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 0, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "# Atlasi Daily Analytics Report" in report
        assert "Warnings" in report
        assert "Postiz API timed out" in report

    def test_auth_errors_propagate_from_phase1(self, conn, city_id):
        """AnalyticsAuthError re-raises from Phase 1 for CLI handling."""
        from daily_report import run_phase1
        from pipeline.analytics import AnalyticsAuthError

        with mock.patch("daily_report.analytics") as mock_analytics:
            mock_analytics.fetch_posts.side_effect = AnalyticsAuthError("401")
            with pytest.raises(AnalyticsAuthError):
                run_phase1(conn, days=3)

    def test_auth_errors_propagate_from_phase2(self, conn, city_id):
        """RevenueCatAuthError re-raises from Phase 2 for CLI handling."""
        from daily_report import run_phase2
        from pipeline.conversions import RevenueCatAuthError

        with mock.patch("daily_report.config") as mock_config, \
             mock.patch("daily_report.conversions") as mock_conv:
            mock_config.REVENUECAT_V2_SECRET_KEY = "sk_test"
            mock_conv.fetch_rc_snapshot.side_effect = RevenueCatAuthError("403")
            with pytest.raises(RevenueCatAuthError):
                run_phase2(conn, days=3)


# ---------------------------------------------------------------------------
# Test: Empty date range
# ---------------------------------------------------------------------------


class TestEmptyDateRange:
    """Empty date range produces a valid minimal report."""

    def test_no_slideshows_produces_valid_report(self, conn, city_id, report_date):
        """When no slideshows exist, the report is still valid markdown."""
        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 0, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "# Atlasi Daily Analytics Report" in report
        assert "Slideshow Performance" in report
        assert "No slideshows posted" in report
        assert "Weight Changes" in report
        assert "No weight changes" in report

    def test_no_platform_stats_produces_valid_report(self, conn, city_id, report_date):
        """When no platform stats exist, the section handles gracefully."""
        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 0, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "Platform Growth" in report
        assert "No platform stats available" in report


# ---------------------------------------------------------------------------
# Test: Weight changes section
# ---------------------------------------------------------------------------


class TestWeightChanges:
    """Weight changes section reflects actual changes from intelligence module."""

    def test_weight_changes_shown_in_report(self, conn, city_id, report_date):
        """When weights change, the changes appear in the report."""
        from daily_report import generate_report

        prev = {"category": {"food_and_drink": 1.0}, "city": {}}
        curr = {"category": {"food_and_drink": 1.15}, "city": {}}

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {
            "post_count": 15,
            "previous_weights": prev,
            "new_weights": curr,
            "evaluated": 1,
        }

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "food_and_drink" in report
        assert "promoted" in report
        assert "+0.1500" in report

    def test_no_weight_changes_message(self, conn, city_id, report_date):
        """When weights don't change, a 'no changes' message appears."""
        from daily_report import generate_report

        same = {"category": {"food_and_drink": 1.0}}

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {
            "post_count": 15,
            "previous_weights": same,
            "new_weights": same,
            "evaluated": 0,
        }

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "No weight changes" in report


# ---------------------------------------------------------------------------
# Test: RC configured mode
# ---------------------------------------------------------------------------


class TestRCConfigured:
    """Report sections for RevenueCat when configured."""

    def test_rc_sections_included(self, conn, city_id, report_date):
        """RevenueCat sections appear when configured."""
        _insert_rc_snapshot(conn, hours_ago=24, mrr=90.0, active_trials=3, revenue=180.0)
        _insert_rc_snapshot(conn, mrr=100.0, active_trials=5, revenue=200.0)
        _insert_platform_stats(conn, followers=1000, total_views=50000)
        conn.commit()

        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {
            "rc_configured": True,
            "rc_snapshot": {"mrr": 100.0, "active_trials": 5, "active_subscriptions": 10, "revenue": 200.0},
            "rc_deltas": {"mrr": 10.0, "active_trials": 2, "active_subscriptions": 1, "revenue": 20.0},
            "conversions_attributed": 2,
        }
        phase3 = {"post_count": 15, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "RevenueCat Summary" in report
        assert "$100.00" in report
        assert "Funnel Diagnosis" in report

    def test_conversion_attribution_table(self, conn, city_id, report_date):
        """Conversion attribution table appears when there are conversions."""
        sid = _insert_slideshow(conn, city_id, hook_text="Best brunch spots")
        _insert_analytics(conn, sid, views=20000)
        _insert_performance(conn, sid, views_at_48h=20000, conversions=3, decision_tag="scale")
        conn.commit()

        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {
            "rc_configured": True,
            "rc_snapshot": {"mrr": 100.0, "active_trials": 5, "active_subscriptions": 10, "revenue": 200.0},
            "rc_deltas": None,
            "conversions_attributed": 3,
        }
        phase3 = {"post_count": 15, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "Conversion Attribution" in report
        assert "Best brunch spots" in report

    def test_slideshow_table_includes_conversions_column(self, conn, city_id, report_date):
        """When RC is configured, the performance table has a Conversions column."""
        sid = _insert_slideshow(conn, city_id)
        _insert_analytics(conn, sid)
        conn.commit()

        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {
            "rc_configured": True,
            "rc_snapshot": {"mrr": 50.0, "active_trials": 1, "active_subscriptions": 5, "revenue": 100.0},
            "rc_deltas": None,
            "conversions_attributed": 0,
        }
        phase3 = {"post_count": 15, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        # Check that the header row has Conversions
        assert "Conversions" in report


# ---------------------------------------------------------------------------
# Test: Circuit breaker in report
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    """Circuit breaker status is reflected in the report."""

    def test_circuit_breaker_warning_in_report(self, conn, city_id, report_date):
        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {
            "post_count": 20,
            "previous_weights": {},
            "new_weights": {},
            "evaluated": 0,
            "circuit_breaker_tripped": True,
        }

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "CIRCUIT BREAKER TRIPPED" in report
        assert "reset to 1.0" in report


# ---------------------------------------------------------------------------
# Test: Hook recommendations
# ---------------------------------------------------------------------------


class TestHookRecommendations:
    """Top-performing hooks appear as recommendations."""

    def test_top_hooks_in_report(self, conn, city_id, report_date):
        sid = _insert_slideshow(conn, city_id, hook_text="Best hidden cafes")
        _insert_analytics(conn, sid, views=50000)
        _insert_performance(conn, sid, views_at_48h=50000, composite_score=5.0, decision_tag="scale")
        conn.commit()

        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 15, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "Best hidden cafes" in report
        assert "score=5.00" in report

    def test_no_hooks_when_empty(self, conn, city_id, report_date):
        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 0, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "Not enough data for hook recommendations" in report


# ---------------------------------------------------------------------------
# Test: Platform growth section
# ---------------------------------------------------------------------------


class TestPlatformGrowth:
    """Platform growth section shows deltas between snapshots."""

    def test_growth_with_two_snapshots(self, conn, city_id, report_date):
        _insert_platform_stats(conn, followers=900, total_views=40000, hours_ago=24)
        _insert_platform_stats(conn, followers=1000, total_views=50000)
        conn.commit()

        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 15, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "+100" in report  # followers delta
        assert "+10,000" in report  # views delta

    def test_growth_with_single_snapshot(self, conn, city_id, report_date):
        _insert_platform_stats(conn, followers=1000, total_views=50000)
        conn.commit()

        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 15, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "First snapshot" in report


# ---------------------------------------------------------------------------
# Test: Helper functions
# ---------------------------------------------------------------------------


class TestHelpers:
    """Test utility functions used in report generation."""

    def test_truncate_short_text(self):
        from daily_report import _truncate
        assert _truncate("hello", 40) == "hello"

    def test_truncate_long_text(self):
        from daily_report import _truncate
        result = _truncate("a" * 50, 40)
        assert len(result) == 40
        assert result.endswith("...")

    def test_truncate_newlines(self):
        from daily_report import _truncate
        result = _truncate("line1\nline2", 40)
        assert "\n" not in result

    def test_format_delta_positive(self):
        from daily_report import _format_delta
        assert _format_delta(100) == "+100"

    def test_format_delta_negative(self):
        from daily_report import _format_delta
        assert _format_delta(-50) == "-50"

    def test_format_delta_zero(self):
        from daily_report import _format_delta
        assert _format_delta(0) == "0"

    def test_compute_weight_changes_detects_change(self):
        from daily_report import _compute_weight_changes
        prev = {"category": {"food": 1.0, "nightlife": 1.0}}
        curr = {"category": {"food": 1.2, "nightlife": 0.8}}
        changes = _compute_weight_changes(prev, curr)
        assert len(changes) == 2
        food_change = [c for c in changes if c["value"] == "food"][0]
        assert food_change["delta"] == pytest.approx(0.2)

    def test_compute_weight_changes_ignores_meta(self):
        from daily_report import _compute_weight_changes
        prev = {"_meta": {"updated_at": "old"}, "category": {}}
        curr = {"_meta": {"updated_at": "new"}, "category": {}}
        changes = _compute_weight_changes(prev, curr)
        assert len(changes) == 0

    def test_compute_weight_changes_empty(self):
        from daily_report import _compute_weight_changes
        changes = _compute_weight_changes({}, {})
        assert changes == []


# ---------------------------------------------------------------------------
# Test: Phase 3 circuit breaker integration
# ---------------------------------------------------------------------------


class TestPhase3Integration:
    """Phase 3 runner correctly handles circuit breaker and weight computation."""

    def test_circuit_breaker_resets_weights(self, conn, city_id):
        """When circuit breaker trips, weights are reset to empty dicts."""
        from daily_report import run_phase3

        # Seed a published slideshow so post_count > 0
        _insert_slideshow(conn, city_id)
        conn.commit()

        phase2 = {"rc_configured": False}

        with mock.patch("daily_report.intelligence") as mock_intel:
            mock_intel.DIMENSIONS = DIMENSIONS
            mock_intel.evaluate_slideshows.return_value = 0
            mock_intel.check_circuit_breaker.return_value = True
            mock_intel.read_weights.return_value = {
                "category": {"food_and_drink": 1.5},
                **{d: {} for d in DIMENSIONS if d != "category"},
            }
            mock_intel.write_weights.return_value = None

            result = run_phase3(conn)

        assert result["circuit_breaker_tripped"] is True
        # New weights should all be empty dicts (reset)
        for dim in DIMENSIONS:
            assert result["new_weights"][dim] == {}
        # write_weights should have been called with circuit_breaker=True
        mock_intel.write_weights.assert_called_once()
        call_kwargs = mock_intel.write_weights.call_args
        assert call_kwargs[1]["circuit_breaker"] is True

    def test_normal_weight_computation(self, conn, city_id):
        """Normal path computes weights from intelligence module."""
        from daily_report import run_phase3

        _insert_slideshow(conn, city_id)
        conn.commit()

        phase2 = {"rc_configured": False}

        computed_weights = {d: {} for d in DIMENSIONS}
        computed_weights["category"]["food_and_drink"] = 1.2

        with mock.patch("daily_report.intelligence") as mock_intel:
            mock_intel.DIMENSIONS = DIMENSIONS
            mock_intel.evaluate_slideshows.return_value = 1
            mock_intel.check_circuit_breaker.return_value = False
            mock_intel.read_weights.return_value = {d: {} for d in DIMENSIONS}
            mock_intel.compute_dimension_weights.return_value = computed_weights
            mock_intel.write_weights.return_value = None

            result = run_phase3(conn)

        assert result["circuit_breaker_tripped"] is False
        assert result["new_weights"]["category"]["food_and_drink"] == 1.2
        mock_intel.compute_dimension_weights.assert_called_once()


# ---------------------------------------------------------------------------
# Test: Estimated views marker
# ---------------------------------------------------------------------------


class TestEstimatedViews:
    """Estimated views are marked with asterisk in the report."""

    def test_estimated_views_asterisk(self, conn, city_id, report_date):
        sid = _insert_slideshow(conn, city_id)
        _insert_analytics(conn, sid, views=3000, views_estimated=True)
        conn.commit()

        from daily_report import generate_report

        phase1 = {"release_ids_connected": 0, "analytics_upserted": 0, "stale_marked": 0}
        phase2 = {"rc_configured": False, "conversions_attributed": 0}
        phase3 = {"post_count": 5, "previous_weights": {}, "new_weights": {}, "evaluated": 0}

        report = generate_report(conn, 3, phase1, phase2, phase3, report_date)

        assert "3,000*" in report
        assert "estimated via delta method" in report
