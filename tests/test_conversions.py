"""Tests for pipeline/conversions.py — RevenueCat integration and attribution."""

import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import requests

from pipeline.conversions import (
    RevenueCatAuthError,
    RevenueCatClient,
    RevenueCatError,
    attribute_conversions,
    compute_rc_deltas,
    diagnose_funnel,
    fetch_rc_snapshot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iso(dt: datetime) -> str:
    """Format a datetime as ISO 8601 with Z suffix."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_client() -> RevenueCatClient:
    """Create a client with dummy credentials (tests mock HTTP calls)."""
    return RevenueCatClient(v2_secret_key="sk_test_secret", project_id="proj_123")


# ---------------------------------------------------------------------------
# RevenueCatClient — get_overview_metrics
# ---------------------------------------------------------------------------


class TestGetOverviewMetrics:
    def test_parses_metrics_correctly(self):
        """Overview metrics are extracted from the {"metrics": [...]} envelope."""
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "metrics": [
                {"id": "mrr", "value": 670},
                {"id": "active_trials", "value": 12},
                {"id": "active_subscriptions", "value": 45},
                {"id": "revenue", "value": 1234.56},
            ]
        }

        with patch.object(client.session, "get", return_value=mock_resp):
            result = client.get_overview_metrics()

        assert result == {
            "mrr": 670.0,
            "active_trials": 12.0,
            "active_subscriptions": 45.0,
            "revenue": 1234.56,
        }

    def test_passes_currency_param(self):
        """Currency parameter is forwarded to the API."""
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"metrics": []}

        with patch.object(client.session, "get", return_value=mock_resp) as mock_get:
            client.get_overview_metrics(currency="EUR")
            call_kwargs = mock_get.call_args
            assert call_kwargs[1]["params"]["currency"] == "EUR"

    def test_raises_on_unexpected_structure(self):
        """RevenueCatError raised when response lacks 'metrics' key."""
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": "unexpected"}

        with patch.object(client.session, "get", return_value=mock_resp):
            with pytest.raises(RevenueCatError, match="Unexpected overview response"):
                client.get_overview_metrics()


# ---------------------------------------------------------------------------
# RevenueCatClient — list_subscriptions (pagination)
# ---------------------------------------------------------------------------


class TestListSubscriptions:
    def test_single_page(self):
        """Single page of results returned when next_page is absent."""
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "subscriptions": [{"id": "sub_1"}, {"id": "sub_2"}],
        }

        with patch.object(client.session, "get", return_value=mock_resp):
            result = client.list_subscriptions()

        assert len(result) == 2
        assert result[0]["id"] == "sub_1"

    def test_paginates_through_multiple_pages(self):
        """Follows next_page cursor until exhausted."""
        client = _make_client()

        page1_resp = MagicMock()
        page1_resp.status_code = 200
        page1_resp.json.return_value = {
            "subscriptions": [{"id": "sub_1"}],
            "next_page": "cursor_abc",
        }

        page2_resp = MagicMock()
        page2_resp.status_code = 200
        page2_resp.json.return_value = {
            "subscriptions": [{"id": "sub_2"}],
            "next_page": "cursor_def",
        }

        page3_resp = MagicMock()
        page3_resp.status_code = 200
        page3_resp.json.return_value = {
            "subscriptions": [{"id": "sub_3"}],
        }

        with patch.object(
            client.session, "get", side_effect=[page1_resp, page2_resp, page3_resp]
        ) as mock_get:
            result = client.list_subscriptions(status="trialing")

        assert len(result) == 3
        assert [s["id"] for s in result] == ["sub_1", "sub_2", "sub_3"]

        # Verify cursor was passed on second and third calls
        calls = mock_get.call_args_list
        assert "starting_after" not in calls[0][1].get("params", {})
        assert calls[1][1]["params"]["starting_after"] == "cursor_abc"
        assert calls[2][1]["params"]["starting_after"] == "cursor_def"

    def test_passes_status_param(self):
        """Status filter is forwarded to the API."""
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"subscriptions": []}

        with patch.object(client.session, "get", return_value=mock_resp) as mock_get:
            client.list_subscriptions(status="trialing")
            assert mock_get.call_args[1]["params"]["status"] == "trialing"


# ---------------------------------------------------------------------------
# RevenueCatClient — get_recent_trials
# ---------------------------------------------------------------------------


class TestGetRecentTrials:
    def test_filters_by_starts_at(self):
        """Only trials within the last N days are returned."""
        client = _make_client()
        now = datetime.now(UTC)

        recent_dt = now - timedelta(days=1)
        old_dt = now - timedelta(days=10)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "subscriptions": [
                {"id": "trial_recent", "starts_at": _iso(recent_dt)},
                {"id": "trial_old", "starts_at": _iso(old_dt)},
            ],
        }

        with patch.object(client.session, "get", return_value=mock_resp):
            result = client.get_recent_trials(days=3)

        assert len(result) == 1
        assert result[0]["id"] == "trial_recent"

    def test_empty_when_no_recent_trials(self):
        """Returns empty list when all trials are older than the window."""
        client = _make_client()
        old_dt = datetime.now(UTC) - timedelta(days=30)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "subscriptions": [
                {"id": "trial_old", "starts_at": _iso(old_dt)},
            ],
        }

        with patch.object(client.session, "get", return_value=mock_resp):
            result = client.get_recent_trials(days=3)

        assert result == []

    def test_skips_missing_starts_at(self):
        """Trials without starts_at are silently skipped."""
        client = _make_client()
        now = datetime.now(UTC)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "subscriptions": [
                {"id": "trial_no_date"},
                {"id": "trial_ok", "starts_at": _iso(now - timedelta(hours=1))},
            ],
        }

        with patch.object(client.session, "get", return_value=mock_resp):
            result = client.get_recent_trials(days=3)

        assert len(result) == 1
        assert result[0]["id"] == "trial_ok"


# ---------------------------------------------------------------------------
# fetch_rc_snapshot
# ---------------------------------------------------------------------------


class TestFetchRcSnapshot:
    def test_stores_snapshot_in_db(self, conn):
        """Snapshot data is inserted into rc_snapshots table."""
        metrics = {
            "mrr": 670.0,
            "active_trials": 12.0,
            "active_subscriptions": 45.0,
            "active_users": 100.0,
            "new_customers": 5.0,
            "revenue": 1234.56,
        }

        mock_client = MagicMock()
        mock_client.get_overview_metrics.return_value = metrics

        with patch("pipeline.conversions._get_client", return_value=mock_client):
            result = fetch_rc_snapshot(conn)
            conn.commit()

        assert result == metrics

        row = conn.execute("SELECT * FROM rc_snapshots ORDER BY id DESC LIMIT 1").fetchone()
        assert row["mrr"] == 670.0
        assert row["active_trials"] == 12
        assert row["active_subscriptions"] == 45
        assert row["revenue"] == 1234.56

    def test_returns_none_when_not_configured(self, conn):
        """Returns None and does not crash when RC is not configured."""
        with patch("pipeline.conversions._get_client", return_value=None):
            result = fetch_rc_snapshot(conn)

        assert result is None
        count = conn.execute("SELECT COUNT(*) as cnt FROM rc_snapshots").fetchone()["cnt"]
        assert count == 0


# ---------------------------------------------------------------------------
# compute_rc_deltas
# ---------------------------------------------------------------------------


class TestComputeRcDeltas:
    def test_computes_deltas_between_snapshots(self, conn):
        """Deltas are computed as latest - previous for each metric."""
        conn.execute(
            """INSERT INTO rc_snapshots
               (fetched_at, mrr, active_trials, active_subscriptions,
                active_users, new_customers, revenue)
               VALUES ('2026-03-24 10:00:00', 500, 10, 40, 90, 3, 1000)"""
        )
        conn.execute(
            """INSERT INTO rc_snapshots
               (fetched_at, mrr, active_trials, active_subscriptions,
                active_users, new_customers, revenue)
               VALUES ('2026-03-25 10:00:00', 670, 12, 45, 100, 5, 1234.56)"""
        )
        conn.commit()

        deltas = compute_rc_deltas(conn)

        assert deltas is not None
        assert deltas["mrr"] == pytest.approx(170.0)
        assert deltas["active_trials"] == pytest.approx(2.0)
        assert deltas["active_subscriptions"] == pytest.approx(5.0)
        assert deltas["active_users"] == pytest.approx(10.0)
        assert deltas["new_customers"] == pytest.approx(2.0)
        assert deltas["revenue"] == pytest.approx(234.56)

    def test_returns_none_with_single_snapshot(self, conn):
        """Returns None when fewer than 2 snapshots exist."""
        conn.execute(
            """INSERT INTO rc_snapshots
               (mrr, active_trials, active_subscriptions,
                active_users, new_customers, revenue)
               VALUES (670, 12, 45, 100, 5, 1234.56)"""
        )
        conn.commit()

        deltas = compute_rc_deltas(conn)
        assert deltas is None

    def test_returns_none_with_no_snapshots(self, conn):
        """Returns None when no snapshots exist at all."""
        deltas = compute_rc_deltas(conn)
        assert deltas is None


# ---------------------------------------------------------------------------
# attribute_conversions
# ---------------------------------------------------------------------------


class TestAttributeConversions:
    def _insert_slideshow(self, conn, city_id, posted_at_str):
        """Helper to insert a published slideshow and return its id."""
        cur = conn.execute(
            """INSERT INTO slideshows
               (city_id, category, format, hook_text, slide_count, output_dir, posted_at, publish_status)
               VALUES (?, 'food_and_drink', 'listicle', 'Test hook', 5, '/tmp/test', ?, 'published')""",
            (city_id, posted_at_str),
        )
        return cur.lastrowid

    def test_last_touch_attribution(self, conn, city_id):
        """Conversion attributed to most recent post before trial start."""
        # Two slideshows posted at different times
        ss1_id = self._insert_slideshow(conn, city_id, "2026-03-20 10:00:00")
        ss2_id = self._insert_slideshow(conn, city_id, "2026-03-22 10:00:00")
        conn.commit()

        # Trial starts after both — should be attributed to ss2 (most recent)
        trial_dt = datetime(2026, 3, 23, 12, 0, 0, tzinfo=UTC)

        mock_client = MagicMock()
        mock_client.get_recent_trials.return_value = [
            {"id": "trial_1", "starts_at": _iso(trial_dt)},
        ]

        with patch("pipeline.conversions._get_client", return_value=mock_client):
            count = attribute_conversions(conn, days=7)
            conn.commit()

        assert count == 1

        # ss2 should have 1 conversion
        perf = conn.execute(
            "SELECT conversions FROM slideshow_performance WHERE slideshow_id = ?",
            (ss2_id,),
        ).fetchone()
        assert perf is not None
        assert perf["conversions"] == 1

        # ss1 should have no conversions
        perf1 = conn.execute(
            "SELECT conversions FROM slideshow_performance WHERE slideshow_id = ?",
            (ss1_id,),
        ).fetchone()
        assert perf1 is None

    def test_multiple_conversions_same_day(self, conn, city_id):
        """Multiple trials on the same day are attributed to the correct posts."""
        ss1_id = self._insert_slideshow(conn, city_id, "2026-03-20 10:00:00")
        ss2_id = self._insert_slideshow(conn, city_id, "2026-03-22 14:00:00")
        conn.commit()

        # Trial A starts between ss1 and ss2 — attributed to ss1
        trial_a_dt = datetime(2026, 3, 21, 8, 0, 0, tzinfo=UTC)
        # Trial B starts after ss2 — attributed to ss2
        trial_b_dt = datetime(2026, 3, 23, 8, 0, 0, tzinfo=UTC)
        # Trial C also starts after ss2 — also attributed to ss2
        trial_c_dt = datetime(2026, 3, 23, 16, 0, 0, tzinfo=UTC)

        mock_client = MagicMock()
        mock_client.get_recent_trials.return_value = [
            {"id": "trial_a", "starts_at": _iso(trial_a_dt)},
            {"id": "trial_b", "starts_at": _iso(trial_b_dt)},
            {"id": "trial_c", "starts_at": _iso(trial_c_dt)},
        ]

        with patch("pipeline.conversions._get_client", return_value=mock_client):
            count = attribute_conversions(conn, days=7)
            conn.commit()

        assert count == 3

        perf1 = conn.execute(
            "SELECT conversions FROM slideshow_performance WHERE slideshow_id = ?",
            (ss1_id,),
        ).fetchone()
        assert perf1["conversions"] == 1

        perf2 = conn.execute(
            "SELECT conversions FROM slideshow_performance WHERE slideshow_id = ?",
            (ss2_id,),
        ).fetchone()
        assert perf2["conversions"] == 2

    def test_no_attribution_when_no_posts(self, conn, city_id):
        """Trials are not attributed when no published slideshows exist."""
        trial_dt = datetime(2026, 3, 23, 12, 0, 0, tzinfo=UTC)

        mock_client = MagicMock()
        mock_client.get_recent_trials.return_value = [
            {"id": "trial_1", "starts_at": _iso(trial_dt)},
        ]

        with patch("pipeline.conversions._get_client", return_value=mock_client):
            count = attribute_conversions(conn, days=7)

        assert count == 0

    def test_skips_when_not_configured(self, conn):
        """Returns 0 when RC is not configured."""
        with patch("pipeline.conversions._get_client", return_value=None):
            count = attribute_conversions(conn, days=7)

        assert count == 0

    def test_trial_before_all_posts_not_attributed(self, conn, city_id):
        """Trial that starts before any post is not attributed."""
        self._insert_slideshow(conn, city_id, "2026-03-22 10:00:00")
        conn.commit()

        # Trial starts before the slideshow was posted
        trial_dt = datetime(2026, 3, 21, 8, 0, 0, tzinfo=UTC)

        mock_client = MagicMock()
        mock_client.get_recent_trials.return_value = [
            {"id": "trial_1", "starts_at": _iso(trial_dt)},
        ]

        with patch("pipeline.conversions._get_client", return_value=mock_client):
            count = attribute_conversions(conn, days=7)

        assert count == 0


# ---------------------------------------------------------------------------
# diagnose_funnel
# ---------------------------------------------------------------------------


class TestDiagnoseFunnel:
    def test_scale(self):
        """Good views + good conversions → SCALE."""
        assert diagnose_funnel(views_good=True, conversions_good=True, has_rc_data=True) == "SCALE"

    def test_fix_cta(self):
        """Good views + poor conversions → FIX CTA."""
        assert diagnose_funnel(views_good=True, conversions_good=False, has_rc_data=True) == "FIX CTA"

    def test_fix_hooks(self):
        """Poor views + good conversions → FIX HOOKS."""
        assert diagnose_funnel(views_good=False, conversions_good=True, has_rc_data=True) == "FIX HOOKS"

    def test_needs_work(self):
        """Poor views + poor conversions → NEEDS WORK."""
        assert diagnose_funnel(views_good=False, conversions_good=False, has_rc_data=True) == "NEEDS WORK"

    def test_no_rc_data_good_views(self):
        """Without RC data, good views → SCALE (conversions unknown)."""
        assert diagnose_funnel(views_good=True, conversions_good=False, has_rc_data=False) == "SCALE"

    def test_no_rc_data_poor_views(self):
        """Without RC data, poor views → FIX HOOKS."""
        assert diagnose_funnel(views_good=False, conversions_good=True, has_rc_data=False) == "FIX HOOKS"


# ---------------------------------------------------------------------------
# Graceful degradation
# ---------------------------------------------------------------------------


class TestGracefulDegradation:
    def test_fetch_snapshot_skips_when_not_configured(self, conn, caplog):
        """No crash and warning logged when RC key is empty."""
        with patch("pipeline.conversions.config") as mock_config:
            mock_config.REVENUECAT_V2_SECRET_KEY = ""
            mock_config.REVENUECAT_PROJECT_ID = "proj_123"
            mock_config.REVENUECAT_BASE_URL = "https://api.revenuecat.com/v2"

            with caplog.at_level(logging.WARNING, logger="pipeline.conversions"):
                result = fetch_rc_snapshot(conn)

        assert result is None
        assert "REVENUECAT_V2_SECRET_KEY not set" in caplog.text

    def test_attribute_skips_when_not_configured(self, conn, caplog):
        """attribution returns 0 and warns when RC key is empty."""
        with patch("pipeline.conversions.config") as mock_config:
            mock_config.REVENUECAT_V2_SECRET_KEY = ""
            mock_config.REVENUECAT_PROJECT_ID = "proj_123"
            mock_config.REVENUECAT_BASE_URL = "https://api.revenuecat.com/v2"

            with caplog.at_level(logging.WARNING, logger="pipeline.conversions"):
                count = attribute_conversions(conn, days=3)

        assert count == 0
        assert "REVENUECAT_V2_SECRET_KEY not set" in caplog.text


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_auth_error_raises_non_retryable(self):
        """401/403 responses raise RevenueCatAuthError (non-retryable)."""
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "Unauthorized"

        with patch.object(client.session, "get", return_value=mock_resp):
            with pytest.raises(RevenueCatAuthError, match="auth failed"):
                client.get_overview_metrics()

    def test_rate_limit_raises_retryable(self):
        """429 raises RevenueCatError which is retryable."""
        client = _make_client()
        mock_resp_429 = MagicMock()
        mock_resp_429.status_code = 429
        mock_resp_429.text = "Rate limited"

        # After retries, the error propagates
        with patch.object(client.session, "get", return_value=mock_resp_429):
            with pytest.raises(RevenueCatError, match="rate-limited"):
                client.get_overview_metrics()

    def test_timeout_raises_retryable(self):
        """Timeout exceptions are retryable via retry_with_backoff."""
        client = _make_client()

        with patch.object(
            client.session, "get", side_effect=requests.exceptions.Timeout("timed out")
        ):
            with pytest.raises(requests.exceptions.Timeout):
                client.get_overview_metrics()

    def test_403_raises_auth_error(self):
        """403 also raises RevenueCatAuthError."""
        client = _make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"

        with patch.object(client.session, "get", return_value=mock_resp):
            with pytest.raises(RevenueCatAuthError):
                client.get_overview_metrics()
