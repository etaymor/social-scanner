"""Tests for pipeline/analytics.py — Postiz analytics fetching, release ID connection,
stale draft detection, and delta fallback."""

import sqlite3
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from pipeline import db
from pipeline.analytics import (
    AnalyticsAuthError,
    AnalyticsError,
    connect_release_ids,
    detect_stale_drafts,
    fetch_platform_stats,
    fetch_post_analytics,
    fetch_posts,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_posted_slideshow(
    conn,
    city_id,
    postiz_post_id="post-abc",
    posted_hours_ago=24,
    release_id=None,
    publish_status="draft",
):
    """Create a slideshow that looks like it was posted N hours ago."""
    sid = db.create_slideshow(
        conn,
        city_id,
        category="food_and_drink",
        hook_format="listicle",
        hook_text="Top spots",
        slide_count=5,
        output_dir="/tmp/test",
    )
    posted_at = datetime.now(UTC) - timedelta(hours=posted_hours_ago)
    conn.execute(
        "UPDATE slideshows SET postiz_post_id = ?, posted_at = ?, "
        "tiktok_release_id = ?, publish_status = ? WHERE id = ?",
        (
            postiz_post_id,
            posted_at.strftime("%Y-%m-%d %H:%M:%S"),
            release_id,
            publish_status,
            sid,
        ),
    )
    conn.commit()
    return sid


def _mock_response(json_data, status_code=200):
    """Build a MagicMock response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = str(json_data)
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# fetch_posts
# ---------------------------------------------------------------------------


class TestFetchPosts:
    @patch("pipeline.analytics.requests.get")
    def test_returns_posts_list(self, mock_get):
        """fetch_posts returns list of post dicts from API."""
        mock_get.return_value = _mock_response([
            {"id": "p1", "integration": {"type": "tiktok"}},
            {"id": "p2", "integration": {"type": "tiktok"}},
        ])

        posts = fetch_posts(days=3)
        assert len(posts) == 2
        assert posts[0]["id"] == "p1"

    @patch("pipeline.analytics.requests.get")
    def test_handles_wrapped_response(self, mock_get):
        """fetch_posts handles {"data": [...]} wrapper."""
        mock_get.return_value = _mock_response({
            "data": [{"id": "p1"}],
        })

        posts = fetch_posts(days=3)
        assert len(posts) == 1
        assert posts[0]["id"] == "p1"

    @patch("pipeline.analytics.requests.get")
    def test_empty_post_list(self, mock_get):
        """fetch_posts handles empty response gracefully."""
        mock_get.return_value = _mock_response([])

        posts = fetch_posts(days=3)
        assert posts == []

    @patch("pipeline.analytics.requests.get")
    def test_auth_error_raises(self, mock_get):
        """fetch_posts raises AnalyticsAuthError on 401."""
        resp = MagicMock()
        resp.status_code = 401
        resp.text = "Unauthorized"
        mock_get.return_value = resp

        with pytest.raises(AnalyticsAuthError, match="auth failed"):
            fetch_posts(days=3)

    @patch("pipeline.analytics.requests.get")
    def test_passes_date_params(self, mock_get):
        """fetch_posts sends startDate and endDate query params."""
        mock_get.return_value = _mock_response([])

        fetch_posts(days=7)

        call_kwargs = mock_get.call_args
        params = call_kwargs[1].get("params") or call_kwargs.kwargs.get("params")
        assert "startDate" in params
        assert "endDate" in params


# ---------------------------------------------------------------------------
# connect_release_ids
# ---------------------------------------------------------------------------


class TestConnectReleaseIds:
    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.put")
    @patch("pipeline.analytics.requests.get")
    def test_connects_missing_release_id(self, mock_get, mock_put, mock_sleep, conn, city_id):
        """connect_release_ids fetches candidates, PUTs release ID, updates DB."""
        sid = _create_posted_slideshow(conn, city_id, postiz_post_id="post-1")

        # GET /posts/{id}/missing returns candidates
        mock_get.return_value = _mock_response([
            {"id": "vid-100"},
            {"id": "vid-200"},
        ])
        mock_put.return_value = _mock_response({"ok": True})

        posts = [{"id": "post-1"}]  # No releaseId -> missing
        count = connect_release_ids(conn, posts)
        conn.commit()

        assert count >= 1

        # Check DB was updated
        row = conn.execute("SELECT tiktok_release_id, publish_status FROM slideshows WHERE id = ?", (sid,)).fetchone()
        assert row["tiktok_release_id"] == "vid-200"  # Last candidate (newest)
        assert row["publish_status"] == "published"

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_skips_post_with_existing_release_id(self, mock_get, mock_sleep, conn, city_id):
        """Posts that already have a releaseId are not fetched for candidates."""
        _create_posted_slideshow(conn, city_id, postiz_post_id="post-1", release_id="vid-already")

        posts = [{"id": "post-1", "releaseId": "vid-already"}]
        count = connect_release_ids(conn, posts)

        # Should not call /posts/{id}/missing
        mock_get.assert_not_called()

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_skips_post_without_matching_slideshow(self, mock_get, mock_sleep, conn, city_id):
        """Posts that don't match any slideshow in DB are skipped."""
        posts = [{"id": "unknown-post"}]
        count = connect_release_ids(conn, posts)
        assert count == 0

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_handles_empty_candidates(self, mock_get, mock_sleep, conn, city_id):
        """If no release candidates found, post stays unconnected."""
        _create_posted_slideshow(conn, city_id, postiz_post_id="post-1")
        mock_get.return_value = _mock_response([])

        posts = [{"id": "post-1"}]
        count = connect_release_ids(conn, posts)

        # The post stays unconnected; only stale recovery might add count
        row = conn.execute("SELECT tiktok_release_id FROM slideshows WHERE postiz_post_id = ?", ("post-1",)).fetchone()
        assert row["tiktok_release_id"] is None

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.put")
    @patch("pipeline.analytics.requests.get")
    def test_recovers_stale_drafts(self, mock_get, mock_put, mock_sleep, conn, city_id):
        """connect_release_ids recovers stale slideshows if Postiz now has a release ID."""
        sid = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-stale",
            posted_hours_ago=100, publish_status="stale",
        )

        # For main posts loop: no matching posts
        # For stale recovery: GET /posts/{id}/missing returns candidates
        mock_get.return_value = _mock_response([{"id": "vid-recovered"}])
        mock_put.return_value = _mock_response({"ok": True})

        count = connect_release_ids(conn, [])  # Empty post list, but stale recovery runs
        conn.commit()

        assert count >= 1
        row = conn.execute("SELECT tiktok_release_id, publish_status FROM slideshows WHERE id = ?", (sid,)).fetchone()
        assert row["tiktok_release_id"] == "vid-recovered"
        assert row["publish_status"] == "published"

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_auth_error_propagates(self, mock_get, mock_sleep, conn, city_id):
        """AnalyticsAuthError propagates without being swallowed."""
        _create_posted_slideshow(conn, city_id, postiz_post_id="post-1")

        resp = MagicMock()
        resp.status_code = 401
        resp.text = "Unauthorized"
        mock_get.return_value = resp

        posts = [{"id": "post-1"}]
        with pytest.raises(AnalyticsAuthError):
            connect_release_ids(conn, posts)

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_non_auth_error_continues(self, mock_get, mock_sleep, conn, city_id):
        """Non-auth AnalyticsError is logged and processing continues."""
        _create_posted_slideshow(conn, city_id, postiz_post_id="post-1")

        resp = MagicMock()
        resp.status_code = 422
        resp.text = "Unprocessable"
        mock_get.return_value = resp

        posts = [{"id": "post-1"}]
        # Should not raise — error is caught and processing continues
        count = connect_release_ids(conn, posts)
        assert count == 0


# ---------------------------------------------------------------------------
# detect_stale_drafts
# ---------------------------------------------------------------------------


class TestDetectStaleDrafts:
    def test_marks_old_drafts_as_stale(self, conn, city_id):
        """Slideshows posted >72h ago with no release ID are marked stale."""
        sid = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-old",
            posted_hours_ago=80, publish_status="draft",
        )

        count = detect_stale_drafts(conn)
        conn.commit()

        assert count == 1
        row = conn.execute("SELECT publish_status FROM slideshows WHERE id = ?", (sid,)).fetchone()
        assert row["publish_status"] == "stale"

    def test_does_not_mark_recent_drafts(self, conn, city_id):
        """Slideshows posted <72h ago are NOT marked stale."""
        sid = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-recent",
            posted_hours_ago=10, publish_status="draft",
        )

        count = detect_stale_drafts(conn)
        conn.commit()

        assert count == 0
        row = conn.execute("SELECT publish_status FROM slideshows WHERE id = ?", (sid,)).fetchone()
        assert row["publish_status"] == "draft"

    def test_does_not_mark_published(self, conn, city_id):
        """Slideshows already published (with release ID) are not touched."""
        sid = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-pub",
            posted_hours_ago=100, release_id="vid-123",
            publish_status="published",
        )

        count = detect_stale_drafts(conn)
        conn.commit()

        assert count == 0
        row = conn.execute("SELECT publish_status FROM slideshows WHERE id = ?", (sid,)).fetchone()
        assert row["publish_status"] == "published"

    def test_does_not_re_stale_already_stale(self, conn, city_id):
        """Slideshows already marked stale are not counted again."""
        _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-already-stale",
            posted_hours_ago=100, publish_status="stale",
        )

        count = detect_stale_drafts(conn)
        assert count == 0

    def test_does_not_mark_unposted_slideshows(self, conn, city_id):
        """Slideshows that were never posted (posted_at IS NULL) are not marked stale."""
        sid = db.create_slideshow(
            conn, city_id, category=None, hook_format="listicle",
            hook_text="test", slide_count=5, output_dir="/tmp/test",
        )
        conn.commit()

        count = detect_stale_drafts(conn)
        assert count == 0


# ---------------------------------------------------------------------------
# fetch_post_analytics
# ---------------------------------------------------------------------------


class TestFetchPostAnalytics:
    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_stores_analytics_for_published_slideshow(self, mock_get, mock_sleep, conn, city_id):
        """Analytics for published slideshows are stored in slideshow_analytics."""
        sid = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-1",
            posted_hours_ago=24, release_id="vid-1",
            publish_status="published",
        )

        mock_get.return_value = _mock_response({
            "views": 5000,
            "likes": 300,
            "comments": 25,
            "shares": 50,
            "saves": 100,
        })

        count = fetch_post_analytics(conn, days=3)
        conn.commit()

        assert count == 1

        row = conn.execute(
            "SELECT * FROM slideshow_analytics WHERE slideshow_id = ?", (sid,)
        ).fetchone()
        assert row["views"] == 5000
        assert row["likes"] == 300
        assert row["comments"] == 25
        assert row["shares"] == 50
        assert row["saves"] == 100
        assert row["views_estimated"] == 0  # FALSE

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_skips_stale_slideshows(self, mock_get, mock_sleep, conn, city_id):
        """Stale slideshows are not fetched for analytics."""
        _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-stale",
            posted_hours_ago=100, publish_status="stale",
        )

        count = fetch_post_analytics(conn, days=7)
        assert count == 0
        mock_get.assert_not_called()

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_skips_drafts_without_release_id(self, mock_get, mock_sleep, conn, city_id):
        """Draft slideshows without release ID are not fetched."""
        _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-draft",
            posted_hours_ago=12, publish_status="draft",
        )

        count = fetch_post_analytics(conn, days=3)
        assert count == 0
        mock_get.assert_not_called()

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_handles_empty_analytics_response(self, mock_get, mock_sleep, conn, city_id):
        """Empty analytics response stores zeros and triggers delta fallback."""
        sid = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-empty",
            posted_hours_ago=24, release_id="vid-1",
            publish_status="published",
        )

        mock_get.return_value = _mock_response([])

        count = fetch_post_analytics(conn, days=3)
        conn.commit()

        assert count == 1
        row = conn.execute(
            "SELECT views FROM slideshow_analytics WHERE slideshow_id = ?", (sid,)
        ).fetchone()
        assert row["views"] == 0  # No delta stats to fall back on either

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_delta_fallback_estimates_views(self, mock_get, mock_sleep, conn, city_id):
        """When per-post views are empty, delta method estimates from platform stats."""
        sid = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-delta",
            posted_hours_ago=24, release_id="vid-1",
            publish_status="published",
        )

        # Insert two platform_stats entries for delta calculation
        conn.execute(
            "INSERT INTO platform_stats (fetched_at, followers, total_views, total_likes, recent_comments, recent_shares, videos) "
            "VALUES (datetime('now', '-1 day'), 1000, 40000, 2000, 100, 50, 10)"
        )
        conn.execute(
            "INSERT INTO platform_stats (fetched_at, followers, total_views, total_likes, recent_comments, recent_shares, videos) "
            "VALUES (datetime('now'), 1000, 50000, 2500, 120, 60, 11)"
        )
        conn.commit()

        # Per-post analytics return empty (0 views)
        mock_get.return_value = _mock_response({"views": 0, "likes": 10})

        count = fetch_post_analytics(conn, days=3)
        conn.commit()

        assert count == 1

        row = conn.execute(
            "SELECT views, views_estimated FROM slideshow_analytics WHERE slideshow_id = ?", (sid,)
        ).fetchone()
        # Delta is 50000 - 40000 = 10000, 1 slideshow -> 10000 per post
        assert row["views"] == 10000
        assert row["views_estimated"] == 1  # TRUE

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_delta_fallback_divides_among_multiple(self, mock_get, mock_sleep, conn, city_id):
        """Delta fallback divides estimated views among all empty-view slideshows."""
        sid1 = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-d1",
            posted_hours_ago=24, release_id="vid-1",
            publish_status="published",
        )
        sid2 = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-d2",
            posted_hours_ago=20, release_id="vid-2",
            publish_status="published",
        )

        conn.execute(
            "INSERT INTO platform_stats (fetched_at, followers, total_views, total_likes, recent_comments, recent_shares, videos) "
            "VALUES (datetime('now', '-1 day'), 1000, 40000, 2000, 100, 50, 10)"
        )
        conn.execute(
            "INSERT INTO platform_stats (fetched_at, followers, total_views, total_likes, recent_comments, recent_shares, videos) "
            "VALUES (datetime('now'), 1000, 50000, 2500, 120, 60, 12)"
        )
        conn.commit()

        # Both posts return 0 views
        mock_get.return_value = _mock_response({"views": 0, "likes": 5})

        count = fetch_post_analytics(conn, days=3)
        conn.commit()

        assert count == 2

        for sid in (sid1, sid2):
            row = conn.execute(
                "SELECT views, views_estimated FROM slideshow_analytics WHERE slideshow_id = ?", (sid,)
            ).fetchone()
            # Delta 10000 / 2 posts = 5000 each
            assert row["views"] == 5000
            assert row["views_estimated"] == 1

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_no_published_slideshows_returns_zero(self, mock_get, mock_sleep, conn, city_id):
        """When there are no published slideshows, returns 0 without API calls."""
        count = fetch_post_analytics(conn, days=3)
        assert count == 0
        mock_get.assert_not_called()

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_auth_error_propagates(self, mock_get, mock_sleep, conn, city_id):
        """AnalyticsAuthError from per-post fetch propagates."""
        _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-1",
            posted_hours_ago=24, release_id="vid-1",
            publish_status="published",
        )

        resp = MagicMock()
        resp.status_code = 401
        resp.text = "Unauthorized"
        mock_get.return_value = resp

        with pytest.raises(AnalyticsAuthError):
            fetch_post_analytics(conn, days=3)

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_non_auth_error_continues(self, mock_get, mock_sleep, conn, city_id):
        """Non-auth errors for individual posts are caught; processing continues."""
        _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-fail",
            posted_hours_ago=24, release_id="vid-1",
            publish_status="published",
        )
        _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-ok",
            posted_hours_ago=20, release_id="vid-2",
            publish_status="published",
        )

        fail_resp = MagicMock()
        fail_resp.status_code = 422
        fail_resp.text = "Unprocessable"

        ok_resp = _mock_response({"views": 1000, "likes": 50})

        mock_get.side_effect = [fail_resp, ok_resp]

        count = fetch_post_analytics(conn, days=3)
        assert count == 1  # Only the successful one

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_rate_limits_between_calls(self, mock_get, mock_sleep, conn, city_id):
        """time.sleep is called between API requests for rate limiting."""
        _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-1",
            posted_hours_ago=24, release_id="vid-1",
            publish_status="published",
        )
        _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-2",
            posted_hours_ago=20, release_id="vid-2",
            publish_status="published",
        )

        mock_get.return_value = _mock_response({"views": 100, "likes": 5})

        fetch_post_analytics(conn, days=3)

        # sleep should be called at least once between requests
        assert mock_sleep.call_count >= 2

    @patch("pipeline.analytics.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_handles_wrapped_analytics_response(self, mock_get, mock_sleep, conn, city_id):
        """Analytics response wrapped in {"data": {...}} is handled."""
        sid = _create_posted_slideshow(
            conn, city_id, postiz_post_id="post-wrap",
            posted_hours_ago=24, release_id="vid-1",
            publish_status="published",
        )

        mock_get.return_value = _mock_response({
            "data": {"views": 7777, "likes": 444, "comments": 33, "shares": 22, "saves": 11},
        })

        count = fetch_post_analytics(conn, days=3)
        conn.commit()

        assert count == 1
        row = conn.execute(
            "SELECT views FROM slideshow_analytics WHERE slideshow_id = ?", (sid,)
        ).fetchone()
        assert row["views"] == 7777


# ---------------------------------------------------------------------------
# fetch_platform_stats
# ---------------------------------------------------------------------------


class TestFetchPlatformStats:
    @patch("pipeline.analytics.requests.get")
    def test_stores_platform_stats(self, mock_get, conn):
        """Platform stats from API are stored in platform_stats table."""
        mock_get.return_value = _mock_response({
            "followers": 5000,
            "total_views": 100000,
            "total_likes": 8000,
            "recent_comments": 200,
            "recent_shares": 150,
            "videos": 42,
        })

        with patch("pipeline.analytics.config") as mock_config:
            mock_config.POSTIZ_TIKTOK_INTEGRATION_ID = "integ-123"
            mock_config.POSTIZ_BASE_URL = "https://api.postiz.com/public/v1"
            mock_config.POSTIZ_API_KEY = "test-key"
            mock_config.POSTIZ_UPLOAD_DELAY = 0

            result = fetch_platform_stats(conn)
            conn.commit()

        assert result["followers"] == 5000
        assert result["total_views"] == 100000

        row = conn.execute("SELECT * FROM platform_stats ORDER BY id DESC LIMIT 1").fetchone()
        assert row["followers"] == 5000
        assert row["total_views"] == 100000
        assert row["total_likes"] == 8000
        assert row["videos"] == 42

    @patch("pipeline.analytics.requests.get")
    def test_handles_camelcase_keys(self, mock_get, conn):
        """Platform stats handle camelCase API keys (totalViews, etc.)."""
        mock_get.return_value = _mock_response({
            "followers": 3000,
            "totalViews": 75000,
            "totalLikes": 6000,
            "recentComments": 100,
            "recentShares": 80,
            "videoCount": 30,
        })

        with patch("pipeline.analytics.config") as mock_config:
            mock_config.POSTIZ_TIKTOK_INTEGRATION_ID = "integ-123"
            mock_config.POSTIZ_BASE_URL = "https://api.postiz.com/public/v1"
            mock_config.POSTIZ_API_KEY = "test-key"
            mock_config.POSTIZ_UPLOAD_DELAY = 0

            result = fetch_platform_stats(conn)
            conn.commit()

        assert result["total_views"] == 75000
        assert result["videos"] == 30

    def test_skips_when_integration_id_empty(self, conn):
        """If POSTIZ_TIKTOK_INTEGRATION_ID is not set, returns None."""
        with patch("pipeline.analytics.config") as mock_config:
            mock_config.POSTIZ_TIKTOK_INTEGRATION_ID = ""

            result = fetch_platform_stats(conn)

        assert result is None

    @patch("pipeline.analytics.requests.get")
    def test_auth_error_raises(self, mock_get, conn):
        """AnalyticsAuthError from platform stats propagates."""
        resp = MagicMock()
        resp.status_code = 403
        resp.text = "Forbidden"
        mock_get.return_value = resp

        with patch("pipeline.analytics.config") as mock_config:
            mock_config.POSTIZ_TIKTOK_INTEGRATION_ID = "integ-123"
            mock_config.POSTIZ_BASE_URL = "https://api.postiz.com/public/v1"
            mock_config.POSTIZ_API_KEY = "test-key"
            mock_config.POSTIZ_UPLOAD_DELAY = 0

            with pytest.raises(AnalyticsAuthError):
                fetch_platform_stats(conn)

    @patch("pipeline.analytics.requests.get")
    def test_handles_wrapped_response(self, mock_get, conn):
        """Platform stats handle {"data": {...}} wrapper."""
        mock_get.return_value = _mock_response({
            "data": {
                "followers": 2000,
                "total_views": 50000,
                "total_likes": 4000,
                "recent_comments": 50,
                "recent_shares": 30,
                "videos": 20,
            },
        })

        with patch("pipeline.analytics.config") as mock_config:
            mock_config.POSTIZ_TIKTOK_INTEGRATION_ID = "integ-123"
            mock_config.POSTIZ_BASE_URL = "https://api.postiz.com/public/v1"
            mock_config.POSTIZ_API_KEY = "test-key"
            mock_config.POSTIZ_UPLOAD_DELAY = 0

            result = fetch_platform_stats(conn)
            conn.commit()

        assert result["followers"] == 2000
        assert result["total_views"] == 50000


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    @patch("pipeline.analytics.requests.get")
    def test_analytics_error_is_retryable(self, mock_get):
        """AnalyticsError is the base for retryable errors."""
        assert issubclass(AnalyticsError, Exception)
        assert issubclass(AnalyticsAuthError, AnalyticsError)

    @patch("pipeline.retry.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_retries_on_5xx(self, mock_get, mock_sleep):
        """5xx errors trigger retry logic."""
        fail_resp = MagicMock()
        fail_resp.status_code = 500
        fail_resp.raise_for_status = MagicMock(
            side_effect=Exception("Server error")
        )

        ok_resp = _mock_response([])

        mock_get.side_effect = [fail_resp, ok_resp]

        # Should succeed on second attempt
        posts = fetch_posts(days=1)
        assert posts == []
        assert mock_get.call_count == 2

    @patch("pipeline.analytics.requests.get")
    def test_4xx_raises_analytics_error(self, mock_get):
        """Non-auth 4xx errors raise AnalyticsError (non-retryable)."""
        resp = MagicMock()
        resp.status_code = 422
        resp.text = "Unprocessable"
        mock_get.return_value = resp

        with pytest.raises(AnalyticsError, match="422"):
            fetch_posts(days=1)

    @patch("pipeline.retry.time.sleep")
    @patch("pipeline.analytics.requests.get")
    def test_timeout_retries(self, mock_get, mock_sleep):
        """Timeout errors trigger retry logic."""
        import requests as real_requests

        mock_get.side_effect = [
            real_requests.Timeout("Connection timed out"),
            _mock_response([{"id": "p1"}]),
        ]

        posts = fetch_posts(days=1)
        assert len(posts) == 1
        assert mock_get.call_count == 2
