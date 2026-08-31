"""Postiz analytics: fetch TikTok post stats, connect release IDs, detect stale drafts."""

import logging
import sqlite3
import time
from datetime import UTC, datetime, timedelta

import requests

import config
from pipeline.retry import retry_with_backoff

log = logging.getLogger(__name__)

# Retry settings (matching posting.py)
MAX_RETRIES = 2
RETRY_BASE_DELAY = 2  # seconds


# ---------------------------------------------------------------------------
# Error hierarchy
# ---------------------------------------------------------------------------


class AnalyticsError(Exception):
    """Non-retryable analytics client error."""

    pass


class AnalyticsAuthError(AnalyticsError):
    """Non-retryable auth failure (401/403)."""

    pass


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_headers() -> dict[str, str]:
    """Return fresh Postiz headers (re-reads config for testability)."""
    return {
        "Authorization": f"Bearer {config.POSTIZ_API_KEY}",
        "Content-Type": "application/json",
    }


def _api_get(url: str, params: dict | None = None) -> dict | list:
    """GET request with retry and standard error handling."""

    def _do_get():
        resp = requests.get(url, headers=_get_headers(), params=params, timeout=60)
        if resp.status_code in (401, 403):
            raise AnalyticsAuthError(
                f"Analytics auth failed (HTTP {resp.status_code})"
            )
        if 400 <= resp.status_code < 500:
            raise AnalyticsError(
                f"Analytics request failed (HTTP {resp.status_code}): {resp.text}"
            )
        resp.raise_for_status()
        return resp.json()

    try:
        return retry_with_backoff(
            _do_get,
            max_retries=MAX_RETRIES + 1,
            base_delay=RETRY_BASE_DELAY,
            non_retryable=(AnalyticsAuthError, AnalyticsError),
        )
    except (AnalyticsAuthError, AnalyticsError):
        raise
    except Exception as e:
        raise AnalyticsError(
            f"Analytics GET {url} failed after {MAX_RETRIES + 1} attempts: {e}"
        ) from e


def _api_put(url: str, payload: dict) -> dict:
    """PUT request with retry and standard error handling."""

    def _do_put():
        resp = requests.put(
            url, headers=_get_headers(), json=payload, timeout=60
        )
        if resp.status_code in (401, 403):
            raise AnalyticsAuthError(
                f"Analytics auth failed (HTTP {resp.status_code})"
            )
        if 400 <= resp.status_code < 500:
            raise AnalyticsError(
                f"Analytics PUT failed (HTTP {resp.status_code}): {resp.text}"
            )
        resp.raise_for_status()
        return resp.json()

    try:
        return retry_with_backoff(
            _do_put,
            max_retries=MAX_RETRIES + 1,
            base_delay=RETRY_BASE_DELAY,
            non_retryable=(AnalyticsAuthError, AnalyticsError),
        )
    except (AnalyticsAuthError, AnalyticsError):
        raise
    except Exception as e:
        raise AnalyticsError(
            f"Analytics PUT {url} failed after {MAX_RETRIES + 1} attempts: {e}"
        ) from e


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_posts(days: int | None = None) -> list[dict]:
    """GET /posts for the last *days* days. Return list of post dicts.

    Each post dict contains at minimum: id, integration (with id and type),
    and optional releaseId.
    """
    if days is None:
        days = config.ANALYTICS_LOOKBACK_DAYS

    end = datetime.now(UTC)
    start = end - timedelta(days=days)

    url = f"{config.POSTIZ_BASE_URL}/posts"
    params = {
        "startDate": start.strftime("%Y-%m-%d"),
        "endDate": end.strftime("%Y-%m-%d"),
    }

    data = _api_get(url, params)

    # Normalise: API may return a list directly or wrap in {"data": [...]}
    if isinstance(data, dict):
        posts = data.get("data", data.get("posts", []))
    else:
        posts = data

    if not isinstance(posts, list):
        log.warning("Unexpected posts response type: %s", type(posts))
        return []

    return posts


def connect_release_ids(conn: sqlite3.Connection, posts: list[dict]) -> int:
    """For posts missing a TikTok release ID, try to connect them via Postiz.

    Steps for each unconnected post:
    1. GET /posts/{id}/missing  -> list of TikTok video candidates
    2. Pick the latest candidate (highest ID = newest)
    3. PUT /posts/{id}/release-id  with the chosen releaseId
    4. Update slideshows.tiktok_release_id and publish_status='published'

    Also re-checks stale slideshows for recovery.

    Returns the number of release IDs successfully connected.
    """
    connected = 0

    for post in posts:
        post_id = post.get("id")
        if not post_id:
            continue

        # Skip posts that already have a release ID
        if post.get("releaseId"):
            # Still ensure our DB is up-to-date
            _update_slideshow_release(conn, post_id, post["releaseId"])
            continue

        # Look up in our DB — only bother if we have a matching slideshow
        row = conn.execute(
            "SELECT id, tiktok_release_id FROM slideshows WHERE postiz_post_id = ?",
            (post_id,),
        ).fetchone()
        if not row:
            continue

        # Already connected in our DB
        if row["tiktok_release_id"]:
            continue

        try:
            time.sleep(config.POSTIZ_UPLOAD_DELAY)
            missing_url = f"{config.POSTIZ_BASE_URL}/posts/{post_id}/missing"
            candidates = _api_get(missing_url)

            # Normalise
            if isinstance(candidates, dict):
                candidates = candidates.get("data", candidates.get("videos", []))
            if not isinstance(candidates, list) or not candidates:
                log.debug("No release candidates for post %s", post_id)
                continue

            # Pick the latest candidate (highest numeric ID or last in list)
            chosen = candidates[-1]
            release_id = chosen if isinstance(chosen, str) else chosen.get("id", chosen.get("releaseId"))
            if not release_id:
                log.warning("Could not extract release ID from candidate for post %s", post_id)
                continue

            time.sleep(config.POSTIZ_UPLOAD_DELAY)
            put_url = f"{config.POSTIZ_BASE_URL}/posts/{post_id}/release-id"
            _api_put(put_url, {"releaseId": str(release_id)})

            _update_slideshow_release(conn, post_id, str(release_id))
            connected += 1
            log.info("Connected release ID %s for post %s", release_id, post_id)

        except AnalyticsAuthError:
            raise
        except AnalyticsError as e:
            log.warning("Failed to connect release ID for post %s: %s", post_id, e)
            continue

    # Re-check stale slideshows for recovery
    connected += _recover_stale_drafts(conn)

    return connected


def _update_slideshow_release(
    conn: sqlite3.Connection, postiz_post_id: str, release_id: str
) -> None:
    """Update the slideshow record with the TikTok release ID and mark published."""
    conn.execute(
        "UPDATE slideshows SET tiktok_release_id = ?, publish_status = 'published' "
        "WHERE postiz_post_id = ? AND (tiktok_release_id IS NULL OR tiktok_release_id != ?)",
        (release_id, postiz_post_id, release_id),
    )


def _recover_stale_drafts(conn: sqlite3.Connection) -> int:
    """Re-check stale slideshows — if Postiz now has a release ID, recover them.

    Returns the number of recovered slideshows.
    """
    stale_rows = conn.execute(
        "SELECT id, postiz_post_id FROM slideshows WHERE publish_status = 'stale' AND postiz_post_id IS NOT NULL"
    ).fetchall()

    recovered = 0
    for row in stale_rows:
        postiz_id = row["postiz_post_id"]
        try:
            time.sleep(config.POSTIZ_UPLOAD_DELAY)
            missing_url = f"{config.POSTIZ_BASE_URL}/posts/{postiz_id}/missing"
            candidates = _api_get(missing_url)

            if isinstance(candidates, dict):
                candidates = candidates.get("data", candidates.get("videos", []))
            if not isinstance(candidates, list) or not candidates:
                continue

            chosen = candidates[-1]
            release_id = chosen if isinstance(chosen, str) else chosen.get("id", chosen.get("releaseId"))
            if not release_id:
                continue

            time.sleep(config.POSTIZ_UPLOAD_DELAY)
            put_url = f"{config.POSTIZ_BASE_URL}/posts/{postiz_id}/release-id"
            _api_put(put_url, {"releaseId": str(release_id)})

            conn.execute(
                "UPDATE slideshows SET tiktok_release_id = ?, publish_status = 'published' WHERE id = ?",
                (str(release_id), row["id"]),
            )
            recovered += 1
            log.info("Recovered stale slideshow %d with release ID %s", row["id"], release_id)

        except AnalyticsAuthError:
            raise
        except AnalyticsError as e:
            log.warning("Failed to recover stale slideshow %d: %s", row["id"], e)
            continue

    return recovered


def detect_stale_drafts(conn: sqlite3.Connection) -> int:
    """Mark slideshows as 'stale' if posted_at > STALE_DRAFT_HOURS ago and no release ID.

    Returns the number of slideshows marked stale.
    """
    cutoff = datetime.now(UTC) - timedelta(hours=config.STALE_DRAFT_HOURS)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    cur = conn.execute(
        "UPDATE slideshows SET publish_status = 'stale' "
        "WHERE posted_at IS NOT NULL "
        "AND posted_at < ? "
        "AND tiktok_release_id IS NULL "
        "AND publish_status = 'draft'",
        (cutoff_str,),
    )
    count = cur.rowcount
    if count:
        log.info("Marked %d slideshows as stale (>%dh without release ID)", count, config.STALE_DRAFT_HOURS)
    return count


def fetch_post_analytics(conn: sqlite3.Connection, days: int | None = None) -> int:
    """Fetch per-post analytics for published slideshows and store in slideshow_analytics.

    For posts where per-post analytics return empty/no views, falls back to the
    delta method using platform-level stats.

    Returns the number of analytics rows upserted.
    """
    if days is None:
        days = config.ANALYTICS_LOOKBACK_DAYS

    cutoff = datetime.now(UTC) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    # Get published slideshows in the lookback window
    rows = conn.execute(
        "SELECT id, postiz_post_id, tiktok_release_id FROM slideshows "
        "WHERE publish_status = 'published' "
        "AND tiktok_release_id IS NOT NULL "
        "AND posted_at >= ?",
        (cutoff_str,),
    ).fetchall()

    if not rows:
        log.info("No published slideshows in the last %d days to fetch analytics for", days)
        return 0

    upserted = 0
    empty_views_slideshows = []

    for row in rows:
        postiz_id = row["postiz_post_id"]
        slideshow_id = row["id"]

        try:
            time.sleep(config.POSTIZ_UPLOAD_DELAY)
            url = f"{config.POSTIZ_BASE_URL}/analytics/post/{postiz_id}"
            data = _api_get(url)

            # Normalise the response
            if isinstance(data, list):
                analytics = data[0] if data else {}
            elif isinstance(data, dict):
                analytics = data.get("data", data)
                if isinstance(analytics, list):
                    analytics = analytics[0] if analytics else {}
            else:
                analytics = {}

            views = analytics.get("views", 0) or 0
            likes = analytics.get("likes", 0) or 0
            comments = analytics.get("comments", 0) or 0
            shares = analytics.get("shares", 0) or 0
            saves = analytics.get("saves", 0) or 0

            if views == 0:
                empty_views_slideshows.append(slideshow_id)

            conn.execute(
                """INSERT INTO slideshow_analytics
                   (slideshow_id, fetched_at, views, likes, comments, shares, saves, views_estimated)
                   VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(slideshow_id, DATE(fetched_at)) DO UPDATE SET
                     views = MAX(excluded.views, slideshow_analytics.views),
                     likes = excluded.likes,
                     comments = excluded.comments,
                     shares = excluded.shares,
                     saves = excluded.saves,
                     views_estimated = CASE
                       WHEN excluded.views > slideshow_analytics.views THEN excluded.views_estimated
                       ELSE slideshow_analytics.views_estimated
                     END""",
                (slideshow_id, views, likes, comments, shares, saves, False),
            )
            upserted += 1

        except AnalyticsAuthError:
            raise
        except AnalyticsError as e:
            log.warning("Failed to fetch analytics for post %s: %s", postiz_id, e)
            continue

    # Delta method fallback for posts with empty views
    if empty_views_slideshows:
        _apply_delta_fallback(conn, empty_views_slideshows)

    return upserted


def _apply_delta_fallback(
    conn: sqlite3.Connection, slideshow_ids: list[int]
) -> None:
    """Estimate per-post views from platform-level total view deltas.

    Takes the delta of total_views between the last two platform_stats entries,
    divides evenly among the given slideshows, and flags as estimated.
    """
    stats = conn.execute(
        "SELECT total_views FROM platform_stats ORDER BY fetched_at DESC LIMIT 2"
    ).fetchall()

    if len(stats) < 2:
        log.warning("Not enough platform_stats entries for delta fallback")
        return

    delta = stats[0]["total_views"] - stats[1]["total_views"]
    if delta <= 0:
        log.info("Platform view delta is %d, skipping delta fallback", delta)
        return

    per_post = delta // len(slideshow_ids)
    if per_post <= 0:
        return

    for sid in slideshow_ids:
        conn.execute(
            "UPDATE slideshow_analytics SET views = ?, views_estimated = ? "
            "WHERE id = (SELECT id FROM slideshow_analytics WHERE slideshow_id = ? ORDER BY fetched_at DESC LIMIT 1)",
            (per_post, True, sid),
        )

    log.info(
        "Delta fallback: estimated %d views/post for %d slideshows (total delta=%d)",
        per_post,
        len(slideshow_ids),
        delta,
    )


def fetch_platform_stats(conn: sqlite3.Connection) -> dict | None:
    """GET /analytics/{integrationId} and store a platform_stats snapshot.

    Returns the stats dict, or None on failure.
    """
    integration_id = config.POSTIZ_TIKTOK_INTEGRATION_ID
    if not integration_id:
        log.warning("POSTIZ_TIKTOK_INTEGRATION_ID not configured, skipping platform stats")
        return None

    url = f"{config.POSTIZ_BASE_URL}/analytics/{integration_id}"
    data = _api_get(url)

    # Normalise
    if isinstance(data, list):
        stats = data[0] if data else {}
    elif isinstance(data, dict):
        stats = data.get("data", data)
        if isinstance(stats, list):
            stats = stats[0] if stats else {}
    else:
        stats = {}

    followers = stats.get("followers", 0) or 0
    total_views = stats.get("total_views", stats.get("totalViews", 0)) or 0
    total_likes = stats.get("total_likes", stats.get("totalLikes", 0)) or 0
    recent_comments = stats.get("recent_comments", stats.get("recentComments", 0)) or 0
    recent_shares = stats.get("recent_shares", stats.get("recentShares", 0)) or 0
    videos = stats.get("videos", stats.get("videoCount", 0)) or 0

    conn.execute(
        "INSERT INTO platform_stats "
        "(fetched_at, followers, total_views, total_likes, recent_comments, recent_shares, videos) "
        "VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)",
        (followers, total_views, total_likes, recent_comments, recent_shares, videos),
    )

    result = {
        "followers": followers,
        "total_views": total_views,
        "total_likes": total_likes,
        "recent_comments": recent_comments,
        "recent_shares": recent_shares,
        "videos": videos,
    }

    log.info("Stored platform stats: %d followers, %d total views", followers, total_views)
    return result
