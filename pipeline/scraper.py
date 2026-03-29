"""Step 2 — Scrape TikTok & Instagram posts via Apify for pending hashtags."""

import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from apify_client import ApifyClient

try:
    from apify_client.errors import ApifyClientError
except ImportError:
    from apify_client._errors import ApifyApiError as ApifyClientError  # v1.x

try:
    import impit

    _IMPIT_HTTP_ERROR: type[Exception] | None = impit.HTTPError
except ImportError:
    _IMPIT_HTTP_ERROR = None

import config

from . import db

_SCRAPE_ERRORS: tuple[type[Exception], ...] = (
    requests.RequestException,
    ApifyClientError,
    KeyError,
    ValueError,
)
if _IMPIT_HTTP_ERROR is not None:
    _SCRAPE_ERRORS = (*_SCRAPE_ERRORS, _IMPIT_HTTP_ERROR)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Field mapping helpers
# ---------------------------------------------------------------------------


def _map_tiktok(item: dict) -> dict:
    """Map a raw TikTok Apify result to our canonical post dict."""
    stats = item.get("stats", {})
    author_meta = item.get("authorMeta", {})
    post_id = item.get("id")
    author_name = author_meta.get("name") or item.get("author")

    # Build caption from text + location metadata
    caption = item.get("text") or item.get("desc") or ""

    # Append TikTok location tag if present (locationMeta contains tagged location)
    loc = item.get("locationMeta") or {}
    loc_name = loc.get("locationName", "").strip()
    loc_addr = loc.get("address", "").strip()
    if loc_name:
        location_parts = [loc_name]
        if loc_addr and loc_addr.lower() != loc_name.lower():
            location_parts.append(loc_addr)
        caption += f"\n📍 Location tag: {', '.join(location_parts)}"

    # Get cover image URL for visual OCR
    video_meta = item.get("videoMeta") or {}
    cover_url = video_meta.get("coverUrl") or video_meta.get("originalCoverUrl") or ""

    return {
        "post_id": post_id,
        "caption": caption,
        "likes": item.get("diggCount") or stats.get("diggCount", 0),
        "comments": item.get("commentCount") or stats.get("commentCount", 0),
        "shares": item.get("shareCount") or stats.get("shareCount", 0),
        "saves": item.get("collectCount", 0),
        "views": item.get("playCount") or stats.get("playCount", 0),
        "url": (
            item.get("webVideoUrl") or f"https://www.tiktok.com/@{author_name}/video/{post_id}"
        ),
        "author": author_name,
        "created_at": item.get("createTime"),
        "cover_url": cover_url,
    }


def _map_instagram(item: dict) -> dict:
    """Map a raw Instagram Apify result to our canonical post dict."""
    caption = item.get("caption") or ""

    # Append location name if the scraper provides it
    loc_name = item.get("locationName", "").strip()
    if not loc_name:
        loc = item.get("location") or {}
        loc_name = loc.get("name", "").strip() if isinstance(loc, dict) else ""
    if loc_name:
        caption += f"\n📍 Location tag: {loc_name}"

    # Get display image URL for visual OCR
    cover_url = item.get("displayUrl") or ""

    return {
        "post_id": item.get("id"),
        "caption": caption,
        "likes": item.get("likesCount", 0),
        "comments": item.get("commentsCount", 0),
        "shares": 0,
        "saves": 0,
        "views": item.get("videoViewCount", 0),
        "url": item.get("url"),
        "author": item.get("ownerUsername"),
        "created_at": item.get("timestamp"),
        "cover_url": cover_url,
    }


# ---------------------------------------------------------------------------
# Engagement filters
# ---------------------------------------------------------------------------


def _passes_tiktok_filter(post: dict) -> bool:
    """Return True if a TikTok post meets the minimum engagement bar."""
    views = post.get("views") or 0
    likes = post.get("likes") or 0
    return views >= config.MIN_VIEWS_TIKTOK and likes >= config.MIN_LIKES_TIKTOK


def _passes_instagram_filter(post: dict) -> bool:
    """Return True if an Instagram post meets the minimum engagement bar.

    For photo posts views may legitimately be 0, so only enforce the view
    threshold when views > 0.
    """
    views = post.get("views") or 0
    likes = post.get("likes") or 0
    if likes < config.MIN_LIKES_INSTAGRAM:
        return False
    return not (views > 0 and views < config.MIN_VIEWS_INSTAGRAM)


# ---------------------------------------------------------------------------
# Core scraper
# ---------------------------------------------------------------------------

# Cap resultsPerPage for TikTok — this is per-hashtag, so keep it low
_TIKTOK_MAX_PER_HASHTAG = 30


def _scrape_batch(
    client: ApifyClient,
    platform: str,
    tags: list[str],
    max_posts: int,
) -> list[dict]:
    """Run ONE Apify actor call for all *tags* and return mapped post dicts."""
    if platform == "tiktok":
        actor = client.actor(config.TIKTOK_ACTOR)
        per_hashtag = min(max_posts, _TIKTOK_MAX_PER_HASHTAG)
        run = actor.call(
            run_input={"hashtags": tags, "resultsPerPage": per_hashtag},
            build="latest",
        )
        mapper = _map_tiktok
        filt = _passes_tiktok_filter
    else:
        actor = client.actor(config.INSTAGRAM_ACTOR)
        results_limit = min(max_posts, 200)
        run = actor.call(
            run_input={"hashtags": tags, "resultsLimit": results_limit},
        )
        mapper = _map_instagram
        filt = _passes_instagram_filter

    if run is None:
        log.warning("Apify actor returned None for %s batch (%d tags)", platform, len(tags))
        return []

    dataset_id = run["defaultDatasetId"]
    items = client.dataset(dataset_id).list_items().items

    mapped = [mapper(item) for item in items]

    # Filter out low-engagement posts
    before = len(mapped)
    mapped = [p for p in mapped if filt(p)]
    filtered_out = before - len(mapped)
    if filtered_out:
        log.info(
            "Filtered out %d/%d low-engagement %s posts (%d tags)",
            filtered_out,
            before,
            platform,
            len(tags),
        )

    return mapped


def scrape_posts(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    max_posts: int = 100,
) -> int:
    """Scrape pending hashtags for *city_id* and store qualifying posts.

    Groups all pending hashtags by platform and sends ONE Apify actor call
    per platform (typically 2 total: one TikTok, one Instagram).

    Returns the total number of new posts inserted.
    """
    client = ApifyClient(config.APIFY_API_TOKEN)
    pending = db.get_pending_hashtags(conn, city_id)

    if not pending:
        log.info("No pending hashtags for %s (city_id=%d)", city_name, city_id)
        return 0

    # Group by platform
    batches: dict[str, list[sqlite3.Row]] = {}
    for row in pending:
        batches.setdefault(row["platform"], []).append(row)

    # Mark all as running
    all_ids = [row["id"] for row in pending]
    db.bulk_update_hashtag_status(conn, all_ids, "running")

    total_inserted = 0

    log.info(
        "Scraping %d hashtags in %d batch(es) for %s...",
        len(pending),
        len(batches),
        city_name,
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {}
        for platform, rows in batches.items():
            tags = [r["tag"] for r in rows]
            future = pool.submit(_scrape_batch, client, platform, tags, max_posts)
            futures[future] = (platform, rows)

        for future in as_completed(futures):
            platform, rows = futures[future]
            hashtag_ids = [r["id"] for r in rows]

            try:
                posts = future.result()
                inserted = 0
                for post_data in posts:
                    if not post_data.get("post_id"):
                        continue
                    # Insert post linked to first hashtag
                    raw_id = db.insert_post(conn, city_id, platform, post_data, hashtag_ids[0])
                    if raw_id is not None:
                        inserted += 1
                        # Link to remaining hashtags
                        for hid in hashtag_ids[1:]:
                            conn.execute(
                                "INSERT OR IGNORE INTO post_hashtags (post_id, hashtag_id) VALUES (?, ?)",
                                (raw_id, hid),
                            )
                conn.commit()

                db.bulk_update_hashtag_status(conn, hashtag_ids, "completed")
                log.info(
                    "Stored %d posts from %d %s hashtags for %s",
                    inserted,
                    len(rows),
                    platform,
                    city_name,
                )
                total_inserted += inserted

            except _SCRAPE_ERRORS:
                log.exception(
                    "Failed to scrape %s batch (%d tags) for %s",
                    platform,
                    len(rows),
                    city_name,
                )
                db.bulk_update_hashtag_status(conn, hashtag_ids, "failed")

    log.info(
        "Scraping complete for %s — %d posts stored from %d hashtags",
        city_name,
        total_inserted,
        len(pending),
    )
    return total_inserted
