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
    return {
        "post_id": post_id,
        "caption": item.get("text") or item.get("desc"),
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
    }


def _map_instagram(item: dict) -> dict:
    """Map a raw Instagram Apify result to our canonical post dict."""
    return {
        "post_id": item.get("id"),
        "caption": item.get("caption"),
        "likes": item.get("likesCount", 0),
        "comments": item.get("commentsCount", 0),
        "shares": 0,
        "saves": 0,
        "views": item.get("videoViewCount", 0),
        "url": item.get("url"),
        "author": item.get("ownerUsername"),
        "created_at": item.get("timestamp"),
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


def _scrape_hashtag(
    client: ApifyClient,
    platform: str,
    tag: str,
    max_posts: int,
) -> list[dict]:
    """Run the appropriate Apify actor and return mapped post dicts."""
    if platform == "tiktok":
        actor = client.actor(config.TIKTOK_ACTOR)
        run = actor.call(
            run_input={"hashtags": [tag], "resultsPerPage": max_posts},
            build="latest",
        )
        mapper = _map_tiktok
        filt = _passes_tiktok_filter
    else:
        actor = client.actor(config.INSTAGRAM_ACTOR)
        run = actor.call(
            run_input={"hashtags": [tag], "resultsLimit": max_posts},
        )
        mapper = _map_instagram
        filt = _passes_instagram_filter

    if run is None:
        log.warning("Apify actor returned None for %s/%s", platform, tag)
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
            "Filtered out %d/%d low-engagement %s posts for #%s",
            filtered_out,
            before,
            platform,
            tag,
        )

    return mapped


def _scrape_one(
    client: ApifyClient, platform: str, tag: str, max_posts: int
) -> tuple[str, str, list[dict]]:
    """Scrape a single hashtag (thread-safe). Returns (tag, platform, posts)."""
    posts = _scrape_hashtag(client, platform, tag, max_posts)
    return tag, platform, posts


def scrape_posts(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    max_posts: int = 100,
) -> int:
    """Scrape pending hashtags for *city_id* and store qualifying posts.

    Returns the total number of posts inserted (including duplicates linked
    to new hashtags).
    """
    client = ApifyClient(config.APIFY_API_TOKEN)
    pending = db.get_pending_hashtags(conn, city_id)

    if not pending:
        log.info("No pending hashtags for %s (city_id=%d)", city_name, city_id)
        return 0

    # Mark all as running
    for row in pending:
        db.update_hashtag_status(conn, row["id"], "running")

    total_inserted = 0
    max_workers = min(3, len(pending))

    log.info("Scraping %d hashtags with %d workers...", len(pending), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_scrape_one, client, row["platform"], row["tag"], max_posts): row
            for row in pending
        }

        for future in as_completed(futures):
            row = futures[future]
            hashtag_id = row["id"]
            tag = row["tag"]
            platform = row["platform"]

            try:
                _, _, posts = future.result()
                inserted = 0
                for post_data in posts:
                    if not post_data.get("post_id"):
                        continue
                    raw_id = db.insert_post(conn, city_id, platform, post_data, hashtag_id)
                    if raw_id is not None:
                        inserted += 1
                conn.commit()

                db.update_hashtag_status(conn, hashtag_id, "completed")
                log.info(
                    "Stored %d posts from #%s (%s) for %s",
                    inserted,
                    tag,
                    platform,
                    city_name,
                )
                total_inserted += inserted

            except _SCRAPE_ERRORS:
                log.exception(
                    "Failed to scrape #%s (%s) for %s",
                    tag,
                    platform,
                    city_name,
                )
                db.update_hashtag_status(conn, hashtag_id, "failed")

    log.info(
        "Scraping complete for %s — %d posts stored from %d hashtags",
        city_name,
        total_inserted,
        len(pending),
    )
    return total_inserted
