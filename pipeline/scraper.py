"""Step 2 — Scrape TikTok & Instagram posts via Apify for pending hashtags."""

import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

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


def _subtitle_urls(video_meta: dict) -> str:
    """Prefer English subtitle download links; store newline-separated."""
    links = video_meta.get("subtitleLinks") or []
    if not isinstance(links, list):
        return ""
    ordered = sorted(
        links,
        key=lambda s: 0 if isinstance(s, dict) and s.get("language") == "en" else 1,
    )
    urls = []
    for entry in ordered:
        if not isinstance(entry, dict):
            continue
        url = (entry.get("downloadLink") or "").strip()
        if url:
            urls.append(url)
    return "\n".join(urls)


def _slideshow_urls(item: dict) -> str:
    """Collect slideshow / carousel image URLs when the actor provides them."""
    urls: list[str] = []
    media = item.get("mediaUrls") or []
    if isinstance(media, list):
        for u in media:
            if isinstance(u, str) and u.strip():
                urls.append(u.strip())
    # Some actor builds nest slideshow images here
    for key in ("slideshowImageLinks", "imageUrls"):
        extra = item.get(key) or []
        if isinstance(extra, list):
            for entry in extra:
                if isinstance(entry, str) and entry.strip():
                    urls.append(entry.strip())
                elif isinstance(entry, dict):
                    u = entry.get("url") or entry.get("downloadLink") or ""
                    if u:
                        urls.append(u.strip())
    # Dedupe preserve order
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return "\n".join(out)


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

    # Get collectCount from top-level OR stats (fallback)
    saves = item.get("collectCount") or stats.get("collectCount", 0)

    return {
        "post_id": post_id,
        "caption": caption,
        "likes": item.get("diggCount") or stats.get("diggCount", 0),
        "comments": item.get("commentCount") or stats.get("commentCount", 0),
        "shares": item.get("shareCount") or stats.get("shareCount", 0),
        "saves": saves,
        "views": item.get("playCount") or stats.get("playCount", 0),
        "url": (
            item.get("webVideoUrl") or f"https://www.tiktok.com/@{author_name}/video/{post_id}"
        ),
        "author": author_name,
        "created_at": item.get("createTime"),
        "cover_url": cover_url,
        "subtitle_urls": _subtitle_urls(video_meta),
        "slideshow_urls": _slideshow_urls(item),
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

# Cap resultsPerPage for TikTok — this is per-hashtag, so keep it low (can be lifted with --max-posts)
_TIKTOK_MAX_PER_HASHTAG = 30


def _generate_search_queries(city_name: str, category: str | None = None) -> list[str]:
    """Generate search queries for TikTok search mode (itinerary and must-eat focused).
    
    Returns queries that target listicle/guide content rather than single-shop features.
    Focuses on overlap with itinerary and must-eat recommendations.
    """
    city = city_name.lower()
    queries = []
    
    if category == "food_and_drink" or category is None:
        # Itinerary-focused queries (English)
        queries.extend([
            f"{city} itinerary",
            f"{city} must eat",
            f"{city} food guide",
            f"{city} best restaurants",
            f"{city} eats",
        ])
        
        # City-specific localized queries
        if city == "tokyo":
            queries.extend([
                "東京 グルメ おすすめ",  # Tokyo gourmet recommendations
            ])
        elif city == "seoul":
            queries.extend([
                "서울 맛집 추천",  # Seoul restaurant recommendations
            ])

    return queries


def _parse_timestamp(ts: str | int | None) -> datetime | None:
    """Parse a TikTok/Instagram timestamp to datetime. Returns None if unparseable."""
    if ts is None:
        return None
    try:
        if isinstance(ts, int):
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if isinstance(ts, str):
            # Try ISO format first
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                # Try as integer timestamp
                return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None
    return None


def _filter_by_window(posts: list[dict], window_days: int | None) -> list[dict]:
    """Client-side filter posts to only include those from the last window_days."""
    if window_days is None:
        return posts

    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    filtered = []
    for post in posts:
        created_dt = _parse_timestamp(post.get("created_at"))
        if created_dt and created_dt >= cutoff:
            filtered.append(post)

    dropped = len(posts) - len(filtered)
    if dropped:
        log.info("Filtered out %d posts outside %d-day window", dropped, window_days)

    return filtered


def _filter_city_relevant(posts: list[dict], city_name: str) -> list[dict]:
    """Drop obvious off-topic search junk (Roblox, recipes, etc.) lacking city cues."""
    city = city_name.lower()
    tokens = {city, city.replace(" ", "")}
    if city == "seoul":
        tokens.update({"한국", "서울", "korea", "korean"})
    elif city == "tokyo":
        tokens.update({"東京", "japan", "japanese", "tokyo"})

    kept = []
    for post in posts:
        caption = (post.get("caption") or "").lower()
        if any(t in caption for t in tokens):
            kept.append(post)
            continue
        # Keep posts with a non-empty location tag (already appended to caption as 📍)
        if "📍 location tag:" in caption:
            kept.append(post)
    dropped = len(posts) - len(kept)
    if dropped:
        log.info(
            "Filtered out %d/%d posts lacking %s relevance cues",
            dropped,
            len(posts),
            city_name,
        )
    return kept


def _scrape_batch(
    client: ApifyClient,
    platform: str,
    tags: list[str],
    max_posts: int,
    window_days: int | None = None,
    search_queries: list[str] | None = None,
    city_name: str | None = None,
) -> list[dict]:
    """Run ONE Apify actor call for all *tags* and return mapped post dicts."""
    if platform == "tiktok":
        actor = client.actor(config.TIKTOK_ACTOR)
        # Lift the 30-post cap when user specifies --max-posts explicitly
        per_hashtag = max_posts
        run_input = {"hashtags": tags, "resultsPerPage": per_hashtag}

        # Add search queries if provided (search mode)
        if search_queries:
            run_input["searchQueries"] = search_queries
            run_input["searchSection"] = "/video"
            # PAST_MONTH is the schema-valid last-30 filter. THIS_MONTH was rejected
            # by clockworks/free-tiktok-scraper (verified on Seoul run 5vvdeReFQ6O9wzbct).
            if window_days is not None and window_days <= 31:
                run_input["videoSearchDateFilter"] = "PAST_MONTH"

        # Ask the actor for assets we need for on-screen extraction. Skipping these
        # (as the Seoul paid run did) means slideshow names never enter the corpus.
        run_input["shouldDownloadSlideshowImages"] = True
        run_input["shouldDownloadCovers"] = False  # remote coverUrl is enough
        run_input["downloadSubtitlesOptions"] = "DOWNLOAD_IF_AVAILABLE"

        # For hashtag scraping with window_days, use oldestPostDateUnified if available
        if window_days is not None and tags:
            cutoff_dt = datetime.now(timezone.utc) - timedelta(days=window_days)
            run_input["oldestPostDateUnified"] = cutoff_dt.isoformat()

        run = actor.call(
            run_input=run_input,
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

    # Filter by date window (client-side)
    mapped = _filter_by_window(mapped, window_days)

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

    if city_name and search_queries:
        mapped = _filter_city_relevant(mapped, city_name)

    return mapped


def scrape_posts(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    max_posts: int = 100,
    window_days: int | None = None,
    search_mode: bool = False,
) -> int:
    """Scrape pending hashtags for *city_id* and store qualifying posts.

    Groups all pending hashtags by platform and sends ONE Apify actor call
    per platform (typically 2 total: one TikTok, one Instagram).

    When window_days is set, posts are filtered client-side to only include
    those from the last N days. When search_mode is True, TikTok scraping
    uses searchQueries in addition to hashtags.

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

    # Generate search queries for TikTok if search mode is enabled
    search_queries = None
    if search_mode:
        # Get category from first hashtag row (they should all be same category)
        category = pending[0]["category"] if pending else None
        search_queries = _generate_search_queries(city_name, category)
        if search_queries:
            log.info("Generated %d search queries for search mode", len(search_queries))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {}
        for platform, rows in batches.items():
            tags = [r["tag"] for r in rows]
            # Only pass search queries to TikTok
            queries = search_queries if platform == "tiktok" else None
            future = pool.submit(
                _scrape_batch,
                client,
                platform,
                tags,
                max_posts,
                window_days,
                queries,
                city_name,
            )
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
                    existing = conn.execute(
                        "SELECT id FROM raw_posts WHERE platform = ? AND post_id = ?",
                        (platform, post_data["post_id"]),
                    ).fetchone()
                    # Insert post linked to first hashtag (no re-download of known posts)
                    raw_id = db.insert_post(conn, city_id, platform, post_data, hashtag_ids[0])
                    if raw_id is not None:
                        if existing is None:
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
