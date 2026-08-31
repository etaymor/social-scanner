"""Replay an existing Apify dataset JSON into SQLite — no paid Apify call."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

from . import db
from .scraper import _filter_by_window, _map_tiktok, _passes_tiktok_filter

log = logging.getLogger(__name__)


def load_apify_items(path: str | Path) -> list[dict]:
    """Load a JSON array (or ``{items: [...]}``) of Apify dataset records."""
    raw = json.loads(Path(path).read_text())
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("items", "data", "posts", "places"):
            if isinstance(raw.get(key), list):
                return raw[key]
    raise ValueError(f"Unrecognized Apify JSON shape in {path}")


def import_apify_json(
    conn: sqlite3.Connection,
    city_id: int,
    path: str | Path,
    *,
    platform: str = "tiktok",
    window_days: int | None = None,
    apply_engagement_filter: bool = True,
) -> int:
    """Map Apify items → ``raw_posts``. Skips duplicates by ``(platform, post_id)``.

    Returns the number of newly inserted posts.
    """
    if platform != "tiktok":
        raise ValueError("Replay currently supports tiktok Apify exports only")

    items = load_apify_items(path)
    log.info("Replaying %d Apify items from %s", len(items), path)

    mapped = [_map_tiktok(item) for item in items]
    mapped = [p for p in mapped if p.get("post_id")]
    mapped = _filter_by_window(mapped, window_days)

    if apply_engagement_filter:
        before = len(mapped)
        mapped = [p for p in mapped if _passes_tiktok_filter(p)]
        log.info("Engagement filter: kept %d/%d posts", len(mapped), before)

    conn.execute(
        """INSERT OR IGNORE INTO hashtags (city_id, tag, platform, scrape_status, category)
           VALUES (?, ?, 'tiktok', 'completed', 'food_and_drink')""",
        (city_id, "replay_import"),
    )
    conn.commit()
    tag_row = conn.execute(
        "SELECT id FROM hashtags WHERE city_id = ? AND tag = ? AND platform = 'tiktok'",
        (city_id, "replay_import"),
    ).fetchone()
    hashtag_id = tag_row["id"]

    inserted = 0
    for post_data in mapped:
        existing = conn.execute(
            "SELECT id FROM raw_posts WHERE platform = ? AND post_id = ?",
            (platform, post_data["post_id"]),
        ).fetchone()
        raw_id = db.insert_post(conn, city_id, platform, post_data, hashtag_id)
        if raw_id is not None and existing is None:
            inserted += 1

    conn.commit()
    log.info("Replay inserted %d new posts for city_id=%d", inserted, city_id)
    return inserted
