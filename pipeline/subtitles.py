"""Fetch TikTok WEBVTT subtitles and append spoken place names to captions."""

from __future__ import annotations

import logging
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

log = logging.getLogger(__name__)

_WEBVTT_SKIP = re.compile(r"^(WEBVTT|\d+|NOTE\b)", re.I)


def _parse_webvtt(body: str) -> str:
    lines: list[str] = []
    for line in body.splitlines():
        s = line.strip()
        if not s or "-->" in s or _WEBVTT_SKIP.match(s):
            continue
        # Drop simple cue timestamps leftover
        if re.fullmatch(r"[\d:\.]+", s):
            continue
        lines.append(s)
    # Dedupe consecutive repeats common in auto-captions
    out: list[str] = []
    for line in lines:
        if out and out[-1] == line:
            continue
        out.append(line)
    return " ".join(out)


def _fetch_one(post_id: int, url: str) -> tuple[int, str | None]:
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        text = _parse_webvtt(resp.text)
        return post_id, text or None
    except requests.RequestException:
        log.debug("Subtitle fetch failed for post %d", post_id, exc_info=True)
        return post_id, None


def enrich_captions_with_subtitles(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    batch_size: int = 50,
) -> int:
    """Append 🎙 Subtitles: … to captions for posts with subtitle_urls.

    Free — uses links already present on Apify items (no new Apify spend).
    """
    enriched = 0
    while True:
        posts = conn.execute(
            """SELECT id, caption, subtitle_urls FROM raw_posts
               WHERE city_id = ?
                 AND processed = FALSE
                 AND subtitle_urls IS NOT NULL AND subtitle_urls != ''
                 AND (caption IS NULL OR caption NOT LIKE '%🎙 Subtitles:%')
               LIMIT ?""",
            (city_id, batch_size),
        ).fetchall()
        if not posts:
            break

        futures = {}
        with ThreadPoolExecutor(max_workers=min(10, len(posts))) as pool:
            for post in posts:
                # Prefer first URL (mapper puts English first when available)
                url = (post["subtitle_urls"] or "").split("\n")[0].strip()
                if not url:
                    continue
                futures[pool.submit(_fetch_one, post["id"], url)] = post

            for future in as_completed(futures):
                post = futures[future]
                post_id, text = future.result()
                if not text:
                    continue
                existing = post["caption"] or ""
                updated = existing + f"\n🎙 Subtitles: {text}"
                conn.execute(
                    "UPDATE raw_posts SET caption = ? WHERE id = ?",
                    (updated, post_id),
                )
                enriched += 1

        conn.commit()

    log.info("Subtitle enrichment for %s: %d posts", city_name, enriched)
    return enriched
