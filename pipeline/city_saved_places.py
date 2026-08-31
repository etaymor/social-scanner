"""City-level repeating-saves ranking for Atlasi city guides.

Ranks places by distinct posts / authors + collectCount bonus — not by
``virality_score``. See docs/city-saved-places.md for method limitations.

Live Apify / OpenRouter runs stay in ``discover.py``; use ``--from-json`` to
replay an existing dataset without spending.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

# Honest published label. Do not replace with "most saved on TikTok".
RANK_METHOD = "places that keep appearing in saved-heavy {city} TikToks"

METHOD_LIMITATIONS = (
    "Not TikTok saved-collection counts",
    "Hashtag recall, not a city geo query",
    "collectCount is video saves, not place bookmarks",
)

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class DurablePlace:
    """A discovered place after official-ID resolution.

    ``google_place_id`` / coords are unset until ``places_resolve`` exists.
    ``resolution_status`` values: pending | resolved | out_of_city | unresolved.
    """

    place_id: int
    name: str
    city: str
    category: str | None
    place_type: str
    google_place_id: str | None = None
    lat: float | None = None
    lng: float | None = None
    formatted_address: str | None = None
    resolved_city: str | None = None
    resolution_status: str = "pending"
    neighborhood: str | None = None


@dataclass(frozen=True)
class PlaceRepeatStats:
    """Raw components used to rank. Export all of them; do not hide the method."""

    distinct_posts: int
    distinct_authors: int
    total_collect_count: int
    mention_count: int
    virality_score: float
    sample_urls: tuple[str, ...] = ()


@dataclass(frozen=True)
class RankedPlace:
    rank: int
    place: DurablePlace
    stats: PlaceRepeatStats
    repeat_save_score: float
    method: str


@dataclass(frozen=True)
class CityGuideExport:
    schema_version: int
    city: str
    category: str | None
    method: str
    method_limitations: tuple[str, ...]
    generated_at: str
    places: tuple[RankedPlace, ...] = field(default_factory=tuple)


class PlaceResolver(Protocol):
    """Resolve a extracted name to a durable venue in ``city``.

    Next build: wrap Google Places Text Search (see ``pipeline.photo_search``)
    with a city bias. Do not use the slideshow LLM ``in_city`` flag as the
    publish-time city filter.
    """

    def resolve(self, name: str, city: str) -> DurablePlace | None:
        """Return a resolved place, or None if lookup fails."""
        ...


def compute_repeat_save_score(stats: PlaceRepeatStats) -> float:
    """Proposed v1 score. Distinct authors first; video saves are a log bonus.

    repeat_save_score =
        distinct_authors * 3.0
      + distinct_posts   * 1.0
      + log1p(total_collect_count) * 0.5

    Gate on distinct_posts / distinct_authors in ``rank_repeating_saves``;
    do not publish a one-video wonder as "keeps repeating."
    """
    import math
    
    return (
        stats.distinct_authors * 3.0
        + stats.distinct_posts * 1.0
        + math.log1p(stats.total_collect_count) * 0.5
    )


def rank_repeating_saves(
    city: str,
    category: str | None = None,
    *,
    min_distinct_posts: int = 2,
    min_distinct_authors: int = 1,
    window_days: int | None = None,
    conn: object | None = None,
    include_near_misses: bool = False,
) -> CityGuideExport:
    """Read places + source posts for ``city`` and return a ranked export.

    Must not use ``places.virality_score`` as the published rank.

    Default gate: ≥2 distinct posts. Author gate defaults to 1 so a venue that
    truly repeats across videos from one guide account is not dropped — the
    previous ≥2 authors AND ≥2 posts gate hid real multi-mention venues when
    extraction split aliases. Pass ``min_distinct_authors=2`` for the stricter
    independent-author bar.
    """
    import sqlite3
    from datetime import datetime, timedelta, timezone
    from . import db
    from .listicle import is_overlay_junk

    own_conn = conn is None
    if own_conn:
        conn = db.get_connection()
    try:
        city_row = conn.execute("SELECT id FROM cities WHERE name = ?", (city,)).fetchone()
        if not city_row:
            raise ValueError(f"City not found: {city}")
        city_id = city_row["id"]

        where_parts = ["p.city_id = ?", "p.hidden = FALSE", "p.is_tourist_trap = FALSE"]
        params: list = [city_id]

        if category:
            where_parts.append("p.category = ?")
            params.append(category)

        where_parts.append("p.type NOT IN ('neighborhood', 'street')")
        where_clause = " AND ".join(where_parts)

        places = conn.execute(
            f"SELECT * FROM places p WHERE {where_clause} ORDER BY p.name",
            params,
        ).fetchall()

        cutoff_date = None
        if window_days is not None:
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()

        ranked: list[RankedPlace] = []
        near_misses: list[RankedPlace] = []

        for place in places:
            # Hide leftover overlay junk even when place_posts predate the upsert filter
            if is_overlay_junk(place["name"]):
                continue
            place_id = place["id"]

            post_query = """
                SELECT DISTINCT rp.id, rp.author, rp.saves, rp.url, rp.posted_at
                FROM raw_posts rp
                JOIN place_posts pp ON pp.post_id = rp.id
                WHERE pp.place_id = ?
            """
            post_params: list = [place_id]

            if cutoff_date:
                post_query += " AND rp.posted_at >= ?"
                post_params.append(cutoff_date)

            posts = conn.execute(post_query, post_params).fetchall()
            if not posts:
                continue

            distinct_posts = len(posts)
            distinct_authors = len({p["author"] for p in posts if p["author"]})
            total_collect_count = sum(p["saves"] or 0 for p in posts)

            stats = PlaceRepeatStats(
                distinct_posts=distinct_posts,
                distinct_authors=distinct_authors,
                total_collect_count=total_collect_count,
                mention_count=place["mention_count"],
                virality_score=place["virality_score"],
                sample_urls=tuple(p["url"] for p in posts[:3] if p["url"]),
            )
            durable = DurablePlace(
                place_id=place_id,
                name=place["name"],
                city=city,
                category=place["category"],
                place_type=place["type"],
            )
            item = RankedPlace(
                rank=0,
                place=durable,
                stats=stats,
                repeat_save_score=compute_repeat_save_score(stats),
                method=RANK_METHOD.format(city=city),
            )

            if (
                distinct_posts >= min_distinct_posts
                and distinct_authors >= min_distinct_authors
            ):
                ranked.append(item)
            elif include_near_misses and distinct_posts >= 1:
                near_misses.append(item)

        ranked.sort(key=lambda x: x.repeat_save_score, reverse=True)
        for i, item in enumerate(ranked, 1):
            ranked[i - 1] = RankedPlace(
                rank=i,
                place=item.place,
                stats=item.stats,
                repeat_save_score=item.repeat_save_score,
                method=item.method,
            )

        method = RANK_METHOD.format(city=city)
        if window_days:
            method = (
                f"places that keep appearing in saved-heavy {city} TikToks "
                f"from the last {window_days} days"
            )

        # Near-misses are appended after ranked rows with rank 0 for CSV inspection
        if include_near_misses:
            near_misses.sort(key=lambda x: x.stats.total_collect_count, reverse=True)
            ranked.extend(near_misses)

        return CityGuideExport(
            schema_version=SCHEMA_VERSION,
            city=city,
            category=category,
            method=method,
            method_limitations=METHOD_LIMITATIONS,
            generated_at=datetime.now(timezone.utc).isoformat(),
            places=tuple(ranked),
        )
    finally:
        if own_conn:
            conn.close()


def export_city_guide(
    payload: CityGuideExport,
    dest: Path,
    *,
    fmt: str = "json",
) -> Path:
    """Write JSON or CSV. ``fmt`` is ``json`` or ``csv`` only.

    CSV is a flat projection of ``payload.places``. JSON keeps method text
    and raw components. Neither format is an Atlasi page.
    """
    import csv
    import json
    
    if fmt == "json":
        # Convert to serializable dict
        data = {
            "schema_version": payload.schema_version,
            "city": payload.city,
            "category": payload.category,
            "method": payload.method,
            "method_limitations": list(payload.method_limitations),
            "generated_at": payload.generated_at,
            "places": [
                {
                    "rank": p.rank,
                    "name": p.place.name,
                    "place_type": p.place.place_type,
                    "category": p.place.category,
                    "distinct_posts": p.stats.distinct_posts,
                    "distinct_authors": p.stats.distinct_authors,
                    "total_collect_count": p.stats.total_collect_count,
                    "mention_count": p.stats.mention_count,
                    "repeat_save_score": round(p.repeat_save_score, 4),
                    "virality_score": round(p.stats.virality_score, 4),
                    "sample_urls": list(p.stats.sample_urls),
                }
                for p in payload.places
            ],
        }
        
        with open(dest, "w") as f:
            json.dump(data, f, indent=2)
    
    elif fmt == "csv":
        with open(dest, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "rank",
                "name",
                "place_type",
                "category",
                "distinct_posts",
                "distinct_authors",
                "total_collect_count",
                "mention_count",
                "repeat_save_score",
                "virality_score",
            ])
            
            for p in payload.places:
                writer.writerow([
                    p.rank,
                    p.place.name,
                    p.place.place_type,
                    p.place.category or "",
                    p.stats.distinct_posts,
                    p.stats.distinct_authors,
                    p.stats.total_collect_count,
                    p.stats.mention_count,
                    round(p.repeat_save_score, 4),
                    round(p.stats.virality_score, 4),
                ])
    
    else:
        raise ValueError(f"Unsupported format: {fmt}. Use 'json' or 'csv'.")
    
    return dest
