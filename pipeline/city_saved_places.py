"""City-level "repeating saves" ranking — interface sketch only.

NOT IMPLEMENTED. See docs/city-saved-places.md.

This module is the extension surface for a later job:

    city in → durable place records → ranked "keeps repeating as a save"
    list → JSON/CSV export for Atlasi

Do not implement the ranker inside ``pipeline.scorer`` (that score is
engagement-rate virality for slideshows). Do not scrape user bookmark
lists. Do not generate an Atlasi page or a guide slideshow here.

Fill these types in on the Tokyo-first build. Live Apify / OpenRouter
runs stay in ``discover.py``.
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
    raise NotImplementedError(
        "Ranking is not implemented. See docs/city-saved-places.md."
    )


def rank_repeating_saves(
    city: str,
    category: str | None = None,
    *,
    min_distinct_posts: int = 2,
) -> CityGuideExport:
    """Read resolved places + source posts for ``city`` and return a ranked export.

    Must not use ``places.virality_score`` as the published rank.
    Must skip rows with resolution_status other than ``resolved``.
    """
    raise NotImplementedError(
        "City saved-place ranking is not implemented. See docs/city-saved-places.md."
    )


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
    raise NotImplementedError(
        "City guide export is not implemented. See docs/city-saved-places.md."
    )
