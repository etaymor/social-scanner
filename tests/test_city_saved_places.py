"""Contract tests for the city saved-places interface stub.

Ranking and export are intentionally unimplemented. These tests lock the
public surface so the Tokyo-first build extends this module instead of
overloading virality scoring.
"""

from pathlib import Path

import pytest

from pipeline.city_saved_places import (
    METHOD_LIMITATIONS,
    RANK_METHOD,
    SCHEMA_VERSION,
    CityGuideExport,
    PlaceRepeatStats,
    compute_repeat_save_score,
    export_city_guide,
    rank_repeating_saves,
)


def test_schema_and_method_label_are_honest():
    assert SCHEMA_VERSION == 1
    assert "most saved" not in RANK_METHOD.lower()
    assert any("bookmark" in item.lower() or "collection" in item.lower() for item in METHOD_LIMITATIONS)


def test_compute_repeat_save_score_is_stubbed():
    stats = PlaceRepeatStats(
        distinct_posts=3,
        distinct_authors=2,
        total_collect_count=100,
        mention_count=3,
        virality_score=0.1,
    )
    with pytest.raises(NotImplementedError, match="city-saved-places"):
        compute_repeat_save_score(stats)


def test_rank_and_export_are_stubbed(tmp_path: Path):
    with pytest.raises(NotImplementedError, match="city-saved-places"):
        rank_repeating_saves("Tokyo", category="food_and_drink")

    payload = CityGuideExport(
        schema_version=SCHEMA_VERSION,
        city="Tokyo",
        category="food_and_drink",
        method=RANK_METHOD.format(city="Tokyo"),
        method_limitations=METHOD_LIMITATIONS,
        generated_at="2026-08-25T00:00:00+00:00",
    )
    with pytest.raises(NotImplementedError, match="city-saved-places"):
        export_city_guide(payload, tmp_path / "tokyo.json")
