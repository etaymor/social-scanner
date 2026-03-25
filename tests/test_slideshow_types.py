"""Tests for pipeline.slideshow_types — shared data contracts."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.slideshow_types import (
    CTASlideText,
    HookSlideText,
    LocationSlideText,
    PostMeta,
    SlideshowMeta,
    from_meta_json,
    from_texts_json,
    load_post_meta,
    save_post_meta,
    to_meta_json,
    to_texts_json,
    validate_slides,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_slides(location_count: int) -> list:
    """Build a valid slide list with the given number of location slides."""
    slides: list = [HookSlideText(text="Top spots!")]
    for i in range(1, location_count + 1):
        slides.append(
            LocationSlideText(
                name=f"Place {i}",
                neighborhood=f"Area {i}",
                number=f"{i}/{location_count}",
            )
        )
    slides.append(CTASlideText(text="Follow for more!"))
    return slides


# ---------------------------------------------------------------------------
# Round-trip: texts.json
# ---------------------------------------------------------------------------

class TestTextsJsonRoundTrip:
    def test_round_trip(self, tmp_path: Path):
        slides = _make_slides(3)
        path = tmp_path / "texts.json"
        path.write_text(to_texts_json(slides), encoding="utf-8")

        loaded = from_texts_json(path)
        assert len(loaded) == len(slides)
        for original, restored in zip(slides, loaded):
            assert type(original) is type(restored)
            assert original == restored

    def test_returns_correct_subtypes(self, tmp_path: Path):
        slides = _make_slides(2)
        path = tmp_path / "texts.json"
        path.write_text(to_texts_json(slides), encoding="utf-8")

        loaded = from_texts_json(path)
        assert isinstance(loaded[0], HookSlideText)
        assert isinstance(loaded[1], LocationSlideText)
        assert isinstance(loaded[2], LocationSlideText)
        assert isinstance(loaded[3], CTASlideText)


# ---------------------------------------------------------------------------
# Round-trip: meta.json
# ---------------------------------------------------------------------------

class TestMetaJsonRoundTrip:
    def test_round_trip(self, tmp_path: Path):
        meta = SlideshowMeta(
            city="Tokyo",
            category="food_and_drink",
            format="listicle",
            hook_text="Best ramen in Tokyo!",
            slide_count=8,
            created_at="2026-03-24T12:00:00Z",
            places=[
                {"id": 1, "name": "Ichiran", "neighborhood": "Shibuya"},
                {"id": 2, "name": "Fuunji", "neighborhood": "Shinjuku"},
            ],
        )
        path = tmp_path / "meta.json"
        path.write_text(to_meta_json(meta), encoding="utf-8")

        loaded = from_meta_json(path)
        assert loaded == meta
        assert loaded.city == "Tokyo"
        assert loaded.places[0]["name"] == "Ichiran"


# ---------------------------------------------------------------------------
# Round-trip: post_meta.json
# ---------------------------------------------------------------------------

class TestPostMetaRoundTrip:
    def test_round_trip(self, tmp_path: Path):
        meta = PostMeta(
            postiz_post_id="abc-123",
            posted_at="2026-03-24T14:00:00Z",
            platform="tiktok",
            privacy_level="SELF_ONLY",
        )
        path = tmp_path / "post_meta.json"
        save_post_meta(meta, path)

        loaded = load_post_meta(path)
        assert loaded == meta
        assert loaded.postiz_post_id == "abc-123"


# ---------------------------------------------------------------------------
# Validation — rejection cases
# ---------------------------------------------------------------------------

class TestValidationRejects:
    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="empty"):
            validate_slides([])

    def test_rejects_wrong_first_type(self):
        slides = [
            LocationSlideText(name="X", neighborhood="Y", number="1/1"),
            CTASlideText(text="Follow!"),
        ]
        with pytest.raises(ValueError, match="First slide must be type 'hook'"):
            validate_slides(slides)

    def test_rejects_wrong_last_type(self):
        slides = [
            HookSlideText(text="Hook!"),
            LocationSlideText(name="X", neighborhood="Y", number="1/1"),
        ]
        with pytest.raises(ValueError, match="Last slide must be type 'cta'"):
            validate_slides(slides)

    def test_rejects_non_sequential_numbers(self):
        slides = [
            HookSlideText(text="Hook!"),
            LocationSlideText(name="A", neighborhood="N", number="1/3"),
            LocationSlideText(name="B", neighborhood="N", number="3/3"),  # skips 2
            LocationSlideText(name="C", neighborhood="N", number="2/3"),
            CTASlideText(text="Follow!"),
        ]
        with pytest.raises(ValueError, match="expected '2/3'"):
            validate_slides(slides)

    def test_rejects_wrong_total_in_number(self):
        slides = [
            HookSlideText(text="Hook!"),
            LocationSlideText(name="A", neighborhood="N", number="1/5"),
            LocationSlideText(name="B", neighborhood="N", number="2/5"),
            CTASlideText(text="Follow!"),
        ]
        with pytest.raises(ValueError, match="expected '1/2'"):
            validate_slides(slides)


# ---------------------------------------------------------------------------
# Validation — acceptance cases
# ---------------------------------------------------------------------------

class TestValidationAccepts:
    @pytest.mark.parametrize("count", [4, 8, 15])
    def test_accepts_variable_location_counts(self, count: int):
        """Validates slide sets with 6, 10, 17 total slides (hook + N + cta)."""
        slides = _make_slides(count)
        validate_slides(slides)  # should not raise

    def test_accepts_minimal_set(self):
        """Hook + 1 location + CTA = 3 slides."""
        slides = _make_slides(1)
        validate_slides(slides)


# ---------------------------------------------------------------------------
# Non-Latin characters
# ---------------------------------------------------------------------------

class TestNonLatinCharacters:
    def test_japanese_place_names_round_trip(self, tmp_path: Path):
        slides = [
            HookSlideText(text="東京のベストスポット！"),
            LocationSlideText(
                name="一蘭 渋谷店",
                neighborhood="渋谷区",
                number="1/2",
            ),
            LocationSlideText(
                name="ふうんじ",
                neighborhood="新宿区",
                number="2/2",
            ),
            CTASlideText(text="フォローしてね！"),
        ]
        path = tmp_path / "texts.json"
        path.write_text(to_texts_json(slides), encoding="utf-8")

        loaded = from_texts_json(path)
        assert loaded[0].text == "東京のベストスポット！"
        assert loaded[1].name == "一蘭 渋谷店"
        assert loaded[2].neighborhood == "新宿区"
        assert loaded[3].text == "フォローしてね！"

    def test_japanese_meta_round_trip(self, tmp_path: Path):
        meta = SlideshowMeta(
            city="東京",
            category="food_and_drink",
            format="listicle",
            hook_text="東京のベストスポット！",
            slide_count=4,
            created_at="2026-03-24T12:00:00Z",
            places=[
                {"id": 1, "name": "一蘭 渋谷店", "neighborhood": "渋谷区"},
            ],
        )
        path = tmp_path / "meta.json"
        path.write_text(to_meta_json(meta), encoding="utf-8")

        loaded = from_meta_json(path)
        assert loaded.city == "東京"
        assert loaded.places[0]["name"] == "一蘭 渋谷店"
