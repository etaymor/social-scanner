"""Tests for pipeline.slideshow_types — shared data contracts."""

import json
from pathlib import Path

import pytest

from pipeline.slideshow_types import (
    CTASlideText,
    HookSlideText,
    LocationSlideText,
    PostMeta,
    SlideshowMeta,
    from_texts_json,
    load_post_meta,
    save_post_meta,
    to_meta_json,
    to_texts_json,
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
        for original, restored in zip(slides, loaded, strict=True):
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
# Type discriminator validation
# ---------------------------------------------------------------------------


class TestTypeDiscriminator:
    def test_hook_rejects_wrong_type(self):
        with pytest.raises(ValueError, match="must be 'hook'"):
            HookSlideText(type="location")

    def test_location_rejects_wrong_type(self):
        with pytest.raises(ValueError, match="must be 'location'"):
            LocationSlideText(type="hook")

    def test_cta_rejects_wrong_type(self):
        with pytest.raises(ValueError, match="must be 'cta'"):
            CTASlideText(type="hook")


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

    def test_japanese_meta_serializes(self, tmp_path: Path):
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

        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert loaded["city"] == "東京"
        assert loaded["places"][0]["name"] == "一蘭 渋谷店"
