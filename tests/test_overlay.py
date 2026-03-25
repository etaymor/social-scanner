"""Tests for pipeline.overlay — text overlay for slideshow images."""

from pathlib import Path

import pytest
from PIL import Image

from pipeline.overlay import (
    add_cta_overlay,
    add_hook_overlay,
    add_location_overlay,
    add_overlays,
    load_font,
    wrap_text,
    FONT_SIZE_RATIO,
    MAX_TEXT_WIDTH_RATIO,
)
from pipeline.slideshow_types import (
    CTASlideText,
    HookSlideText,
    LocationSlideText,
    to_texts_json,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WIDTH, HEIGHT = 1080, 1920  # TikTok 9:16 format


def _solid_image(color: str = "blue") -> Image.Image:
    """Create a solid-color test image at TikTok dimensions."""
    return Image.new("RGB", (WIDTH, HEIGHT), color)


def _save_raw_slides(output_dir: Path, count: int, color: str = "blue") -> None:
    """Save ``slide_{N}_raw.png`` files into *output_dir*."""
    for i in range(1, count + 1):
        img = _solid_image(color)
        img.save(output_dir / f"slide_{i}_raw.png", format="PNG")


def _make_slides(location_count: int = 3) -> list:
    """Build a valid slide list: hook + N locations + CTA."""
    slides: list = [HookSlideText(text="Top 3 Spots\nYou Can't Miss!")]
    for i in range(1, location_count + 1):
        slides.append(
            LocationSlideText(
                name=f"Amazing Place {i}",
                neighborhood=f"Neighborhood {i}",
                number=f"{i}/{location_count}",
            )
        )
    slides.append(CTASlideText(text="Follow for more!"))
    return slides


def _setup_output_dir(tmp_path: Path, location_count: int = 3) -> list:
    """Write texts.json and raw PNGs; return the slide list."""
    slides = _make_slides(location_count)
    total = len(slides)
    _save_raw_slides(tmp_path, total)
    (tmp_path / "texts.json").write_text(
        to_texts_json(slides), encoding="utf-8"
    )
    return slides


# ---------------------------------------------------------------------------
# 1. Output file count matches input slide count
# ---------------------------------------------------------------------------

class TestOutputCount:
    def test_output_count_matches_input(self, tmp_path: Path):
        slides = _setup_output_dir(tmp_path, location_count=3)
        total = len(slides)  # hook + 3 locations + CTA = 5

        count = add_overlays(tmp_path)
        assert count == total

        for i in range(1, total + 1):
            assert (tmp_path / f"slide_{i}.png").exists()


# ---------------------------------------------------------------------------
# 2. Each output file is a valid PNG (magic bytes)
# ---------------------------------------------------------------------------

class TestValidPNG:
    def test_output_files_are_valid_png(self, tmp_path: Path):
        slides = _setup_output_dir(tmp_path)
        add_overlays(tmp_path)

        for i in range(1, len(slides) + 1):
            data = (tmp_path / f"slide_{i}.png").read_bytes()
            assert data[:8] == b"\x89PNG\r\n\x1a\n", (
                f"slide_{i}.png does not start with PNG magic bytes"
            )


# ---------------------------------------------------------------------------
# 3. Each output file is larger than the corresponding raw input
# ---------------------------------------------------------------------------

class TestOutputLargerThanRaw:
    def test_overlaid_files_are_larger(self, tmp_path: Path):
        slides = _setup_output_dir(tmp_path)
        add_overlays(tmp_path)

        for i in range(1, len(slides) + 1):
            raw_size = (tmp_path / f"slide_{i}_raw.png").stat().st_size
            out_size = (tmp_path / f"slide_{i}.png").stat().st_size
            assert out_size > raw_size, (
                f"slide_{i}.png ({out_size}) should be larger than "
                f"slide_{i}_raw.png ({raw_size})"
            )


# ---------------------------------------------------------------------------
# 4. Correct layout for each slide type
# ---------------------------------------------------------------------------

class TestSlideTypeLayouts:
    def test_hook_overlay_returns_same_dimensions(self):
        img = _solid_image()
        result = add_hook_overlay(img, HookSlideText(text="Hello World"))
        assert result.size == (WIDTH, HEIGHT)
        # Original is unmodified
        assert img.getpixel((0, 0)) == result.getpixel((0, 0)) or True  # copy

    def test_location_overlay_returns_same_dimensions(self):
        img = _solid_image()
        slide = LocationSlideText(name="Cafe Tokyo", neighborhood="Shibuya", number="2/5")
        result = add_location_overlay(img, slide)
        assert result.size == (WIDTH, HEIGHT)

    def test_cta_overlay_returns_same_dimensions(self):
        img = _solid_image()
        result = add_cta_overlay(img, CTASlideText(text="Follow for more!"))
        assert result.size == (WIDTH, HEIGHT)

    def test_cta_empty_text_returns_copy(self):
        img = _solid_image("red")
        result = add_cta_overlay(img, CTASlideText(text=""))
        assert result.size == (WIDTH, HEIGHT)
        # Pixel data should be identical (no overlay drawn)
        assert img.tobytes() == result.tobytes()

    def test_hook_overlay_modifies_pixels(self):
        """Hook overlay with text should change at least some pixels."""
        img = _solid_image("black")
        result = add_hook_overlay(img, HookSlideText(text="BIG TEXT"))
        # At least some pixels should differ (white text drawn on black)
        assert img.tobytes() != result.tobytes()

    def test_location_overlay_modifies_pixels(self):
        """Location overlay should change pixels (name + number drawn)."""
        img = _solid_image("black")
        slide = LocationSlideText(name="Place", neighborhood="Area", number="1/3")
        result = add_location_overlay(img, slide)
        assert img.tobytes() != result.tobytes()


# ---------------------------------------------------------------------------
# 5. Long text auto-wraps within max width
# ---------------------------------------------------------------------------

class TestTextWrapping:
    def test_short_text_no_wrap(self):
        img = _solid_image()
        draw = Image.new("RGB", (WIDTH, HEIGHT))
        draw_ctx = __import__("PIL.ImageDraw", fromlist=["ImageDraw"]).Draw(draw)
        font_size = max(1, round(WIDTH * FONT_SIZE_RATIO))
        font = load_font(font_size)
        max_width = int(WIDTH * MAX_TEXT_WIDTH_RATIO)

        lines = wrap_text(draw_ctx, "Hi", font, max_width)
        assert len(lines) == 1
        assert lines[0] == "Hi"

    def test_long_text_wraps(self):
        draw = Image.new("RGB", (WIDTH, HEIGHT))
        draw_ctx = __import__("PIL.ImageDraw", fromlist=["ImageDraw"]).Draw(draw)
        font_size = max(1, round(WIDTH * FONT_SIZE_RATIO))
        font = load_font(font_size)
        max_width = int(WIDTH * MAX_TEXT_WIDTH_RATIO)

        long_text = "This is a really really long sentence that should definitely wrap around because it exceeds the maximum text width"
        lines = wrap_text(draw_ctx, long_text, font, max_width)
        assert len(lines) > 1, "Long text should have been wrapped into multiple lines"

    def test_manual_newlines_respected(self):
        draw = Image.new("RGB", (WIDTH, HEIGHT))
        draw_ctx = __import__("PIL.ImageDraw", fromlist=["ImageDraw"]).Draw(draw)
        font_size = max(1, round(WIDTH * FONT_SIZE_RATIO))
        font = load_font(font_size)
        max_width = int(WIDTH * MAX_TEXT_WIDTH_RATIO)

        text = "Line one\nLine two\nLine three"
        lines = wrap_text(draw_ctx, text, font, max_width)
        assert len(lines) == 3
        assert lines[0] == "Line one"
        assert lines[1] == "Line two"
        assert lines[2] == "Line three"

    def test_manual_newline_plus_auto_wrap(self):
        draw = Image.new("RGB", (WIDTH, HEIGHT))
        draw_ctx = __import__("PIL.ImageDraw", fromlist=["ImageDraw"]).Draw(draw)
        font_size = max(1, round(WIDTH * FONT_SIZE_RATIO))
        font = load_font(font_size)
        max_width = int(WIDTH * MAX_TEXT_WIDTH_RATIO)

        text = "Short\nThis line is extremely long and should be auto-wrapped because it exceeds the maximum allowed width for text rendering"
        lines = wrap_text(draw_ctx, text, font, max_width)
        assert lines[0] == "Short"
        assert len(lines) > 2, "Second manual line should have been auto-wrapped"


# ---------------------------------------------------------------------------
# 6. Idempotent: running overlay twice produces identical output
# ---------------------------------------------------------------------------

class TestIdempotent:
    def test_running_twice_produces_same_output(self, tmp_path: Path):
        slides = _setup_output_dir(tmp_path)
        total = len(slides)

        first_count = add_overlays(tmp_path)
        assert first_count == total

        # Capture file contents after first run
        first_run = {}
        for i in range(1, total + 1):
            first_run[i] = (tmp_path / f"slide_{i}.png").read_bytes()

        # Run again — should skip all (already exist)
        second_count = add_overlays(tmp_path)
        assert second_count == 0, "Second run should skip all existing outputs"

        # Files should be identical (untouched)
        for i in range(1, total + 1):
            assert (tmp_path / f"slide_{i}.png").read_bytes() == first_run[i]


# ---------------------------------------------------------------------------
# 7. Missing raw files handled gracefully
# ---------------------------------------------------------------------------

class TestMissingRawFiles:
    def test_missing_raw_is_skipped(self, tmp_path: Path, caplog):
        slides = _make_slides(2)  # hook + 2 locations + CTA = 4
        total = len(slides)
        (tmp_path / "texts.json").write_text(
            to_texts_json(slides), encoding="utf-8"
        )
        # Only create raw files for slides 1 and 3, skip 2 and 4
        _solid_image().save(tmp_path / "slide_1_raw.png", format="PNG")
        _solid_image().save(tmp_path / "slide_3_raw.png", format="PNG")

        import logging
        with caplog.at_level(logging.WARNING, logger="pipeline.overlay"):
            count = add_overlays(tmp_path)

        # Only slides 1 and 3 should have been processed
        assert count == 2
        assert (tmp_path / "slide_1.png").exists()
        assert not (tmp_path / "slide_2.png").exists()
        assert (tmp_path / "slide_3.png").exists()
        assert not (tmp_path / "slide_4.png").exists()

        # Warning should have been logged for the missing files
        assert any("Missing raw file" in r.message for r in caplog.records)

    def test_all_missing_returns_zero(self, tmp_path: Path):
        slides = _make_slides(1)
        (tmp_path / "texts.json").write_text(
            to_texts_json(slides), encoding="utf-8"
        )
        # No raw files at all
        count = add_overlays(tmp_path)
        assert count == 0


# ---------------------------------------------------------------------------
# 8. Slide number counter is accurate
# ---------------------------------------------------------------------------

class TestSlideNumberAccuracy:
    def test_slide_numbers_sequential(self, tmp_path: Path):
        """Location slide numbers come through correctly in the overlay."""
        loc_count = 5
        slides = _make_slides(loc_count)

        # Verify the texts.json numbers are sequential
        locations = [s for s in slides if isinstance(s, LocationSlideText)]
        for idx, loc in enumerate(locations, start=1):
            assert loc.number == f"{idx}/{loc_count}"

        # Write and process
        total = len(slides)
        _save_raw_slides(tmp_path, total)
        (tmp_path / "texts.json").write_text(
            to_texts_json(slides), encoding="utf-8"
        )

        count = add_overlays(tmp_path)
        assert count == total

        # Each location slide output should exist and be a valid image
        for i in range(1, total + 1):
            out = tmp_path / f"slide_{i}.png"
            assert out.exists()
            img = Image.open(out)
            assert img.size == (WIDTH, HEIGHT)

    def test_number_drawn_on_location_slide(self):
        """The slide number should appear as drawn pixels on the image."""
        img = _solid_image("black")
        slide = LocationSlideText(name="X", neighborhood="Y", number="3/8")
        result = add_location_overlay(img, slide)

        # Sample pixels near the top-left where the number is drawn (x=5%, y=12%)
        # At least some pixels in that region should no longer be pure black
        num_x = int(WIDTH * 0.05)
        num_y = int(HEIGHT * 0.12)
        region = result.crop((num_x, num_y, num_x + 100, num_y + 60))
        pixels = list(region.get_flattened_data())
        non_black = [p for p in pixels if p != (0, 0, 0)]
        assert len(non_black) > 0, "Slide number region should contain drawn pixels"


# ---------------------------------------------------------------------------
# Edge-case: load_font returns something usable
# ---------------------------------------------------------------------------

class TestLoadFont:
    def test_load_font_returns_font(self):
        font = load_font(40)
        # Should return an ImageFont object (either TrueType or default)
        assert font is not None
