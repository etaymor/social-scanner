"""Text overlay for slideshow images using Pillow.

Ports Larry's proven overlay parameters (node-canvas) to Pillow,
supporting hook, location, and CTA slide types.
"""

import logging
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from pipeline.slideshow_types import (
    CTASlideText,
    HookSlideText,
    LocationSlideText,
    from_texts_json,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — Larry's proven viral-format parameters
# ---------------------------------------------------------------------------

FONT_SIZE_RATIO = 0.065          # 6.5 % of image width
STROKE_WIDTH_RATIO = 0.15        # 15 % of font size
MAX_TEXT_WIDTH_RATIO = 0.75       # 75 % of image width
TEXT_Y_POSITION_RATIO = 0.30      # 30 % from top
SAFE_ZONE_TOP_RATIO = 0.10       # top 10 % is safe zone
SAFE_ZONE_BOTTOM_RATIO = 0.20    # bottom 20 % is safe zone
LINE_HEIGHT_RATIO = 1.25          # 125 % of font size
SLIDE_NUMBER_FONT_RATIO = 0.04   # smaller font for slide number
NEIGHBORHOOD_FONT_RATIO = 0.045  # slightly smaller for neighborhood text

# ---------------------------------------------------------------------------
# Font loading
# ---------------------------------------------------------------------------

_FONT_PATHS = [
    "assets/fonts/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
]


def load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Load a TrueType font at *size* pixels, falling back to Pillow default."""
    for font_path in _FONT_PATHS:
        try:
            return ImageFont.truetype(font_path, size)
        except (OSError, IOError):
            continue

    logger.warning(
        "Could not load any TrueType font; falling back to Pillow default. "
        "Text may look basic.  Install NotoSansCJK-Bold.ttc into assets/fonts/ "
        "for production quality."
    )
    return ImageFont.load_default()


# ---------------------------------------------------------------------------
# Text wrapping (ported from Larry's wrapText)
# ---------------------------------------------------------------------------

def wrap_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    max_width: int,
) -> list[str]:
    """Word-wrap *text* so every line fits within *max_width* pixels.

    Manual ``\\n`` line breaks are respected first; any resulting line that
    still exceeds *max_width* is split further at word boundaries.
    """
    manual_lines = text.split("\n")
    wrapped: list[str] = []

    for line in manual_lines:
        line = line.strip()
        bbox = draw.textbbox((0, 0), line, font=font)
        line_width = bbox[2] - bbox[0]
        if line_width <= max_width:
            wrapped.append(line)
            continue

        # Auto-wrap at word boundaries
        words = line.split()
        current = ""
        for word in words:
            test = f"{current} {word}".strip() if current else word
            bbox = draw.textbbox((0, 0), test, font=font)
            test_width = bbox[2] - bbox[0]
            if test_width <= max_width:
                current = test
            else:
                if current:
                    wrapped.append(current)
                current = word
        if current:
            wrapped.append(current)

    return wrapped


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_y(start_y: float, total_text_height: float, img_height: int) -> int:
    """Clamp *start_y* so the text block stays inside safe zones."""
    min_y = img_height * SAFE_ZONE_TOP_RATIO
    max_y = img_height * (1 - SAFE_ZONE_BOTTOM_RATIO) - total_text_height
    return int(max(min_y, min(start_y, max_y)))


def _draw_text_with_stroke(
    draw: ImageDraw.ImageDraw,
    xy: tuple[float, float],
    text: str,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    stroke_width: int,
    anchor: str | None = None,
) -> None:
    """Draw white text with a black stroke (outline)."""
    draw.text(
        xy,
        text,
        font=font,
        fill="white",
        stroke_fill="black",
        stroke_width=stroke_width,
        anchor=anchor,
    )


# ---------------------------------------------------------------------------
# Overlay functions — one per slide type
# ---------------------------------------------------------------------------

def add_hook_overlay(image: Image.Image, slide_text: HookSlideText) -> Image.Image:
    """Large centered text overlay for the opening hook slide."""
    img = image.copy()
    draw = ImageDraw.Draw(img)

    font_size = max(1, round(img.width * FONT_SIZE_RATIO))
    stroke_width = max(1, round(font_size * STROKE_WIDTH_RATIO))
    max_width = int(img.width * MAX_TEXT_WIDTH_RATIO)
    line_height = font_size * LINE_HEIGHT_RATIO

    font = load_font(font_size)
    lines = wrap_text(draw, slide_text.text, font, max_width)

    total_height = len(lines) * line_height
    start_y = (img.height * TEXT_Y_POSITION_RATIO) - (total_height / 2) + (line_height / 2)
    y = _safe_y(start_y, total_height, img.height)
    x = img.width / 2

    for line in lines:
        _draw_text_with_stroke(draw, (x, y), line, font, stroke_width, anchor="mt")
        y += line_height

    return img


def add_location_overlay(
    image: Image.Image, slide_text: LocationSlideText
) -> Image.Image:
    """Place name + neighborhood + slide number overlay."""
    img = image.copy()
    draw = ImageDraw.Draw(img)

    # Fonts
    name_font_size = max(1, round(img.width * FONT_SIZE_RATIO))
    neighborhood_font_size = max(1, round(img.width * NEIGHBORHOOD_FONT_RATIO))
    number_font_size = max(1, round(img.width * SLIDE_NUMBER_FONT_RATIO))

    stroke_name = max(1, round(name_font_size * STROKE_WIDTH_RATIO))
    stroke_neigh = max(1, round(neighborhood_font_size * STROKE_WIDTH_RATIO))
    stroke_num = max(1, round(number_font_size * STROKE_WIDTH_RATIO))

    name_font = load_font(name_font_size)
    neigh_font = load_font(neighborhood_font_size)
    number_font = load_font(number_font_size)

    max_width = int(img.width * MAX_TEXT_WIDTH_RATIO)
    line_height_name = name_font_size * LINE_HEIGHT_RATIO
    line_height_neigh = neighborhood_font_size * LINE_HEIGHT_RATIO

    # Wrap name text
    name_lines = wrap_text(draw, slide_text.name, name_font, max_width)

    # Calculate total block height (name lines + gap + neighborhood)
    gap = name_font_size * 0.4
    total_height = (
        len(name_lines) * line_height_name
        + gap
        + line_height_neigh
    )

    start_y = (img.height * TEXT_Y_POSITION_RATIO) - (total_height / 2) + (line_height_name / 2)
    y = _safe_y(start_y, total_height, img.height)
    x = img.width / 2

    # Draw place name
    for line in name_lines:
        _draw_text_with_stroke(draw, (x, y), line, name_font, stroke_name, anchor="mt")
        y += line_height_name

    # Draw neighborhood below name
    y += gap
    if slide_text.neighborhood:
        _draw_text_with_stroke(
            draw, (x, y), slide_text.neighborhood, neigh_font, stroke_neigh, anchor="mt"
        )

    # Draw slide number in top-left (within safe zone)
    if slide_text.number:
        num_x = img.width * 0.05
        num_y = img.height * 0.12
        _draw_text_with_stroke(
            draw, (num_x, num_y), slide_text.number, number_font, stroke_num, anchor="lt"
        )

    return img


def add_cta_overlay(image: Image.Image, slide_text: CTASlideText) -> Image.Image:
    """Minimal call-to-action overlay (or none if text is empty)."""
    if not slide_text.text:
        return image.copy()

    img = image.copy()
    draw = ImageDraw.Draw(img)

    font_size = max(1, round(img.width * NEIGHBORHOOD_FONT_RATIO))
    stroke_width = max(1, round(font_size * STROKE_WIDTH_RATIO))
    font = load_font(font_size)
    max_width = int(img.width * MAX_TEXT_WIDTH_RATIO)
    line_height = font_size * LINE_HEIGHT_RATIO

    lines = wrap_text(draw, slide_text.text, font, max_width)
    total_height = len(lines) * line_height

    # Position above the bottom safe zone
    y = int(img.height * (1 - SAFE_ZONE_BOTTOM_RATIO) - total_height)
    x = img.width / 2

    for line in lines:
        _draw_text_with_stroke(draw, (x, y), line, font, stroke_width, anchor="mt")
        y += line_height

    return img


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

_OVERLAY_DISPATCH = {
    "hook": add_hook_overlay,
    "location": add_location_overlay,
    "cta": add_cta_overlay,
}


def add_overlays(output_dir: str | Path) -> int:
    """Apply text overlays to all raw slides in *output_dir*.

    Reads ``texts.json`` from *output_dir*, finds ``slide_{N}_raw.png`` for
    each slide, applies the appropriate overlay, and saves ``slide_{N}.png``.

    Returns the number of overlays applied.  Skips slides whose output
    already exists (idempotent).
    """
    output_dir = Path(output_dir)
    texts_path = output_dir / "texts.json"
    slides = from_texts_json(texts_path)

    applied = 0
    for idx, slide_text in enumerate(slides, start=1):
        out_path = output_dir / f"slide_{idx}.png"
        if out_path.exists():
            logger.info("Skipping slide %d — %s already exists", idx, out_path.name)
            continue

        raw_path = output_dir / f"slide_{idx}_raw.png"
        if not raw_path.exists():
            logger.warning(
                "Missing raw file for slide %d: %s — skipping", idx, raw_path.name
            )
            continue

        image = Image.open(raw_path)
        overlay_fn = _OVERLAY_DISPATCH.get(slide_text.type)
        if overlay_fn is None:
            logger.warning("Unknown slide type %r for slide %d — skipping", slide_text.type, idx)
            continue

        result = overlay_fn(image, slide_text)
        result.save(out_path, format="PNG")
        logger.info("Saved overlay slide %d → %s", idx, out_path.name)
        applied += 1

    return applied
