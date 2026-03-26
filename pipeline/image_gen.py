"""Image generation via OpenRouter + Gemini Flash for slideshow slides."""

from __future__ import annotations

import base64
import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from pipeline.image_styles import SlideshowStyle

from config import (
    GEMINI_MAX_RETRIES,
    GEMINI_MODEL,
    GEMINI_TIMEOUT,
    GOOGLE_PLACES_API_KEY,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
)
from pipeline.retry import retry_with_backoff

log = logging.getLogger(__name__)

_RETRY_BASE_DELAY = 2  # seconds
_MAX_IMAGE_SIZE = 50 * 1024 * 1024  # 50 MB


# ---------------------------------------------------------------------------
# Error hierarchy (mirrors llm.py pattern)
# ---------------------------------------------------------------------------


class GeminiError(Exception):
    """Retryable image generation error."""

    pass


class GeminiQuotaError(GeminiError):
    """Non-retryable — credits/quota exhausted."""

    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _slugify(name: str) -> str:
    """Convert a place name to a filesystem-safe slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    return slug


def _encode_image_file(path: Path) -> str:
    """Read an image file and return its base64-encoded data URI."""
    data = path.read_bytes()
    b64 = base64.b64encode(data).decode()
    return f"data:image/png;base64,{b64}"


# ---------------------------------------------------------------------------
# Core generation function
# ---------------------------------------------------------------------------


def generate_image(
    prompt: str,
    output_path: Path,
    *,
    reference_images: list[Path] | None = None,
    system_prompt: str | None = None,
) -> bool:
    """Call OpenRouter with the Gemini model to generate one image.

    Args:
        prompt: Text prompt for image generation.
        output_path: Path where the generated PNG will be saved.
        reference_images: Optional list of image file paths to include as
            visual context (base64-encoded in the request).
        system_prompt: Optional system-level instruction prepended to the
            conversation to guide the model's overall behaviour.

    Returns:
        True on success.

    Raises:
        GeminiQuotaError: On HTTP 402 (credits exhausted) — non-retryable.
        GeminiError: On content filtering (blocked/empty images) or after
            exhausting all retry attempts on transient errors.
    """
    if not OPENROUTER_API_KEY:
        raise GeminiError("OPENROUTER_API_KEY not set in environment")

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    # Build message list
    messages: list[dict] = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})

    if reference_images:
        content_parts: list[dict] = []
        for img_path in reference_images:
            content_parts.append(
                {
                    "type": "image_url",
                    "image_url": {"url": _encode_image_file(Path(img_path))},
                }
            )
        content_parts.append({"type": "text", "text": prompt})
        messages.append({"role": "user", "content": content_parts})
    else:
        messages.append({"role": "user", "content": prompt})

    payload = {
        "model": GEMINI_MODEL,
        "messages": messages,
        "modalities": ["image", "text"],
        "image_config": {
            "aspect_ratio": "9:16",
            "image_size": "2K",
        },
    }

    def _do_generate():
        resp = requests.post(
            OPENROUTER_BASE_URL,
            headers=headers,
            json=payload,
            timeout=GEMINI_TIMEOUT,
        )

        if resp.status_code == 402:
            raise GeminiQuotaError(
                "OpenRouter credits exhausted (HTTP 402). Add credits and retry."
            )

        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            raise GeminiError(
                f"Non-retryable client error (HTTP {resp.status_code}): {resp.text[:200]}"
            )

        resp.raise_for_status()
        data = resp.json()

        images = data.get("choices", [{}])[0].get("message", {}).get("images", [])

        if not images:
            raise GeminiError("Content filtered or blocked: no images in response")

        image_url = images[0].get("image_url", {}).get("url", "")
        if not image_url:
            raise GeminiError("Content filtered or blocked: empty image URL in response")

        prefix = "data:image/png;base64,"
        if image_url.startswith(prefix):
            b64_data = image_url[len(prefix) :]
        elif ";base64," in image_url:
            b64_data = image_url.split(";base64,", 1)[1]
        else:
            b64_data = image_url

        image_bytes = base64.b64decode(b64_data)

        if len(image_bytes) > _MAX_IMAGE_SIZE:
            raise GeminiError(f"Decoded image too large: {len(image_bytes)} bytes")

        is_png = image_bytes[:8] == b"\x89PNG\r\n\x1a\n"
        is_jpeg = image_bytes[:2] == b"\xff\xd8"
        is_webp = image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP"

        if not (is_png or is_jpeg or is_webp):
            raise GeminiError("Decoded data is not a valid image (not PNG, JPEG, or WebP)")

        # Convert JPEG/WebP to PNG so downstream pipeline always gets PNG
        if not is_png:
            from io import BytesIO

            from PIL import Image as PILImage

            buf = BytesIO(image_bytes)
            img = PILImage.open(buf)
            png_buf = BytesIO()
            img.save(png_buf, format="PNG")
            image_bytes = png_buf.getvalue()

        dest = Path(output_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(image_bytes)

        log.info("Image saved to %s (%d bytes)", dest, len(image_bytes))
        return True

    try:
        return retry_with_backoff(
            _do_generate,
            max_retries=max(GEMINI_MAX_RETRIES, 3),
            base_delay=_RETRY_BASE_DELAY,
            non_retryable=(GeminiQuotaError,),
        )
    except (GeminiQuotaError, GeminiError):
        raise
    except Exception as e:
        raise GeminiError(f"Image generation failed after retries: {e}") from e


# ---------------------------------------------------------------------------
# Slideshow orchestrator
# ---------------------------------------------------------------------------

def build_cta_image(
    city_name: str,
    place_names: list[str],
    output_path: Path,
    hook_image_path: Path | None = None,
) -> bool:
    """Build the CTA slide programmatically — no AI generation.

    Creates a clean Atlasi app "Save Place" UI mockup with the actual
    place names rendered as crisp text using Pillow.
    """
    from PIL import Image as PILImage, ImageDraw as PILDraw, ImageFont as PILFont

    W, H = 1080, 1920

    # Colors matching Atlasi brand
    bg_color = (255, 251, 243)
    text_dark = (35, 35, 35)
    text_gray = (120, 120, 120)
    accent_gold = (232, 185, 56)
    divider_color = (230, 225, 215)

    img = PILImage.new("RGB", (W, H), bg_color)
    draw = PILDraw.Draw(img)

    def _font(size: int) -> PILFont.FreeTypeFont | PILFont.ImageFont:
        for p in [
            "/System/Library/Fonts/Helvetica.ttc",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ]:
            try:
                return PILFont.truetype(p, size)
            except OSError:
                continue
        return PILFont.load_default()

    def _font_bold(size: int) -> PILFont.FreeTypeFont | PILFont.ImageFont:
        for p in [
            "/System/Library/Fonts/Helvetica.ttc",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ]:
            try:
                return PILFont.truetype(p, size)
            except OSError:
                continue
        return PILFont.load_default()

    title_font = _font_bold(42)
    place_font = _font_bold(32)
    subtitle_font = _font(24)
    section_font = _font_bold(22)
    button_font = _font_bold(34)
    caption_font = _font(22)
    margin = 40

    y = 60

    # Header
    draw.text((W // 2, y), "Save Place", font=title_font, fill=text_dark, anchor="mt")
    draw.text((W - 60, y + 5), "✕", font=_font(36), fill=text_gray, anchor="mt")
    y += 70

    # TikTok thumbnail
    thumb_h = 300
    if hook_image_path and hook_image_path.exists():
        try:
            with PILImage.open(hook_image_path) as hook_img:
                hook_w, hook_h = hook_img.size
                target_ratio = (W - margin * 2) / thumb_h
                current_ratio = hook_w / hook_h
                if current_ratio > target_ratio:
                    new_w = int(hook_h * target_ratio)
                    left = (hook_w - new_w) // 2
                    hook_img = hook_img.crop((left, 0, left + new_w, hook_h))
                else:
                    new_h = int(hook_w / target_ratio)
                    top = (hook_h - new_h) // 2
                    hook_img = hook_img.crop((0, top, hook_w, top + new_h))
                hook_img = hook_img.resize((W - margin * 2, thumb_h))
                img.paste(hook_img, (margin, y))
        except Exception:
            draw.rectangle([margin, y, W - margin, y + thumb_h], fill=(60, 60, 60))
    else:
        draw.rectangle([margin, y, W - margin, y + thumb_h], fill=(60, 60, 60))

    # TikTok badge
    draw.rounded_rectangle([margin + 10, y + 10, margin + 120, y + 42], radius=6, fill=(0, 0, 0))
    draw.text((margin + 65, y + 26), "TikTok", font=_font_bold(18), fill="white", anchor="mm")
    y += thumb_h + 20

    # Caption
    draw.text((margin, y), f"Which one would you do first in {city_name}?", font=caption_font, fill=text_dark)
    y += 35
    draw.text((margin, y), "Show more ▾", font=_font(20), fill=(70, 130, 180))
    y += 45
    draw.line([(margin, y), (W - margin, y)], fill=divider_color, width=1)
    y += 20

    # Section header
    draw.text((margin, y), "SELECT PLACES", font=section_font, fill=text_gray)
    draw.text((W - margin, y), "Clear all", font=_font(22), fill=(70, 130, 180), anchor="rt")
    y += 50

    # Place list
    for i, name in enumerate(place_names[:8]):
        pin_cx, pin_cy = margin + 18, y + 22
        draw.ellipse([pin_cx - 14, pin_cy - 14, pin_cx + 14, pin_cy + 14], fill=accent_gold)
        draw.ellipse([pin_cx - 4, pin_cy - 4, pin_cx + 4, pin_cy + 4], fill="white")

        text_x = margin + 50
        draw.text((text_x, y + 5), name, font=place_font, fill=text_dark)
        draw.text((text_x, y + 42), city_name, font=subtitle_font, fill=text_gray)

        y += 80
        if i < len(place_names) - 1:
            draw.line([(text_x, y - 5), (W - margin, y - 5)], fill=divider_color, width=1)

    # Save button
    btn_h = 65
    btn_y = H - 100 - btn_h
    draw.rounded_rectangle([50, btn_y, W - 50, btn_y + btn_h], radius=32, fill=accent_gold)
    draw.text(
        (W // 2, btn_y + btn_h // 2),
        f"Save {len(place_names)} Places",
        font=button_font, fill=text_dark, anchor="mm",
    )

    dest = Path(output_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    img.save(dest, format="PNG")
    log.info("CTA image built programmatically: %s", dest)
    return True


def generate_slideshow_images(
    output_dir: Path,
    places: list[dict],
    hook_image_prompt: str,
    cta_template_path: Path | None = None,
    style: SlideshowStyle | None = None,
    city: str = "default",
    date_str: str | None = None,
) -> dict:
    """Generate all slideshow images: hook + location slides + CTA.

    Args:
        output_dir: Directory to save generated images.
        places: List of place dicts, each with at least ``name`` and
            ``image_prompt`` fields.
        hook_image_prompt: Image generation prompt for the hook slide.
        cta_template_path: Optional path to a CTA template image to use
            as visual reference when generating the CTA slide.
        style: Visual style dict from :func:`select_slideshow_style`.
            When *None* a default safe style is used.
        city: City name used to seed deterministic perspective rotation.
        date_str: Date string (YYYY-MM-DD) used to seed perspective
            rotation.  Defaults to today when *None*.

    Returns:
        Dict with keys ``generated``, ``skipped``, ``failed``, and
        ``failed_slides`` (list of slide numbers that failed).
    """
    from pipeline.image_styles import (
        IMAGE_SYSTEM_PROMPT,
        build_hook_style_block,
        build_location_style_suffix,
        get_perspectives_for_slides,
        select_slideshow_style,
    )

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    from datetime import datetime

    # Use provided style or fall back to a default
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    if style is None:
        style = select_slideshow_style(city, date_str)

    n_places = len(places)
    prompts: dict[str, str] = {}
    generated = 0
    skipped = 0
    failed = 0
    failed_slides: list[int] = []

    # Pre-compute per-slide perspectives for location variety
    perspectives = get_perspectives_for_slides(city, date_str, n_places)

    # ------------------------------------------------------------------
    # Slide 1: Hook
    # ------------------------------------------------------------------
    hook_path = output_dir / "slide_1_hook_raw.png"
    hook_style_block = build_hook_style_block(style)
    full_hook_prompt = f"{hook_image_prompt}. {hook_style_block}"
    prompts["slide_1_hook"] = full_hook_prompt

    try:
        if _should_skip(hook_path):
            log.info("Slide 1 (hook): skipped (exists)")
            skipped += 1
        else:
            log.info("Slide 1 (hook): generating...")
            generate_image(
                full_hook_prompt, hook_path, system_prompt=IMAGE_SYSTEM_PROMPT
            )
            generated += 1
    except GeminiQuotaError:
        raise
    except GeminiError as e:
        log.error("Slide 1 (hook) failed: %s", e)
        failed += 1
        failed_slides.append(1)

    # ------------------------------------------------------------------
    # Slides 2 .. N+1: Locations
    # ------------------------------------------------------------------
    for i, place in enumerate(places):
        slide_num = i + 2
        place_name = place.get("name", f"place_{i}")
        slide_path = output_dir / f"slide_{slide_num}_raw.png"

        if _should_skip(slide_path):
            log.info("Slide %d (%s): skipped (exists)", slide_num, place_name)
            skipped += 1
            continue

        # Try real photo first (Google Places API)
        photo_found = False
        if GOOGLE_PLACES_API_KEY:
            try:
                from pipeline.photo_search import search_place_photo

                photo_found = search_place_photo(
                    place_name=place_name,
                    city=city,
                    output_path=slide_path,
                )
                if photo_found:
                    log.info("Slide %d (%s): real photo sourced", slide_num, place_name)
                    prompts[f"slide_{slide_num}_{_slugify(place_name)}"] = "(google_places_photo)"
                    generated += 1
            except Exception as e:
                log.warning(
                    "Slide %d (%s): photo search failed (%s), falling back to AI",
                    slide_num, place_name, e,
                )

        # Fall back to AI generation
        if not photo_found:
            raw_prompt = place.get("image_prompt", "")
            location_suffix = build_location_style_suffix(
                style, perspective_override=perspectives[i]
            )
            full_prompt = f"{raw_prompt}. {location_suffix}"
            prompts[f"slide_{slide_num}_{_slugify(place_name)}"] = full_prompt

            try:
                log.info("Slide %d (%s): generating AI image...", slide_num, place_name)
                generate_image(
                    full_prompt, slide_path, system_prompt=IMAGE_SYSTEM_PROMPT
                )
                generated += 1
            except GeminiQuotaError:
                raise
            except GeminiError as e:
                log.error("Slide %d (%s) failed: %s", slide_num, place_name, e)
                failed += 1
                failed_slides.append(slide_num)

    # ------------------------------------------------------------------
    # Slide N+2: CTA (built programmatically, not AI-generated)
    # ------------------------------------------------------------------
    cta_slide_num = n_places + 2
    cta_path = output_dir / f"slide_{cta_slide_num}_cta_raw.png"
    place_names_list = [p.get("name", "") for p in places if p.get("name")]
    prompts[f"slide_{cta_slide_num}_cta"] = "(programmatic — Atlasi ingest UI mockup)"

    # Use the hook image as the TikTok thumbnail in the CTA
    hook_raw = output_dir / "slide_1_hook_raw.png"
    if not hook_raw.exists():
        hook_raw = output_dir / "slide_1_raw.png"

    try:
        if _should_skip(cta_path):
            log.info("Slide %d (CTA): skipped (exists)", cta_slide_num)
            skipped += 1
        else:
            log.info("Slide %d (CTA): building programmatically...", cta_slide_num)
            build_cta_image(
                city_name=city,
                place_names=place_names_list,
                output_path=cta_path,
                hook_image_path=hook_raw if hook_raw.exists() else None,
            )
            generated += 1
    except Exception as e:
        log.error("Slide %d (CTA) failed: %s", cta_slide_num, e)
        failed += 1
        failed_slides.append(cta_slide_num)

    # ------------------------------------------------------------------
    # Save prompts.json for debugging
    # ------------------------------------------------------------------
    prompts_path = output_dir / "prompts.json"
    prompts_path.write_text(
        json.dumps(prompts, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Prompts saved to %s", prompts_path)

    result = {
        "generated": generated,
        "skipped": skipped,
        "failed": failed,
        "failed_slides": failed_slides,
    }
    log.info(
        "Image generation complete: %d generated, %d skipped, %d failed",
        generated,
        skipped,
        failed,
    )
    return result


# ---------------------------------------------------------------------------
# Resume helpers
# ---------------------------------------------------------------------------

_MIN_FILE_SIZE = 10 * 1024  # 10 KB


def _should_skip(slide_path: Path) -> bool:
    """Return True if the slide image already exists and is large enough."""
    return slide_path.exists() and slide_path.stat().st_size > _MIN_FILE_SIZE
