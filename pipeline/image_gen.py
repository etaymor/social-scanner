"""Image generation via OpenRouter + Gemini Flash for slideshow slides."""

import base64
import json
import logging
import re
from pathlib import Path

import requests

from config import (
    GEMINI_MAX_RETRIES,
    GEMINI_MODEL,
    GEMINI_TIMEOUT,
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
) -> bool:
    """Call OpenRouter with the Gemini model to generate one image.

    Args:
        prompt: Text prompt for image generation.
        output_path: Path where the generated PNG will be saved.
        reference_images: Optional list of image file paths to include as
            visual context (base64-encoded in the request).

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

    # Build message content
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
        messages = [{"role": "user", "content": content_parts}]
    else:
        messages = [{"role": "user", "content": prompt}]

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
        if not image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
            raise GeminiError("Decoded data is not a valid PNG image")

        dest = Path(output_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(image_bytes)

        log.info("Image saved to %s (%d bytes)", dest, len(image_bytes))
        return True

    try:
        return retry_with_backoff(
            _do_generate,
            max_retries=GEMINI_MAX_RETRIES,
            base_delay=_RETRY_BASE_DELAY,
            non_retryable=(GeminiQuotaError, GeminiError),
        )
    except (GeminiQuotaError, GeminiError):
        raise
    except Exception as e:
        raise GeminiError(f"Image generation failed after {GEMINI_MAX_RETRIES} retries: {e}") from e


# ---------------------------------------------------------------------------
# Slideshow orchestrator
# ---------------------------------------------------------------------------

_LOCATION_STYLE_SUFFIX = (
    "Shot on iPhone 15 Pro, natural lighting, shallow depth of field, "
    "editorial travel photography, no text or watermarks, no people facing camera"
)

_CTA_PROMPT_TEMPLATE = (
    "Generate an image of a modern mobile app screen showing a 'Save Place' "
    "feature. The screen displays a list of saved places including: {place_names}. "
    "Clean UI design, rounded cards, map pin icons, warm color palette. "
    "The app name is 'Atlasi'. Mobile screenshot style, 9:16 aspect ratio."
)


def generate_slideshow_images(
    output_dir: Path,
    places: list[dict],
    hook_image_prompt: str,
    cta_template_path: Path | None = None,
) -> dict:
    """Generate all slideshow images: hook + location slides + CTA.

    Args:
        output_dir: Directory to save generated images.
        places: List of place dicts, each with at least ``name`` and
            ``image_prompt`` fields.
        hook_image_prompt: Image generation prompt for the hook slide.
        cta_template_path: Optional path to a CTA template image to use
            as visual reference when generating the CTA slide.

    Returns:
        Dict with keys ``generated``, ``skipped``, ``failed``, and
        ``failed_slides`` (list of slide numbers that failed).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    n_places = len(places)
    prompts: dict[str, str] = {}
    generated = 0
    skipped = 0
    failed = 0
    failed_slides: list[int] = []

    # ------------------------------------------------------------------
    # Slide 1: Hook
    # ------------------------------------------------------------------
    hook_path = output_dir / "slide_1_hook_raw.png"
    prompts["slide_1_hook"] = hook_image_prompt

    try:
        if _should_skip(hook_path):
            log.info("Slide 1 (hook): skipped (exists)")
            skipped += 1
        else:
            log.info("Slide 1 (hook): generating...")
            generate_image(hook_image_prompt, hook_path)
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
        raw_prompt = place.get("image_prompt", "")
        full_prompt = f"{raw_prompt}. {_LOCATION_STYLE_SUFFIX}"
        slide_path = output_dir / f"slide_{slide_num}_raw.png"
        prompts[f"slide_{slide_num}_{_slugify(place_name)}"] = full_prompt

        try:
            if _should_skip(slide_path):
                log.info("Slide %d (%s): skipped (exists)", slide_num, place_name)
                skipped += 1
            else:
                log.info("Slide %d (%s): generating...", slide_num, place_name)
                generate_image(full_prompt, slide_path)
                generated += 1
        except GeminiQuotaError:
            raise
        except GeminiError as e:
            log.error("Slide %d (%s) failed: %s", slide_num, place_name, e)
            failed += 1
            failed_slides.append(slide_num)

    # ------------------------------------------------------------------
    # Slide N+2: CTA
    # ------------------------------------------------------------------
    cta_slide_num = n_places + 2
    cta_path = output_dir / f"slide_{cta_slide_num}_cta_raw.png"
    place_names = ", ".join(p.get("name", "") for p in places)
    cta_prompt = _CTA_PROMPT_TEMPLATE.format(place_names=place_names)
    prompts[f"slide_{cta_slide_num}_cta"] = cta_prompt

    reference_images = None
    if cta_template_path and Path(cta_template_path).exists():
        reference_images = [Path(cta_template_path)]

    try:
        if _should_skip(cta_path):
            log.info("Slide %d (CTA): skipped (exists)", cta_slide_num)
            skipped += 1
        else:
            log.info("Slide %d (CTA): generating...", cta_slide_num)
            generate_image(cta_prompt, cta_path, reference_images=reference_images)
            generated += 1
    except GeminiQuotaError:
        raise
    except GeminiError as e:
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
# Resume / override helpers
# ---------------------------------------------------------------------------

_MIN_FILE_SIZE = 10 * 1024  # 10 KB


def _should_skip(slide_path: Path) -> bool:
    """Return True if the slide image already exists and is large enough."""
    return slide_path.exists() and slide_path.stat().st_size > _MIN_FILE_SIZE
