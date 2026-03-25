"""Hook generation for slideshow formats (listicle and story)."""

import json
import logging
import random

from .llm import call_llm_json, LLMError, sanitize_text

log = logging.getLogger(__name__)

HOOK_TEMPLATES = {
    "listicle": [
        "{n} places in {city}\ntourists never find",
        "{n} hidden gems in {city}\nlocals don't share",
        "{n} spots in {city}\nyou need to visit",
        "{n} places in {city}\nthat aren't in guidebooks",
        "{n} secret spots in {city}\nonly locals know",
    ],
}

_IMAGE_PROMPT_TEMPLATE = (
    "A stunning establishing shot of {city}, shot on iPhone 15 Pro, "
    "golden hour lighting, wide-angle street-level perspective, "
    "cinematic travel photography, no text or watermarks, no people facing camera"
)

_LISTICLE_SYSTEM = """\
You are a TikTok content strategist generating hooks for travel slideshows.

You will be given a set of hook templates, a city name, a slide count, and optionally \
a category. Your job is to pick the best template for this city/category combination \
and optionally customize it to be more specific or engaging.

Rules:
- The hook_text MUST contain literal \\n line breaks so each line has 4-6 words max
- Keep it punchy — this is a TikTok hook overlay
- If a category is provided, weave it in naturally (e.g. "5 cafes in Tokyo\\nlocals don't share")
- The hook_image_prompt should describe a visually striking establishing shot of the city
- The caption should be conversational, mention "Atlasi" naturally, and end with 3-5 hashtags

Return ONLY a JSON object with these keys:
{"hook_text": "...", "hook_image_prompt": "...", "caption": "..."}"""

_STORY_SYSTEM = """\
You are a TikTok content strategist generating hooks for travel slideshows.

Generate a hook in the person+conflict travel discovery format.
Examples:
- "I showed my mom\\nwhat tourists NEVER\\nfind in Tokyo"
- "My local friend showed me\\nthe REAL side of Paris"
- "Nobody told me about\\nthese spots in Barcelona"

Rules:
- The hook_text MUST contain literal \\n line breaks so each line has 4-6 words max
- Use the person+conflict pattern adapted for travel discovery
- The hook_image_prompt should describe a visually striking establishing shot of the city
- The caption should be conversational, mention "Atlasi" naturally, and end with 3-5 hashtags

Return ONLY a JSON object with these keys:
{"hook_text": "...", "hook_image_prompt": "...", "caption": "..."}"""


def _build_listicle_prompt(city_name: str, slide_count: int, category: str | None) -> str:
    """Build the user prompt for listicle hook generation."""
    safe_city = sanitize_text(city_name, max_length=200)
    safe_category = sanitize_text(category, max_length=100) if category else None
    templates_formatted = "\n".join(
        f"  - {t.format(n=slide_count, city=safe_city)}"
        for t in HOOK_TEMPLATES["listicle"]
    )
    parts = [
        f"City: {safe_city}",
        f"Number of places: {slide_count}",
        f"Templates:\n{templates_formatted}",
    ]
    if safe_category:
        parts.append(f"Category: {safe_category}")
    parts.append(
        "Pick the best template (or customize it) for this city/category. "
        "Return the JSON object."
    )
    return "\n".join(parts)


def _build_story_prompt(city_name: str, slide_count: int, category: str | None) -> str:
    """Build the user prompt for story hook generation."""
    safe_city = sanitize_text(city_name, max_length=200)
    safe_category = sanitize_text(category, max_length=100) if category else None
    parts = [
        "Generate a TikTok hook in the person+conflict travel discovery format.",
        f"City: {safe_city}",
        f"Number of places: {slide_count}",
    ]
    if safe_category:
        parts.append(f"Category: {safe_category}")
    parts.append("Return the JSON object.")
    return "\n".join(parts)


def _fallback_listicle(city_name: str, slide_count: int, category: str | None) -> dict:
    """Produce a fallback hook dict when the LLM call fails (listicle)."""
    template = random.choice(HOOK_TEMPLATES["listicle"])
    hook_text = template.format(n=slide_count, city=city_name)
    image_prompt = _IMAGE_PROMPT_TEMPLATE.format(city=city_name)
    city_lower = city_name.lower().replace(" ", "")
    caption = (
        f"{city_name} has the BEST hidden spots — I found these gems using Atlasi "
        f"and every single one blew my mind "
        f"#{city_lower}travel #hiddengems #traveltok #atlasi #{city_lower}"
    )
    return {
        "hook_text": hook_text,
        "hook_image_prompt": image_prompt,
        "caption": caption,
    }


def _fallback_story(city_name: str, slide_count: int, category: str | None) -> dict:
    """Produce a fallback hook dict when the LLM call fails (story)."""
    hook_text = f"Nobody told me about\nthese spots in {city_name}"
    image_prompt = _IMAGE_PROMPT_TEMPLATE.format(city=city_name)
    city_lower = city_name.lower().replace(" ", "")
    caption = (
        f"I never expected to find these spots in {city_name} — "
        f"I found them all on Atlasi and they completely changed my trip "
        f"#{city_lower}travel #hiddengems #traveltok #atlasi #{city_lower}"
    )
    return {
        "hook_text": hook_text,
        "hook_image_prompt": image_prompt,
        "caption": caption,
    }


def _validate_hook_result(result: dict) -> bool:
    """Check that the LLM result has all required fields with non-empty strings."""
    required = ("hook_text", "hook_image_prompt", "caption")
    return all(isinstance(result.get(k), str) and result[k].strip() for k in required)


def generate_hook(
    city_name: str,
    slide_count: int,
    hook_format: str,
    category: str | None = None,
) -> dict:
    """Generate hook text, hook image prompt, and TikTok caption.

    Args:
        city_name: The target city.
        slide_count: Number of place slides in the slideshow.
        hook_format: Either "listicle" or "story".
        category: Optional category slug (e.g. "food_and_drink").

    Returns:
        Dict with keys: hook_text, hook_image_prompt, caption.
    """
    if hook_format == "listicle":
        system = _LISTICLE_SYSTEM
        prompt = _build_listicle_prompt(city_name, slide_count, category)
        fallback_fn = _fallback_listicle
    else:
        system = _STORY_SYSTEM
        prompt = _build_story_prompt(city_name, slide_count, category)
        fallback_fn = _fallback_story

    try:
        result = call_llm_json(prompt, system=system, temperature=0.9)
        if not isinstance(result, dict) or not _validate_hook_result(result):
            log.warning(
                "LLM returned invalid hook structure for %s (%s); using fallback",
                city_name, hook_format,
            )
            return fallback_fn(city_name, slide_count, category)
        return {
            "hook_text": result["hook_text"],
            "hook_image_prompt": result["hook_image_prompt"],
            "caption": result["caption"],
        }
    except LLMError:
        log.warning(
            "LLM call failed for hook generation (%s, %s); using fallback",
            city_name, hook_format,
            exc_info=True,
        )
        return fallback_fn(city_name, slide_count, category)
