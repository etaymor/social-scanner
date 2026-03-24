"""Step 1 – Hashtag generation for the Atlasi Place Discovery Pipeline.

Generates city-specific hashtags via an LLM and merges them with a
hardcoded set of universal tags.  Results are deduplicated (case-
insensitive) and persisted to SQLite.
"""

import logging
import sqlite3

import config
from .db import insert_hashtags
from .llm import call_llm_json, LLMError

log = logging.getLogger(__name__)

PROMPT_TEMPLATE = """\
You are a social media research assistant. Given a city name, generate 15 hashtags
that travelers and locals use on TikTok and Instagram to share NON-OBVIOUS,
local-favorite, hidden-gem places. Avoid generic tourism hashtags.

Focus on these categories:
- Hidden gems / secret spots
- Local favorites
- Food and drink spots locals love
- Underrated neighborhoods
- Nightlife that isn't in guidebooks
- Cool viewpoints or photo spots

City: {city_name}

Return ONLY a JSON object with a "hashtags" key containing an array of hashtag strings without the # symbol.
Example: {{"hashtags": ["istanbulhiddengems", "istanbullocals", "istanbulfoodie"]}}"""

CATEGORY_PROMPT_TEMPLATE = """\
You are a social media research assistant. Given a city name and a category,
generate {count} hashtags that travelers and locals use on TikTok and Instagram
to share NON-OBVIOUS, local-favorite places in that specific category.
Avoid generic tourism hashtags.

City: {city_name}
Category: {category_label} — {category_description}

Return ONLY a JSON object with a "hashtags" key containing an array of hashtag strings without the # symbol.
Example: {{"hashtags": ["istanbulhiddenbars", "istanbulnightlife", "istanbulclubs"]}}"""


def _universal_hashtags(city_name: str) -> list[str]:
    """Return five hardcoded universal hashtags for *city_name*."""
    city = city_name.lower().replace(" ", "")
    return [
        f"{city}hiddengems",
        f"{city}locals",
        f"{city}secretspots",
        f"{city}underrated",
        f"{city}foodie",
    ]


def _category_seed_hashtags(city_name: str, category: str) -> list[str]:
    """Return category-specific seed hashtags for *city_name*."""
    city = city_name.lower().replace(" ", "")
    seeds = config.CATEGORY_HASHTAG_SEEDS.get(category, {})
    tags: list[str] = []
    for suffix in seeds.get("suffixes", []):
        tags.append(f"{city}{suffix}")
    tags.extend(seeds.get("tags", []))
    return tags


def _call_llm_for_tags(prompt: str) -> list[str]:
    """Call the LLM and extract hashtags from the response."""
    try:
        data = call_llm_json(prompt)
        raw = data.get("hashtags") if isinstance(data, dict) else None
        return list(raw) if isinstance(raw, list) else []
    except LLMError:
        log.exception("LLM call failed for hashtag generation")
        return []


def _deduplicate(tags: list[str]) -> list[str]:
    """Case-insensitive deduplication preserving first occurrence."""
    seen: set[str] = set()
    unique: list[str] = []
    for item in tags:
        if item is None:
            continue
        tag = str(item).strip()
        if not tag:
            continue
        key = tag.lower()
        if key not in seen:
            seen.add(key)
            unique.append(tag)
    return unique


def generate_hashtags(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    category: str | None = None,
) -> list[str]:
    """Generate hashtags for *city_name*, store them, and return the unique list.

    When *category* is provided, generates ~15 category-specific LLM hashtags
    (single call) + category seed tags + 5 universal hardcoded tags.
    When omitted, uses the original 15-tag generic prompt + 5 universal tags.
    """
    log.info(
        "Generating hashtags for %s (city_id=%d, category=%s)",
        city_name, city_id, category or "generic",
    )

    if category and category in config.VALID_CATEGORIES:
        cat_info = config.CATEGORIES[category]

        # ~15 category-specific LLM hashtags (single call)
        cat_prompt = CATEGORY_PROMPT_TEMPLATE.format(
            count=15,
            city_name=city_name,
            category_label=cat_info["label"],
            category_description=cat_info["description"],
        )
        cat_tags = _call_llm_for_tags(cat_prompt)
        log.info("LLM returned %d category-specific hashtags", len(cat_tags))

        # Category-specific seed hashtags
        seed_tags = _category_seed_hashtags(city_name, category)

        # Universal hardcoded
        universal = _universal_hashtags(city_name)

        all_tags = cat_tags + seed_tags + universal
    else:
        # Original behavior: 15 generic LLM hashtags + 5 universal
        prompt = PROMPT_TEMPLATE.format(city_name=city_name)
        llm_tags = _call_llm_for_tags(prompt)
        log.info("LLM returned %d hashtags", len(llm_tags))

        universal = _universal_hashtags(city_name)
        all_tags = llm_tags + universal

    unique_tags = _deduplicate(all_tags)

    log.info("Total unique hashtags: %d", len(unique_tags))
    log.debug("Hashtags: %s", unique_tags)

    # --- Persist ----------------------------------------------------------
    insert_hashtags(conn, city_id, unique_tags, category=category)
    log.info("Hashtags stored for city_id=%d", city_id)

    return unique_tags
