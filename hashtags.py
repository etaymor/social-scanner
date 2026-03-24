"""Step 1 – Hashtag generation for the Atlasi Place Discovery Pipeline.

Generates city-specific hashtags via an LLM and merges them with a
hardcoded set of universal tags.  Results are deduplicated (case-
insensitive) and persisted to SQLite.
"""

import logging
import sqlite3

from db import insert_hashtags
from llm import call_llm_json

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


def generate_hashtags(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
) -> list[str]:
    """Generate hashtags for *city_name*, store them, and return the unique list."""

    log.info("Generating hashtags for %s (city_id=%d)", city_name, city_id)

    # --- LLM-generated hashtags -------------------------------------------
    prompt = PROMPT_TEMPLATE.format(city_name=city_name)
    data = call_llm_json(prompt)
    raw = data.get("hashtags") if isinstance(data, dict) else None
    llm_tags: list[str] = list(raw) if isinstance(raw, list) else []
    log.info("LLM returned %d hashtags", len(llm_tags))

    # --- Hardcoded universal hashtags -------------------------------------
    universal = _universal_hashtags(city_name)

    # --- Merge & deduplicate (case-insensitive) ---------------------------
    seen: set[str] = set()
    unique_tags: list[str] = []
    for item in llm_tags + universal:
        if item is None:
            continue
        tag = str(item).strip()
        if not tag:
            continue
        key = tag.lower()
        if key not in seen:
            seen.add(key)
            unique_tags.append(tag)

    log.info("Total unique hashtags: %d", len(unique_tags))
    log.debug("Hashtags: %s", unique_tags)

    # --- Persist ----------------------------------------------------------
    insert_hashtags(conn, city_id, unique_tags)
    log.info("Hashtags stored for city_id=%d", city_id)

    return unique_tags
