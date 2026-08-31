"""Step 5 — Classify places as tourist traps using an LLM."""

import logging
import math
import re
import sqlite3

import config

from . import db
from .llm import LLMError, call_llm_json

log = logging.getLogger(__name__)

_TRUTHY = frozenset({"true", "1", "yes"})
_FALSY = frozenset({"false", "0", "no"})

# Off-city location patterns that should be filtered out
OFF_CITY_PATTERNS = [
    r"\bAachen\b",
    r"\bTucson\b",
    r"\bLittle Tokyo\b",
    r"\bUnited States\b",
    r"\bUSA\b",
    r"\bU\.S\.A\b",
    r"\bGermany\b",
    r"\bArizona\b",
    r"\bAZ\b",
]

# Generic city+cuisine patterns (case-insensitive)
GENERIC_CUISINE_TERMS = [
    "ramen",
    "sushi",
    "cafe",
    "coffee",
    "bakery",
    "izakaya",
    "restaurant",
    "bar",
    "food",
]


def _normalize_bool(value: object) -> bool:
    """Coerce booleans, ints, and common string representations to bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in _TRUTHY:
            return True
        if lower in _FALSY:
            return False
    return False


def _is_generic_city_cuisine(name: str, city_name: str) -> bool:
    """Return True if the name is a generic city + cuisine pattern.
    
    Examples: "Tokyo Sushi", "Tokyo Ramen", "Tokyo Cafe"
    """
    name_lower = name.lower()
    city_lower = city_name.lower()
    
    # Check if name starts with city name
    if not name_lower.startswith(city_lower):
        return False
    
    # Remove city name and check if remainder is a generic cuisine term
    remainder = name_lower[len(city_lower):].strip()
    return remainder in GENERIC_CUISINE_TERMS


def _has_off_city_location(caption: str) -> bool:
    """Return True if the caption contains an off-city location tag.
    
    Examples: "📍 Location tag: Tucson", "📍 Location tag: Aachen, Germany"
    """
    if not caption:
        return False
    
    for pattern in OFF_CITY_PATTERNS:
        if re.search(pattern, caption, re.IGNORECASE):
            return True
    
    return False


def filter_generic_and_off_city(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
) -> int:
    """Filter out generic city+cuisine names and places with off-city location tags.
    
    Returns the number of places marked as hidden.
    """
    places = db.get_all_places(conn, city_id)
    if not places:
        log.info("No places to filter for generic names in %s", city_name)
        return 0
    
    hidden_count = 0
    
    for place in places:
        should_hide = False
        reason = ""
        
        # Check for generic city+cuisine pattern
        if _is_generic_city_cuisine(place["name"], city_name):
            should_hide = True
            reason = f"Generic city+cuisine: {place['name']}"
        
        # Check for off-city location tags in linked posts
        if not should_hide:
            # Get sample caption or check linked posts
            posts = conn.execute(
                """SELECT rp.caption 
                   FROM place_posts pp 
                   JOIN raw_posts rp ON pp.post_id = rp.id 
                   WHERE pp.place_id = ?
                   LIMIT 5""",
                (place["id"],),
            ).fetchall()
            
            for post in posts:
                if _has_off_city_location(post["caption"]):
                    should_hide = True
                    reason = f"Off-city location tag in caption"
                    break
        
        if should_hide:
            # Mark as hidden by setting is_tourist_trap=TRUE (reusing existing flag)
            db.update_tourist_trap(conn, place["id"], True)
            hidden_count += 1
            log.debug("  Filtered: %s — %s", place["name"], reason)
    
    if hidden_count:
        conn.commit()
        log.info(
            "Filtered %d generic/off-city places for %s",
            hidden_count,
            city_name,
        )
    
    return hidden_count


PROMPT_TEMPLATE = """\
You are a travel expert who knows the difference between tourist traps and
genuinely interesting places in {city_name}.

Review this list of places and mark each as a tourist trap or not.
A tourist trap is a place that:
- Appears in every single guidebook and top-10 list
- Is primarily visited by tourists, not locals
- Is famous for being famous rather than being genuinely great
- Charges tourist-inflated prices for mediocre quality

A place is NOT a tourist trap if:
- It's popular but genuinely beloved by locals too
- It's a hidden gem that went viral on social media
- It's a newer/trendy spot that hasn't been overexposed yet

Places:
{numbered_place_list}

Return ONLY a JSON object with a "results" key containing an array of objects:
{{"results": [{{"index": 0, "is_tourist_trap": true, "reason": "brief reason"}}]}}
"""


def _build_place_list(places: list[sqlite3.Row]) -> str:
    """Format places as a numbered list for the LLM prompt."""
    lines: list[str] = []
    for i, place in enumerate(places):
        lines.append(f"{i}. {place['name']} ({place['type']})")
    return "\n".join(lines)


def filter_tourist_traps(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
) -> None:
    """Classify every place for a city as a tourist trap or not."""
    places = db.get_all_places(conn, city_id)
    if not places:
        log.info("No places to filter for %s", city_name)
        return

    batch_size = config.FILTER_BATCH_SIZE
    total_batches = math.ceil(len(places) / batch_size)

    log.info(
        "Filtering %d places for %s in %d batch(es)",
        len(places),
        city_name,
        total_batches,
    )

    for batch_num in range(total_batches):
        start = batch_num * batch_size
        end = start + batch_size
        batch = places[start:end]

        log.info(
            "Filtering batch %d/%d (%d places)...",
            batch_num + 1,
            total_batches,
            len(batch),
        )

        numbered_place_list = _build_place_list(batch)
        prompt = PROMPT_TEMPLATE.format(
            city_name=city_name,
            numbered_place_list=numbered_place_list,
        )

        try:
            response = call_llm_json(prompt, temperature=0.3)
        except LLMError:
            log.exception("LLM call failed for batch %d/%d", batch_num + 1, total_batches)
            continue

        results = response.get("results", []) if isinstance(response, dict) else []

        # Build a lookup from index to is_tourist_trap
        trap_lookup: dict[int, bool] = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            idx = item.get("index")
            if not isinstance(idx, int):
                continue
            trap_lookup[idx] = _normalize_bool(item.get("is_tourist_trap", False))

        for i, place in enumerate(batch):
            if i not in trap_lookup:
                continue
            is_trap = trap_lookup[i]
            if bool(place["is_tourist_trap"]) == is_trap:
                continue
            db.update_tourist_trap(conn, place["id"], is_trap)
            if is_trap:
                reason = ""
                for item in results:
                    if isinstance(item, dict) and item.get("index") == i:
                        reason = item.get("reason", "")
                        break
                log.debug("  Tourist trap: %s — %s", place["name"], reason)

        conn.commit()
        log.info("Batch %d/%d committed", batch_num + 1, total_batches)

    trap_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM places WHERE city_id = ? AND is_tourist_trap = TRUE",
        (city_id,),
    ).fetchone()["cnt"]
    log.info(
        "Filtering complete for %s: %d/%d places marked as tourist traps",
        city_name,
        trap_count,
        len(places),
    )
