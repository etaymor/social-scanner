"""Step 3 — Extract place names from post captions using an LLM."""

import logging
import sqlite3

import config
from . import db
from .llm import call_llm_json, LLMError, sanitize_text

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are extracting specific place names from social media captions about {city_name}.

Rules:
- Only extract places with actual names (not "this cute cafe" without a name)
- Include the neighborhood/area if mentioned alongside the place
- Classify each place by type
- Assign each place a category from this list: food_and_drink, places_to_stay, sights_and_attractions, nightlife, shopping, outdoors_and_nature, arts_and_culture, activities_and_experiences
- Skip generic city landmarks unless the caption frames them in a non-obvious way

Valid types: restaurant, cafe, bar, club, market, neighborhood, viewpoint, park, museum, gallery, shop, activity, street, hotel, hostel, tour, class, beach, temple, spa, brewery, lounge, bakery, garden, theater, monument, boutique, trail, workshop, other

Return ONLY a JSON object with a "results" key containing an array of objects. Each object:
{{"caption_index": <int>, "places": [{{"name": "<place name>", "type": "<type>", "category": "<category>"}}]}}

If a caption mentions no specific named place, return an empty places array for it."""

USER_PROMPT_TEMPLATE = """\
Extract place names from these captions:

{numbered_captions}"""


def _build_numbered_captions(posts: list[sqlite3.Row]) -> tuple[str, dict[int, sqlite3.Row]]:
    """Build a numbered caption list, skipping empty captions.

    Returns the formatted string and a mapping from 1-based index to post row.
    """
    lines: list[str] = []
    index_to_post: dict[int, sqlite3.Row] = {}
    idx = 0
    for post in posts:
        caption = post["caption"]
        if not caption or not caption.strip():
            continue
        idx += 1
        lines.append(f"{idx}. {sanitize_text(caption.strip(), max_length=500)}")
        index_to_post[idx] = post
    return "\n".join(lines), index_to_post


def _validate_place_type(place_type: str) -> str:
    """Normalise and validate a place type, falling back to 'other'."""
    cleaned = place_type.strip().lower() if place_type else "other"
    return cleaned if cleaned in config.VALID_PLACE_TYPES else "other"


def _validate_category(category: str | None, place_type: str) -> str:
    """Validate a category, falling back to TYPE_TO_CATEGORY then default."""
    if category:
        cleaned = category.strip().lower().replace(" ", "_")
        if cleaned in config.VALID_CATEGORIES:
            return cleaned
    # Fallback: derive from place type
    if place_type in config.TYPE_TO_CATEGORY:
        return config.TYPE_TO_CATEGORY[place_type]
    return "sights_and_attractions"


def _process_batch(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    posts: list[sqlite3.Row],
) -> int:
    """Send one batch to the LLM and upsert extracted places.

    Returns the number of places extracted.
    """
    numbered_captions, index_to_post = _build_numbered_captions(posts)

    if not index_to_post:
        # Every caption in the batch was empty — nothing to send.
        return 0

    system = SYSTEM_PROMPT.format(city_name=city_name)
    user_prompt = USER_PROMPT_TEMPLATE.format(numbered_captions=numbered_captions)

    response = call_llm_json(user_prompt, system=system, temperature=0.2)

    results = response.get("results", []) if isinstance(response, dict) else []
    places_extracted = 0

    for item in results:
        if not isinstance(item, dict):
            continue
        caption_index = item.get("caption_index")
        places = item.get("places", [])
        if not isinstance(places, list):
            continue
        if caption_index is None or caption_index not in index_to_post:
            continue

        post = index_to_post[caption_index]
        for place in places:
            if not isinstance(place, dict):
                continue
            name = place.get("name", "").strip()
            if not name:
                continue
            place_type = _validate_place_type(place.get("type", "other"))
            category = _validate_category(place.get("category"), place_type)
            db.upsert_place(
                conn,
                city_id,
                name,
                place_type,
                post["id"],
                (post["caption"] or "")[:500],
                category=category,
            )
            places_extracted += 1
            log.debug("  -> %s (%s, %s)", name, place_type, category)

    return places_extracted


def extract_places(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
) -> int:
    """Extract place names from all unprocessed posts for a city.

    Processes posts in batches of ``config.EXTRACTION_BATCH_SIZE``.
    Returns the total number of places extracted.
    """
    batch_size = config.EXTRACTION_BATCH_SIZE
    batch_num = 0
    total_places = 0

    while True:
        posts = db.get_unprocessed_posts(conn, city_id, batch_size)
        if not posts:
            break

        batch_num += 1
        log.info("Extracting places from batch %d (%d posts)...", batch_num, len(posts))

        post_ids = [post["id"] for post in posts]

        try:
            extracted = _process_batch(conn, city_id, city_name, posts)
            total_places += extracted
            db.mark_posts_processed(conn, post_ids)
            conn.commit()
        except LLMError:
            log.exception("LLM error on batch %d — aborting extraction", batch_num)
            break

    log.info(
        "Extraction complete: %d batch(es), %d place(s) extracted.",
        batch_num,
        total_places,
    )
    return total_places
