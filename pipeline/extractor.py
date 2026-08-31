"""Step 3 — Extract place names from post captions using an LLM."""

import logging
import sqlite3

import config

from . import db
from .listicle import extract_named_places_heuristic, is_overlay_junk
from .llm import LLMError, call_llm_json, sanitize_text

log = logging.getLogger(__name__)

# Keep listicle tails (Top 10/30 restaurants). Old 500-char cap dropped venues.
_CAPTION_MAX_CHARS = 2500

SYSTEM_PROMPT = """\
You are extracting specific place names from social media captions about {city_name}.

Rules:
- Extract ALL named places, businesses, venues, and locations mentioned in each caption
- Include places from: caption text, 📍 location tags, 🔤 on-screen text (OCR), 🎙 Subtitles, addresses, and any other location references
- Extract EVERY place mentioned, even if multiple places appear in a single caption (e.g. "Top 5 restaurants" lists)
- Include the neighborhood/area if mentioned alongside the place
- Classify each place by type
- Assign each place a category from this list: food_and_drink, places_to_stay, sights_and_attractions, nightlife, shopping, outdoors_and_nature, arts_and_culture, activities_and_experiences
- DO extract specific neighborhoods, streets, markets, and districts by name
- DO extract places even if only a business name is given (e.g. "@CafeBlue" → "Cafe Blue")
- DO extract places from numbered lists (e.g. "1. Sushi Dai 2. Ramen Street" → extract both)
- DO extract places written only in on-screen/subtitle blocks even when the main caption is generic
- Treat 🔤 on-screen text as noisy video overlay chrome, NOT as a clean caption: DO extract clear venue/business names; DO NOT extract floor/building labels (1F, 2F, 5F, B1), city-only labels ("IN SEOUL", "{city_name}"), English filler (HERE, delicious, yummy, wow), or dish/cuisine phrases without a venue name (naengmyeon, Korean beef barbecue, ramen, salt bread)
- DO NOT extract the city name itself ("{city_name}") or the country name as a place
- DO NOT extract generic city + cuisine combinations (e.g. "Tokyo Sushi", "Tokyo Ramen") unless they are clearly a specific business name with additional context
- DO NOT extract standalone neighborhood or area names from location tags unless paired with a specific business name (e.g. "Shibuya" alone is not a venue, but "Shibuya Crossing" or "Cafe in Shibuya" would be)
- Skip only truly generic references without any name (e.g. "this cute cafe", "a random bar")

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
        lines.append(f"{idx}. {sanitize_text(caption.strip(), max_length=_CAPTION_MAX_CHARS)}")
        index_to_post[idx] = post
    return "\n".join(lines), index_to_post


def _upsert_extracted(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    post: sqlite3.Row,
    places: list,
) -> int:
    """Validate and upsert a list of place dicts for one post. Returns count."""
    places_extracted = 0
    for place in places:
        if not isinstance(place, dict):
            continue
        name = place.get("name", "").strip()
        if not name:
            continue
        if name.lower() == city_name.lower():
            continue
        # OCR overlay chrome (floors, city-only, filler, dish generics) is not a venue
        if is_overlay_junk(name):
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
    return config.DEFAULT_CATEGORY


def _process_batch(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    posts: list[sqlite3.Row],
    *,
    heuristic_only: bool = False,
) -> int:
    """Send one batch to the LLM (unless heuristic_only) and upsert places.

    Always merges deterministic listicle/OCR/subtitle parses so multi-place
    slideshow captions are not lost when the model truncates.
    """
    numbered_captions, index_to_post = _build_numbered_captions(posts)

    if not index_to_post:
        return 0

    places_extracted = 0

    # Deterministic pass first (listicles + on-screen blocks)
    for post in index_to_post.values():
        heuristic = extract_named_places_heuristic(post["caption"] or "")
        places_extracted += _upsert_extracted(conn, city_id, city_name, post, heuristic)

    if heuristic_only:
        return places_extracted

    system = SYSTEM_PROMPT.format(city_name=city_name)
    user_prompt = USER_PROMPT_TEMPLATE.format(numbered_captions=numbered_captions)
    response = call_llm_json(user_prompt, system=system, temperature=0.2)
    results = response.get("results", []) if isinstance(response, dict) else []

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
        places_extracted += _upsert_extracted(conn, city_id, city_name, post, places)

    return places_extracted


def extract_places(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    *,
    heuristic_only: bool = False,
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
            extracted = _process_batch(
                conn, city_id, city_name, posts, heuristic_only=heuristic_only
            )
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
