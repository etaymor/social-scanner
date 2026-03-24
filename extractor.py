"""Step 3 — Extract place names from post captions using an LLM."""

import logging
import sqlite3

import config
import db
from llm import call_llm_json, LLMError

log = logging.getLogger(__name__)

PROMPT_TEMPLATE = """\
You are extracting specific place names from social media captions about {city_name}.

For each caption, extract any SPECIFIC, NAMED places mentioned — restaurants, cafes,
bars, clubs, markets, neighborhoods, viewpoints, parks, museums, galleries, shops,
or activities.

Rules:
- Only extract places with actual names (not "this cute cafe" without a name)
- Include the neighborhood/area if mentioned alongside the place
- Classify each place by type
- Skip generic city landmarks unless the caption frames them in a non-obvious way

Captions:
{numbered_captions}

Return ONLY a JSON object with a "results" key containing an array of objects. Each object:
{{"caption_index": <int>, "places": [{{"name": "<place name>", "type": "<restaurant|cafe|bar|club|market|neighborhood|viewpoint|park|museum|gallery|shop|activity|street|other>"}}]}}

If a caption mentions no specific named place, return an empty places array for it."""


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
        lines.append(f"{idx}. {caption.strip()}")
        index_to_post[idx] = post
    return "\n".join(lines), index_to_post


def _validate_place_type(place_type: str) -> str:
    """Normalise and validate a place type, falling back to 'other'."""
    cleaned = place_type.strip().lower() if place_type else "other"
    return cleaned if cleaned in config.VALID_PLACE_TYPES else "other"


def _process_batch(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    posts: list[sqlite3.Row],
    verbose: bool = False,
) -> int:
    """Send one batch to the LLM and upsert extracted places.

    Returns the number of places extracted.
    """
    numbered_captions, index_to_post = _build_numbered_captions(posts)

    if not index_to_post:
        # Every caption in the batch was empty — nothing to send.
        return 0

    prompt = PROMPT_TEMPLATE.format(
        city_name=city_name,
        numbered_captions=numbered_captions,
    )

    response = call_llm_json(prompt, temperature=0.2)

    results = response.get("results", []) if isinstance(response, dict) else []
    places_extracted = 0

    for item in results:
        caption_index = item.get("caption_index")
        places = item.get("places", [])
        if caption_index is None or caption_index not in index_to_post:
            continue

        post = index_to_post[caption_index]
        for place in places:
            name = place.get("name", "").strip()
            if not name:
                continue
            place_type = _validate_place_type(place.get("type", "other"))
            db.upsert_place(
                conn,
                city_id,
                name,
                place_type,
                post["id"],
                (post["caption"] or "")[:500],
            )
            places_extracted += 1
            if verbose:
                log.debug("  -> %s (%s)", name, place_type)

    return places_extracted


def extract_places(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    verbose: bool = False,
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
            extracted = _process_batch(conn, city_id, city_name, posts, verbose)
            total_places += extracted
        except LLMError:
            log.exception("LLM error on batch %d — skipping batch", batch_num)

        db.mark_posts_processed(conn, post_ids)
        conn.commit()

    log.info(
        "Extraction complete: %d batch(es), %d place(s) extracted.",
        batch_num,
        total_places,
    )
    return total_places
