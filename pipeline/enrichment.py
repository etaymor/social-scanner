"""LLM enrichment for places — adds neighborhood and image prompt data."""

import logging
import math
import sqlite3

from .llm import LLMError, call_llm_json, sanitize_text

log = logging.getLogger(__name__)

ENRICHMENT_BATCH_SIZE = 10

SYSTEM_PROMPT = """\
You are a world-class travel photographer and location scout with 20 years of \
experience shooting for Conde Nast Traveler, National Geographic Traveler, and \
Monocle. You specialise in capturing the authentic character of places — not the \
postcard version, but the version a well-connected local would show you.

You help enrich place data with accurate neighborhood information and extremely \
specific visual descriptions for AI image generation. Your image prompts must read \
like shooting notes from a creative director — not generic stock photography \
descriptions."""

USER_PROMPT_TEMPLATE = """\
For each place listed below in {city_name}, provide:

1. The neighborhood or district within the city where this place is located.

2. A detailed image generation prompt (80-120 words) describing a single \
breathtaking photograph of this place. Your prompt MUST include ALL of these elements:

   SUBJECT: What specifically is in frame? Not "a restaurant" but "a cramped \
eight-seat ramen counter with a chef mid-motion ladling broth, steam rising into \
the overhead pendant light." Be surgically specific about what is visible.

   SENSORY DETAIL: One atmospheric detail that makes the scene feel alive — steam, \
condensation, morning dew, crumbling plaster, peeling paint, flickering lanterns, \
wet stone, fabric rippling in wind, smoke from a grill, light catching dust motes.

   DEPTH AND LAYERS: Describe at least three depth planes — what is in the blurred \
foreground (a plant, a hand on a railing, a cafe table edge, a hanging lantern), \
what is the sharp midground subject, and what is in the soft background.

   CULTURAL SPECIFICITY: Include one detail unique to {city_name} or this \
neighborhood — something that could NOT exist in any other city. An architectural \
detail, a specific material, a type of signage, a local plant species.

   DO NOT include camera settings, device names, or photography terminology like \
"bokeh" or "f-stop". DO NOT use generic adjectives like "beautiful", "stunning", \
or "amazing." DO NOT describe the mood — describe the physical scene and let the \
mood emerge from the details.

Places:
{numbered_place_list}

Return ONLY a JSON object with a "results" key containing an array of objects:
{{"results": [{{"place_id": <int>, "neighborhood": "<neighborhood/district>", \
"image_prompt": "<detailed visual description for image generation>"}}]}}"""


def _needs_enrichment(place: sqlite3.Row) -> bool:
    """Return True if the place is missing neighborhood or image_prompt."""
    return not place["neighborhood"] or not place["image_prompt"]


def _build_place_list(places: list[sqlite3.Row]) -> str:
    """Format places as a numbered list for the LLM prompt."""
    lines: list[str] = []
    for place in places:
        name = sanitize_text(place["name"] or "", max_length=200)
        place_type = sanitize_text(place["type"] or "", max_length=100)
        category = sanitize_text(place["category"] or "unknown", max_length=100)
        sample = sanitize_text(place["sample_caption"] or "", max_length=200)
        caption_part = f', sample caption: "{sample}"' if sample else ""
        lines.append(
            f"- ID {place['id']}: {name} (type: {place_type}, category: {category}{caption_part})"
        )
    return "\n".join(lines)


def _extract_results(parsed: dict | list) -> list[dict]:
    """Extract the results array from the parsed LLM response."""
    if isinstance(parsed, dict):
        results = parsed.get("results", [])
    elif isinstance(parsed, list):
        results = parsed
    else:
        return []
    return results if isinstance(results, list) else []


def enrich_places(
    conn: sqlite3.Connection,
    places: list[sqlite3.Row],
    city_name: str,
) -> int:
    """Enrich places with neighborhood and image_prompt data via LLM.

    Processes places in batches of ENRICHMENT_BATCH_SIZE. Skips places that
    already have both neighborhood AND image_prompt populated. Commits after
    each batch so that progress survives crashes.

    Returns the number of places successfully enriched.
    """
    # Filter to only places that need enrichment
    to_enrich = [p for p in places if _needs_enrichment(p)]

    if not to_enrich:
        log.info("All %d places already enriched — nothing to do", len(places))
        return 0

    log.info(
        "Enriching %d/%d places for %s",
        len(to_enrich),
        len(places),
        city_name,
    )

    batch_size = ENRICHMENT_BATCH_SIZE
    total_batches = math.ceil(len(to_enrich) / batch_size)
    total_enriched = 0

    for batch_num in range(total_batches):
        start = batch_num * batch_size
        end = start + batch_size
        batch = to_enrich[start:end]

        log.info(
            "Enriching batch %d/%d (%d places)...",
            batch_num + 1,
            total_batches,
            len(batch),
        )

        numbered_place_list = _build_place_list(batch)
        prompt = USER_PROMPT_TEMPLATE.format(
            city_name=city_name,
            numbered_place_list=numbered_place_list,
        )

        try:
            parsed = call_llm_json(prompt, system=SYSTEM_PROMPT, temperature=0.4)
        except LLMError:
            log.exception(
                "LLM call failed for enrichment batch %d/%d — skipping",
                batch_num + 1,
                total_batches,
            )
            continue

        results = _extract_results(parsed)

        # Build a lookup from place_id to enrichment data
        enrichment_lookup: dict[int, dict] = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            place_id = item.get("place_id")
            if not isinstance(place_id, int):
                continue
            neighborhood = item.get("neighborhood", "")
            image_prompt = item.get("image_prompt", "")
            if neighborhood and image_prompt:
                enrichment_lookup[place_id] = {
                    "neighborhood": str(neighborhood).strip(),
                    "image_prompt": str(image_prompt).strip(),
                }

        # Apply enrichments
        batch_enriched = 0
        for place in batch:
            enrichment = enrichment_lookup.get(place["id"])
            if not enrichment:
                log.warning(
                    "No enrichment data returned for place %d (%s) — skipping",
                    place["id"],
                    place["name"],
                )
                continue

            conn.execute(
                "UPDATE places SET neighborhood = ?, image_prompt = ? WHERE id = ?",
                (enrichment["neighborhood"], enrichment["image_prompt"], place["id"]),
            )
            batch_enriched += 1

        conn.commit()
        total_enriched += batch_enriched
        log.info(
            "Batch %d/%d committed — %d/%d places enriched",
            batch_num + 1,
            total_batches,
            batch_enriched,
            len(batch),
        )

    log.info(
        "Enrichment complete for %s: %d/%d places enriched",
        city_name,
        total_enriched,
        len(to_enrich),
    )
    return total_enriched
