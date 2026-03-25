"""LLM enrichment for places — adds neighborhood and image prompt data."""

import json
import logging
import math
import sqlite3

from .llm import call_llm, LLMError, sanitize_text

log = logging.getLogger(__name__)

ENRICHMENT_BATCH_SIZE = 10

SYSTEM_PROMPT = """\
You are a travel expert with deep knowledge of neighborhoods, districts, and the \
visual character of places around the world. You help enrich place data with accurate \
neighborhood information and vivid visual descriptions."""

USER_PROMPT_TEMPLATE = """\
For each place listed below in {city_name}, provide:
1. The neighborhood or district within the city where this place is located.
2. A detailed visual description suitable for AI image generation, describing what \
this type of place typically looks like in this city. Emphasize natural iPhone \
photography aesthetics — warm natural lighting, candid perspective, slightly shallow \
depth of field, authentic atmosphere.

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
        sample = sanitize_text(place["sample_caption"] or "", max_length=200)
        caption_part = f', sample caption: "{sample}"' if sample else ""
        lines.append(
            f"- ID {place['id']}: {place['name']} (type: {place['type']}, "
            f"category: {place['category'] or 'unknown'}{caption_part})"
        )
    return "\n".join(lines)


def _parse_enrichment_response(raw: str) -> list[dict]:
    """Defensively parse the LLM JSON response into a list of enrichment dicts."""
    text = raw.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    # Try direct parse
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # Try to extract JSON object or array from the response
        for start_char, end_char in [("{", "}"), ("[", "]")]:
            start = text.find(start_char)
            end = text.rfind(end_char)
            if start != -1 and end != -1 and end > start:
                try:
                    parsed = json.loads(text[start:end + 1])
                    break
                except json.JSONDecodeError:
                    continue
        else:
            log.warning("Failed to parse LLM enrichment response as JSON")
            return []

    # Extract results array
    if isinstance(parsed, dict):
        results = parsed.get("results", [])
    elif isinstance(parsed, list):
        results = parsed
    else:
        return []

    if not isinstance(results, list):
        return []

    return results


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
        len(to_enrich), len(places), city_name,
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
            batch_num + 1, total_batches, len(batch),
        )

        numbered_place_list = _build_place_list(batch)
        prompt = USER_PROMPT_TEMPLATE.format(
            city_name=city_name,
            numbered_place_list=numbered_place_list,
        )

        try:
            raw_response = call_llm(prompt, system=SYSTEM_PROMPT, temperature=0.4)
        except LLMError:
            log.exception(
                "LLM call failed for enrichment batch %d/%d — skipping",
                batch_num + 1, total_batches,
            )
            continue

        results = _parse_enrichment_response(raw_response)

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
                    place["id"], place["name"],
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
            batch_num + 1, total_batches, batch_enriched, len(batch),
        )

    log.info(
        "Enrichment complete for %s: %d/%d places enriched",
        city_name, total_enriched, len(to_enrich),
    )
    return total_enriched
