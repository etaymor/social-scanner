"""Step 5 — Classify places as tourist traps using an LLM."""

import logging
import math
import sqlite3

import config
import db
from llm import call_llm_json, LLMError

log = logging.getLogger(__name__)

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
        len(places), city_name, total_batches,
    )

    for batch_num in range(total_batches):
        start = batch_num * batch_size
        end = start + batch_size
        batch = places[start:end]

        log.info(
            "Filtering batch %d/%d (%d places)...",
            batch_num + 1, total_batches, len(batch),
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
            idx = item.get("index")
            is_trap = item.get("is_tourist_trap", False)
            if idx is not None:
                trap_lookup[idx] = bool(is_trap)

        for i, place in enumerate(batch):
            if i not in trap_lookup:
                continue
            is_trap = trap_lookup[i]
            db.update_tourist_trap(conn, place["id"], is_trap)
            if is_trap:
                reason = ""
                for item in results:
                    if item.get("index") == i:
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
        city_name, trap_count, len(places),
    )
