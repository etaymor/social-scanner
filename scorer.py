"""Step 4 — Fuzzy deduplication of places and virality scoring."""

import json
import logging
import math
import sqlite3
from itertools import combinations

from rapidfuzz import fuzz

import config
import db
from llm import call_llm_json, LLMError

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    """Lowercase, strip leading 'the ', collapse whitespace."""
    n = name.lower().strip()
    if n.startswith("the "):
        n = n[4:]
    return " ".join(n.split())


def _build_merge_groups(pairs: list[tuple[int, int]], all_ids: set[int]) -> list[set[int]]:
    """Given duplicate pairs, return connected components (union-find)."""
    parent: dict[int, int] = {i: i for i in all_ids}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for a, b in pairs:
        union(a, b)

    groups: dict[int, set[int]] = {}
    for i in all_ids:
        root = find(i)
        groups.setdefault(root, set()).add(i)

    return [g for g in groups.values() if len(g) >= 2]


# ---------------------------------------------------------------------------
# Fuzzy dedup
# ---------------------------------------------------------------------------

def _find_candidate_pairs(
    places: list[sqlite3.Row],
) -> list[tuple[int, int]]:
    """Return (place_id_a, place_id_b) pairs that may be duplicates."""
    normed: dict[int, str] = {}
    for p in places:
        normed[p["id"]] = _normalize_name(p["name"])

    ids = list(normed.keys())
    pairs: list[tuple[int, int]] = []

    for i, j in combinations(range(len(ids)), 2):
        id_a, id_b = ids[i], ids[j]
        na, nb = normed[id_a], normed[id_b]

        # Token sort ratio check
        score = fuzz.token_sort_ratio(na, nb)
        if score >= config.DEDUP_SCORE_CUTOFF:
            pairs.append((id_a, id_b))
            continue

        # Containment check — if one name fully contains the other, it's
        # a strong signal even when lengths differ significantly.
        if na in nb or nb in na:
            longer = max(len(na), len(nb))
            shorter = min(len(na), len(nb))
            if longer > 0 and (shorter / longer) > config.DEDUP_RELATIVE_THRESHOLD:
                pairs.append((id_a, id_b))

    return pairs


def _ask_llm_to_confirm_groups(
    group_places: list[sqlite3.Row],
    city_name: str,
) -> list[list[int]]:
    """Ask the LLM which places in a merge group are truly the same.

    Returns a list of confirmed sub-groups, each being a list of place IDs.
    """
    numbered = "\n".join(
        f"  {idx}. {p['name']}" for idx, p in enumerate(group_places)
    )
    prompt = (
        f"These place names were found in social media posts about {city_name}.\n"
        f"Some may refer to the same place. Group the ones that are definitely the same place.\n\n"
        f"Places:\n{numbered}\n\n"
        f"Return ONLY a JSON object with a \"groups\" key containing an array of arrays.\n"
        f"Each inner array has the indices (0-based) of places that are the same.\n"
        f"Places that are unique should appear as single-element arrays.\n"
        f"Example: {{\"groups\": [[0, 2], [1], [3, 4]]}}"
    )

    try:
        result = call_llm_json(prompt, temperature=0.3)
    except LLMError:
        log.warning("LLM dedup confirmation failed; skipping merge for this group")
        return []

    raw_groups: list[list[int]] = result.get("groups", [])
    confirmed: list[list[int]] = []

    for g in raw_groups:
        if not isinstance(g, list) or len(g) < 2:
            continue
        # Convert indices to place IDs, validating bounds
        place_ids: list[int] = []
        for idx in g:
            if isinstance(idx, int) and 0 <= idx < len(group_places):
                place_ids.append(group_places[idx]["id"])
        if len(place_ids) >= 2:
            confirmed.append(place_ids)

    return confirmed


def _perform_dedup(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    verbose: bool = False,
) -> int:
    """Find and merge duplicate places. Returns the number of places merged away."""
    places = db.get_all_places(conn, city_id)
    if len(places) < 2:
        log.info("Fewer than 2 places — nothing to deduplicate")
        return 0

    candidate_pairs = _find_candidate_pairs(places)
    if not candidate_pairs:
        log.info("No duplicate candidates found")
        return 0

    log.info("Found %d candidate duplicate pairs", len(candidate_pairs))

    all_candidate_ids = set()
    for a, b in candidate_pairs:
        all_candidate_ids.add(a)
        all_candidate_ids.add(b)

    merge_groups = _build_merge_groups(candidate_pairs, all_candidate_ids)
    log.info("Formed %d merge groups to verify with LLM", len(merge_groups))

    # Build a lookup by place ID
    place_by_id: dict[int, sqlite3.Row] = {p["id"]: p for p in places}

    total_merged = 0

    for group_ids in merge_groups:
        group_places = [place_by_id[pid] for pid in sorted(group_ids)]
        if verbose:
            names = [p["name"] for p in group_places]
            log.info("Checking merge group: %s", names)

        confirmed_subgroups = _ask_llm_to_confirm_groups(group_places, city_name)

        for subgroup_ids in confirmed_subgroups:
            # Pick the place with the highest mention count as canonical
            subgroup_places = [place_by_id[pid] for pid in subgroup_ids]
            canonical = max(subgroup_places, key=lambda p: p["mention_count"])
            merge_ids = [p["id"] for p in subgroup_places if p["id"] != canonical["id"]]

            if not merge_ids:
                continue

            if verbose:
                merge_names = [place_by_id[mid]["name"] for mid in merge_ids]
                log.info(
                    "Merging %s into '%s' (id=%d)",
                    merge_names, canonical["name"], canonical["id"],
                )

            db.merge_places(conn, canonical["id"], merge_ids)
            total_merged += len(merge_ids)

    if total_merged:
        conn.commit()
        log.info("Dedup complete: merged away %d duplicate places", total_merged)
    else:
        log.info("LLM confirmed no true duplicates")

    return total_merged


# ---------------------------------------------------------------------------
# Virality scoring
# ---------------------------------------------------------------------------

def _score_places(
    conn: sqlite3.Connection,
    city_id: int,
    verbose: bool = False,
) -> int:
    """Calculate and store virality scores for all places. Returns count scored."""
    places = db.get_all_places(conn, city_id)
    if not places:
        log.info("No places to score")
        return 0

    scored = 0
    for place in places:
        place_id: int = place["id"]
        post_ids = db.get_place_post_ids(conn, place_id)
        posts = db.get_posts_by_ids(conn, post_ids)

        if not posts:
            db.update_virality_score(conn, place_id, 0.0)
            continue

        total_score = 0.0
        for post in posts:
            engagement = (
                (post["saves"] or 0) * config.WEIGHT_SAVES
                + (post["shares"] or 0) * config.WEIGHT_SHARES
                + (post["comments"] or 0) * config.WEIGHT_COMMENTS
                + (post["likes"] or 0) * config.WEIGHT_LIKES
            )
            views = max(post["views"] or 0, 1)
            rate = engagement / views
            total_score += rate

        mention_bonus = math.log(len(posts) + 1)
        final_score = round(total_score * mention_bonus, 4)

        db.update_virality_score(conn, place_id, final_score)
        scored += 1

        if verbose:
            log.info(
                "  %s: score=%.4f (posts=%d, mention_bonus=%.2f)",
                place["name"], final_score, len(posts), mention_bonus,
            )

    conn.commit()
    log.info("Scored %d places", scored)
    return scored


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def deduplicate_and_score(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    verbose: bool = False,
) -> dict[str, int]:
    """Run fuzzy dedup then virality scoring for a city.

    Returns a summary dict with keys 'merged' and 'scored'.
    """
    log.info("=== Step 4: Dedup & Score — %s ===", city_name)

    merged = _perform_dedup(conn, city_id, city_name, verbose=verbose)
    scored = _score_places(conn, city_id, verbose=verbose)

    log.info("Done — merged %d duplicates, scored %d places", merged, scored)
    return {"merged": merged, "scored": scored}
