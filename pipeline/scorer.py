"""Step 4 — Fuzzy deduplication of places and virality scoring."""

import logging
import math
import sqlite3
from rapidfuzz import fuzz, process as rfprocess

import config
from . import db
from .llm import call_llm_json, LLMError

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
    names = [normed[pid] for pid in ids]
    pairs: list[tuple[int, int]] = []

    if len(ids) < 2:
        return pairs

    # Vectorized fuzzy matching via rapidfuzz cdist (C-optimized)
    score_matrix = rfprocess.cdist(
        names, names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=config.DEDUP_SCORE_CUTOFF,
        workers=-1,
    )
    # Extract upper-triangle pairs above threshold
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            if score_matrix[i][j] >= config.DEDUP_SCORE_CUTOFF:
                pairs.append((ids[i], ids[j]))
                continue

            # Containment check for pairs not caught by token_sort_ratio
            na, nb = names[i], names[j]
            if na in nb or nb in na:
                longer = max(len(na), len(nb))
                shorter = min(len(na), len(nb))
                if longer > 0 and (shorter / longer) > config.DEDUP_RELATIVE_THRESHOLD:
                    pairs.append((ids[i], ids[j]))

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
        log.debug("Checking merge group: %s", [p["name"] for p in group_places])

        confirmed_subgroups = _ask_llm_to_confirm_groups(group_places, city_name)

        for subgroup_ids in confirmed_subgroups:
            # Pick the place with the highest mention count as canonical
            subgroup_places = [place_by_id[pid] for pid in subgroup_ids]
            canonical = max(subgroup_places, key=lambda p: p["mention_count"])
            merge_ids = [p["id"] for p in subgroup_places if p["id"] != canonical["id"]]

            if not merge_ids:
                continue

            log.debug(
                "Merging %s into '%s' (id=%d)",
                [place_by_id[mid]["name"] for mid in merge_ids],
                canonical["name"], canonical["id"],
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
) -> int:
    """Calculate and store virality scores for all places. Returns count scored."""
    places = db.get_all_places(conn, city_id)
    if not places:
        log.info("No places to score")
        return 0

    # Single JOIN query to get all engagement data for all places in the city
    rows = conn.execute(
        """SELECT p.id AS place_id, p.name,
                  COALESCE(rp.saves, 0) AS saves,
                  COALESCE(rp.shares, 0) AS shares,
                  COALESCE(rp.comments, 0) AS comments_count,
                  COALESCE(rp.likes, 0) AS likes,
                  COALESCE(rp.views, 0) AS views
           FROM places p
           LEFT JOIN place_posts pp ON pp.place_id = p.id
           LEFT JOIN raw_posts rp ON rp.id = pp.post_id
           WHERE p.city_id = ?""",
        (city_id,),
    ).fetchall()

    # Aggregate per place
    place_data: dict[int, dict] = {}
    for row in rows:
        pid = row["place_id"]
        if pid not in place_data:
            place_data[pid] = {"name": row["name"], "total_score": 0.0, "post_count": 0}
        if row["likes"] is not None:  # has a linked post
            engagement = (
                row["saves"] * config.WEIGHT_SAVES
                + row["shares"] * config.WEIGHT_SHARES
                + row["comments_count"] * config.WEIGHT_COMMENTS
                + row["likes"] * config.WEIGHT_LIKES
            )
            views = max(row["views"], 1)
            place_data[pid]["total_score"] += engagement / views
            place_data[pid]["post_count"] += 1

    # Batch update
    updates = []
    for pid, data in place_data.items():
        if data["post_count"] == 0:
            updates.append((0.0, pid))
        else:
            mention_bonus = math.log(data["post_count"] + 1)
            final_score = round(data["total_score"] * mention_bonus, 4)
            updates.append((final_score, pid))
            log.debug(
                "  %s: score=%.4f (posts=%d, mention_bonus=%.2f)",
                data["name"], final_score, data["post_count"], mention_bonus,
            )

    conn.executemany("UPDATE places SET virality_score = ? WHERE id = ?", updates)
    conn.commit()

    scored = len(updates)
    log.info("Scored %d places", scored)
    return scored


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def deduplicate_and_score(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
) -> dict[str, int]:
    """Run fuzzy dedup then virality scoring for a city.

    Returns a summary dict with keys 'merged' and 'scored'.
    """
    log.info("=== Step 4: Dedup & Score — %s ===", city_name)

    merged = _perform_dedup(conn, city_id, city_name)
    scored = _score_places(conn, city_id)

    log.info("Done — merged %d duplicates, scored %d places", merged, scored)
    return {"merged": merged, "scored": scored}
