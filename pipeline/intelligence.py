"""Intelligence module: aggregate performance, compute weights, apply decision rules.

Reads slideshow_analytics and slideshow_performance to compute per-dimension
Bayesian-smoothed weights with exponential decay.  Writes the learned weights
to ``performance_weights.json`` atomically (tmp + rename).
"""

import json
import logging
import os
import sqlite3
import statistics
from datetime import UTC, datetime, timedelta
from pathlib import Path

import config

log = logging.getLogger(__name__)

# The 9 tracked dimensions.  The first 5 come directly from slideshows
# columns; the next 4 are decomposed from the visual_style JSON column;
# virality_band is derived from places.virality_score.
DIMENSIONS = (
    "category",
    "city",
    "format",
    "time_of_day",
    "weather",
    "perspective",
    "color_mood",
    "cta",
    "virality_band",
)

_VISUAL_STYLE_KEYS = ("time_of_day", "weather", "perspective", "color_mood")

_VIRALITY_BANDS = ("0-25", "25-50", "50-75", "75-100")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _virality_band(score: float) -> str:
    """Bucket a virality_score (0-100) into a quartile band string."""
    if score < 25:
        return "0-25"
    if score < 50:
        return "25-50"
    if score < 75:
        return "50-75"
    return "75-100"


def _parse_visual_style(raw: str | None) -> dict[str, str]:
    """Parse the visual_style JSON column into a dict of axis name -> value.

    Returns an empty dict if *raw* is None or not valid JSON.
    """
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        result: dict[str, str] = {}
        for key in _VISUAL_STYLE_KEYS:
            val = data.get(key)
            if isinstance(val, dict):
                # e.g. {"name": "golden_hour", "desc": "..."}
                result[key] = val.get("name", "")
            elif isinstance(val, str):
                result[key] = val
        return result
    except (json.JSONDecodeError, TypeError):
        return {}


def _get_alpha() -> float:
    """Return the composite-score blending alpha.

    1.0 (views-only) when RevenueCat is not configured; 0.6 when it is.
    """
    if config.REVENUECAT_V2_SECRET_KEY:
        return 0.6
    return config.SCORE_VIEWS_WEIGHT  # 1.0


def _days_since(iso_str: str) -> float:
    """Return fractional days since *iso_str* (assumed UTC)."""
    try:
        dt = datetime.fromisoformat(iso_str)
    except (ValueError, TypeError):
        # SQLite CURRENT_TIMESTAMP produces 'YYYY-MM-DD HH:MM:SS' (no tz)
        try:
            dt = datetime.strptime(iso_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        except (ValueError, TypeError):
            return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    delta = datetime.now(UTC) - dt
    return max(delta.total_seconds() / 86400, 0.0)


# ---------------------------------------------------------------------------
# evaluate_slideshows
# ---------------------------------------------------------------------------


def evaluate_slideshows(conn: sqlite3.Connection) -> int:
    """Evaluate matured (48 h+) published slideshows that have analytics data.

    For each eligible slideshow, computes ``views_at_48h``, ``composite_score``,
    and a ``decision_tag``.  Inserts an **append-only** row into
    ``slideshow_performance``.

    Does NOT commit -- caller controls the transaction.

    Returns the number of slideshows evaluated.
    """
    maturation = config.POST_MATURATION_HOURS  # 48

    # Published slideshows matured 48 h+ that have at least one analytics row
    rows = conn.execute(
        """
        SELECT s.id, s.posted_at, s.category, s.city_id, s.format,
               s.visual_style, s.cta_text
        FROM slideshows s
        WHERE s.publish_status = 'published'
          AND s.posted_at IS NOT NULL
          AND s.posted_at <= datetime('now', ?)
          AND EXISTS (
              SELECT 1 FROM slideshow_analytics sa WHERE sa.slideshow_id = s.id
          )
          AND NOT EXISTS (
              SELECT 1 FROM slideshow_performance sp
              WHERE sp.slideshow_id = s.id AND sp.decision_tag IS NOT NULL
          )
        """,
        (f"-{maturation} hours",),
    ).fetchall()

    if not rows:
        return 0

    # Global median views for normalization
    all_views = conn.execute(
        """
        SELECT sa.views
        FROM slideshow_analytics sa
        JOIN slideshows s ON s.id = sa.slideshow_id
        WHERE s.publish_status = 'published'
          AND s.posted_at IS NOT NULL
          AND s.posted_at <= datetime('now', ?)
        ORDER BY sa.fetched_at DESC
        """,
        (f"-{maturation} hours",),
    ).fetchall()

    view_values = [r["views"] for r in all_views if r["views"] and r["views"] > 0]
    global_median = statistics.median(view_values) if view_values else 1.0

    alpha = _get_alpha()
    evaluated = 0

    for row in rows:
        slideshow_id = row["id"]
        posted_at = row["posted_at"]

        # Snapshot closest to 48 h post-publish
        target_time_str = _offset_timestamp(posted_at, maturation)
        snap = conn.execute(
            """
            SELECT views, likes, comments, shares, saves, views_estimated
            FROM slideshow_analytics
            WHERE slideshow_id = ?
            ORDER BY ABS(
                julianday(fetched_at) - julianday(?)
            )
            LIMIT 1
            """,
            (slideshow_id, target_time_str),
        ).fetchone()

        if not snap:
            continue

        views_at_48h = snap["views"] or 0
        views_estimated = bool(snap["views_estimated"])
        views_confidence = 0.5 if views_estimated else 1.0

        # Latest analytics snapshot for views_latest
        latest = conn.execute(
            "SELECT views, likes, comments, shares, saves FROM slideshow_analytics "
            "WHERE slideshow_id = ? ORDER BY fetched_at DESC LIMIT 1",
            (slideshow_id,),
        ).fetchone()

        views_latest = latest["views"] if latest else views_at_48h
        likes = latest["likes"] if latest else 0
        comments = latest["comments"] if latest else 0
        shares = latest["shares"] if latest else 0
        saves = latest["saves"] if latest else 0

        # Conversions (may have been attributed by conversions.py)
        conv_row = conn.execute(
            "SELECT COALESCE(SUM(conversions), 0) as total FROM slideshow_performance "
            "WHERE slideshow_id = ?",
            (slideshow_id,),
        ).fetchone()
        conversions = conv_row["total"] if conv_row else 0

        # Composite score
        normalized_views = views_at_48h / global_median if global_median > 0 else 0.0
        conversions_per_1k = (conversions / max(views_at_48h, 1)) * 1000
        composite_score = normalized_views * alpha + conversions_per_1k * (1 - alpha)

        conversion_rate = conversions / max(views_at_48h, 1)

        # Decision tag
        decision_tag = apply_decision_rules(views_at_48h, conn, slideshow_id)

        conn.execute(
            """
            INSERT INTO slideshow_performance
                (slideshow_id, evaluated_at, views_at_48h, views_latest,
                 likes, comments, shares, saves,
                 conversions, conversion_rate, composite_score,
                 views_estimated, views_confidence, decision_tag)
            VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                slideshow_id,
                views_at_48h,
                views_latest,
                likes,
                comments,
                shares,
                saves,
                conversions,
                conversion_rate,
                composite_score,
                views_estimated,
                views_confidence,
                decision_tag,
            ),
        )
        evaluated += 1
        log.info(
            "Evaluated slideshow %d: views_48h=%d composite=%.3f tag=%s",
            slideshow_id,
            views_at_48h,
            composite_score,
            decision_tag,
        )

    return evaluated


def _offset_timestamp(iso_str: str, hours: int) -> str:
    """Return *iso_str* + *hours* as an ISO-format string (no tz)."""
    try:
        dt = datetime.fromisoformat(iso_str)
    except (ValueError, TypeError):
        try:
            dt = datetime.strptime(iso_str, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return iso_str
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    result = dt + timedelta(hours=hours)
    return result.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# apply_decision_rules
# ---------------------------------------------------------------------------


def apply_decision_rules(
    views_at_48h: int, conn: sqlite3.Connection, slideshow_id: int
) -> str:
    """Return a decision tag based on view thresholds.

    * >= VIEWS_SCALE (50 K) -> "scale"
    * >= VIEWS_GOOD  (10 K) -> "keep"
    * >= VIEWS_TEST  ( 1 K) -> "test"
    * <  VIEWS_TEST  -> "drop" if this is the *second* sub-1 K evaluation
      for the same dimension combination; otherwise "test"
    """
    if views_at_48h >= config.VIEWS_SCALE:
        return "scale"
    if views_at_48h >= config.VIEWS_GOOD:
        return "keep"
    if views_at_48h >= config.VIEWS_TEST:
        return "test"

    # < 1 K — check for prior sub-1 K evaluation on same dimension combo
    row = conn.execute(
        "SELECT category, city_id, format FROM slideshows WHERE id = ?",
        (slideshow_id,),
    ).fetchone()
    if not row:
        return "test"

    category = row["category"]
    city_id = row["city_id"]
    fmt = row["format"]

    prior = conn.execute(
        """
        SELECT COUNT(*) as cnt
        FROM slideshow_performance sp
        JOIN slideshows s ON s.id = sp.slideshow_id
        WHERE s.category IS ? AND s.city_id = ? AND s.format = ?
          AND sp.views_at_48h < ?
          AND sp.slideshow_id != ?
        """,
        (category, city_id, fmt, config.VIEWS_TEST, slideshow_id),
    ).fetchone()

    if prior and prior["cnt"] >= 1:
        return "drop"
    return "test"


# ---------------------------------------------------------------------------
# compute_dimension_weights
# ---------------------------------------------------------------------------


def compute_dimension_weights(
    conn: sqlite3.Connection,
    previous_weights: dict | None = None,
) -> dict[str, dict[str, float]]:
    """Compute per-dimension Bayesian-smoothed weights with exponential decay.

    Returns a nested dict: ``{dimension_name: {value: weight, ...}, ...}``
    with all 9 dimension keys present.
    """
    if previous_weights is None:
        previous_weights = {}

    decay_days = config.WEIGHT_DECAY_DAYS
    decay_factor = config.WEIGHT_DECAY_FACTOR
    min_posts = config.MIN_POSTS_FOR_WEIGHT
    prior_n = config.WEIGHT_PRIOR_N
    min_w = config.MIN_WEIGHT
    max_w = config.MAX_WEIGHT
    max_delta = config.MAX_WEIGHT_DELTA_PER_DAY

    # Fetch matured performance data within the decay window
    rows = conn.execute(
        """
        SELECT sp.slideshow_id, sp.composite_score, sp.views_confidence,
               sp.evaluated_at, sp.views_at_48h,
               s.category, s.city_id, s.format, s.visual_style, s.cta_text,
               s.posted_at, s.publish_status
        FROM slideshow_performance sp
        JOIN slideshows s ON s.id = sp.slideshow_id
        WHERE s.publish_status = 'published'
          AND s.posted_at IS NOT NULL
          AND s.posted_at >= datetime('now', ?)
          AND s.posted_at <= datetime('now', ?)
        """,
        (f"-{decay_days} days", f"-{config.POST_MATURATION_HOURS} hours"),
    ).fetchall()

    # Batch fetch virality bands for all slideshows in one query
    virality_map: dict[int, str] = {}
    slideshow_ids = list({row["slideshow_id"] for row in rows})
    if slideshow_ids:
        placeholders = ",".join("?" * len(slideshow_ids))
        vr_rows = conn.execute(
            f"""
            SELECT sp_link.slideshow_id, AVG(p.virality_score) as avg_v
            FROM slideshow_places sp_link
            JOIN places p ON p.id = sp_link.place_id
            WHERE sp_link.slideshow_id IN ({placeholders})
            GROUP BY sp_link.slideshow_id
            """,
            slideshow_ids,
        ).fetchall()
        for vr in vr_rows:
            score = vr["avg_v"] if vr["avg_v"] is not None else 50.0
            virality_map[vr["slideshow_id"]] = _virality_band(score)
        # Default for slideshows with no linked places
        for sid in slideshow_ids:
            if sid not in virality_map:
                virality_map[sid] = _virality_band(50.0)

    # Build per-dimension groups and overall scores in a single pass
    groups: dict[str, dict[str, list[tuple[float, float]]]] = {
        d: {} for d in DIMENSIONS
    }
    all_scores: list[tuple[float, float]] = []

    for row in rows:
        score = row["composite_score"] or 0.0
        confidence = row["views_confidence"] if row["views_confidence"] is not None else 1.0
        posted_at = row["posted_at"]
        days = _days_since(posted_at) if posted_at else 0.0
        decay_w = (decay_factor ** days) * confidence

        all_scores.append((score, decay_w))

        # Dimension values for this slideshow
        dim_values: dict[str, str | None] = {
            "category": row["category"],
            "city": str(row["city_id"]) if row["city_id"] else None,
            "format": row["format"],
            "cta": row["cta_text"],
            "virality_band": virality_map.get(row["slideshow_id"]),
        }

        # Decompose visual_style into 4 sub-dimensions
        vs = _parse_visual_style(row["visual_style"])
        for key in _VISUAL_STYLE_KEYS:
            dim_values[key] = vs.get(key)

        for dim in DIMENSIONS:
            val = dim_values.get(dim)
            if val is None or val == "":
                continue
            groups[dim].setdefault(val, []).append((score, decay_w))

    total_weight_sum = sum(w for _, w in all_scores)
    overall_avg = (
        sum(s * w for s, w in all_scores) / total_weight_sum
        if total_weight_sum > 0
        else 1.0
    )
    # Guard against zero average
    if overall_avg <= 0:
        overall_avg = 1.0

    # Compute weights per dimension
    result: dict[str, dict[str, float]] = {d: {} for d in DIMENSIONS}

    for dim in DIMENSIONS:
        prev_dim = previous_weights.get(dim, {})
        for val, entries in groups[dim].items():
            n = len(entries)
            if n < min_posts:
                result[dim][val] = 1.0
                continue

            # Decay-weighted average
            w_sum = sum(w for _, w in entries)
            if w_sum <= 0:
                result[dim][val] = 1.0
                continue
            weighted_avg = sum(s * w for s, w in entries) / w_sum

            # Raw weight = ratio to overall average
            raw_weight = weighted_avg / overall_avg

            # Bayesian smoothing
            adjusted = (n * raw_weight + prior_n * 1.0) / (n + prior_n)

            # Clamp
            clamped = max(min_w, min(max_w, adjusted))

            # Rate limiter
            prev_val = prev_dim.get(val, 1.0) if isinstance(prev_dim, dict) else 1.0
            delta = clamped - prev_val
            if abs(delta) > max_delta:
                clamped = prev_val + max_delta * (1.0 if delta > 0 else -1.0)
            # Re-clamp after rate limiting
            clamped = max(min_w, min(max_w, clamped))

            result[dim][val] = round(clamped, 4)

    return result


# ---------------------------------------------------------------------------
# check_circuit_breaker
# ---------------------------------------------------------------------------


def check_circuit_breaker(conn: sqlite3.Connection) -> bool:
    """Return True (and log critical) if 7-day avg views < 50 % of 30-day avg.

    This is a safety valve -- when triggered, all weights should be reset
    to 1.0 by the caller.
    """
    avg_7 = conn.execute(
        """
        SELECT AVG(sa.views) as avg_views
        FROM slideshow_analytics sa
        JOIN slideshows s ON s.id = sa.slideshow_id
        WHERE s.posted_at >= datetime('now', '-7 days')
          AND s.publish_status = 'published'
        """
    ).fetchone()

    avg_30 = conn.execute(
        """
        SELECT AVG(sa.views) as avg_views
        FROM slideshow_analytics sa
        JOIN slideshows s ON s.id = sa.slideshow_id
        WHERE s.posted_at >= datetime('now', '-30 days')
          AND s.publish_status = 'published'
        """
    ).fetchone()

    views_7 = avg_7["avg_views"] if avg_7 and avg_7["avg_views"] is not None else None
    views_30 = avg_30["avg_views"] if avg_30 and avg_30["avg_views"] is not None else None

    if views_7 is None or views_30 is None or views_30 <= 0:
        return False

    ratio = views_7 / views_30
    if ratio < config.CIRCUIT_BREAKER_THRESHOLD:
        log.critical(
            "CIRCUIT BREAKER: 7-day avg views (%.0f) is %.0f%% of 30-day avg (%.0f). "
            "Resetting all weights to 1.0.",
            views_7,
            ratio * 100,
            views_30,
        )
        return True

    return False


# ---------------------------------------------------------------------------
# write_weights / read_weights
# ---------------------------------------------------------------------------


def write_weights(
    weights_dict: dict[str, dict[str, float]],
    path: Path | str | None = None,
    post_count: int = 0,
    circuit_breaker: bool = False,
) -> None:
    """Atomically write *weights_dict* to ``performance_weights.json``.

    Writes to a ``.tmp`` sibling then ``os.replace()`` to guarantee readers
    always see a complete file.
    """
    target = Path(path) if path else config.PERFORMANCE_WEIGHTS_PATH

    payload = dict(weights_dict)
    payload["_meta"] = {
        "updated_at": datetime.now(UTC).isoformat(),
        "post_count": post_count,
        "circuit_breaker": circuit_breaker,
    }

    tmp_path = target.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp_path, target)

    log.info("Wrote performance weights to %s (post_count=%d)", target, post_count)


def read_weights(path: Path | str | None = None) -> dict[str, dict[str, float]]:
    """Read ``performance_weights.json`` and return the weights dict.

    Returns defaults (all dimensions present, empty value dicts) when the
    file is missing or corrupt so the pipeline always has a valid structure.
    """
    target = Path(path) if path else config.PERFORMANCE_WEIGHTS_PATH
    defaults: dict[str, dict[str, float]] = {d: {} for d in DIMENSIONS}

    if not target.exists():
        log.info("Weights file %s not found, using defaults (all 1.0)", target)
        return defaults

    try:
        with open(target) as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("top-level value is not a dict")

        meta = data.get("_meta", {})
        updated = meta.get("updated_at", "unknown")
        log.info("Loaded weights from %s (updated_at=%s)", target, updated)

        result: dict[str, dict[str, float]] = {}
        for dim in DIMENSIONS:
            dim_data = data.get(dim, {})
            if isinstance(dim_data, dict):
                result[dim] = {
                    k: float(v) for k, v in dim_data.items() if isinstance(v, (int, float))
                }
            else:
                result[dim] = {}
        return result

    except (json.JSONDecodeError, ValueError, OSError) as exc:
        log.warning("Corrupt weights file %s (%s), using defaults", target, exc)
        return defaults
