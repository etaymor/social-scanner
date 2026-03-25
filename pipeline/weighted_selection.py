"""Weighted selection utilities for the analytics intelligence loop.

Provides exploit/explore weighted random selection, clamped combined weights,
and weighted ranking for biasing slideshow generation toward high-performing
content dimensions.
"""

import logging
import random
from typing import Any, Callable, Sequence, TypeVar

import config

log = logging.getLogger(__name__)

T = TypeVar("T")


def roll_exploit_explore() -> bool:
    """Return True for exploit (use weights), False for explore (uniform random).

    Uses ``config.EXPLOIT_RATIO`` (default 0.7 = 70 % exploit).
    """
    return random.random() < config.EXPLOIT_RATIO


def weighted_choice(
    options: Sequence[str],
    weights_dict: dict[str, float],
    default_weight: float = 1.0,
) -> str:
    """Return a single option chosen via weighted random selection.

    On an *exploit* roll (70 %), selection uses weights from *weights_dict*.
    On an *explore* roll (30 %), selection is uniform random.

    Args:
        options: Available option keys (e.g. category slugs, format names).
        weights_dict: Mapping of option key -> weight.  Missing keys get
            *default_weight*.
        default_weight: Weight for options not present in *weights_dict*.

    Returns:
        One of the *options* strings.
    """
    if not options:
        raise ValueError("weighted_choice requires at least one option")

    option_list = list(options)

    if roll_exploit_explore():
        # Exploit: use performance weights
        weights = [weights_dict.get(opt, default_weight) for opt in option_list]
        log.debug("Exploit selection from %d options with weights %s", len(option_list), weights)
        return random.choices(option_list, weights=weights, k=1)[0]
    else:
        # Explore: uniform random
        log.debug("Explore selection (uniform) from %d options", len(option_list))
        return random.choice(option_list)


def clamped_combined_weight(
    dimension_weights: dict[str, dict[str, float]],
    candidate: dict[str, str],
    default_weight: float = 1.0,
) -> float:
    """Multiply per-dimension weights for a candidate, capping the ratio.

    The ratio between the best and worst candidate's combined weight is capped
    at ``config.MAX_COMBINED_WEIGHT_RATIO`` to prevent extreme skew from
    multiplicative combination of many dimensions.

    Args:
        dimension_weights: ``{dimension_name: {value: weight, ...}, ...}``
        candidate: ``{dimension_name: value, ...}`` for this candidate.
        default_weight: Weight when a dimension/value pair is missing.

    Returns:
        The multiplicative combined weight (unclamped).  Clamping across
        candidates is done by the caller (see ``weighted_rank``).
    """
    combined = 1.0
    for dim, val in candidate.items():
        dim_weights = dimension_weights.get(dim, {})
        w = dim_weights.get(val, default_weight)
        combined *= w
    return combined


def _clamp_weights(weights: list[float]) -> list[float]:
    """Re-normalize weights so max/min ratio does not exceed MAX_COMBINED_WEIGHT_RATIO."""
    if not weights:
        return weights

    min_w = min(weights)
    max_w = max(weights)

    if min_w <= 0:
        # Avoid division by zero; treat as uniform
        return [1.0] * len(weights)

    ratio = max_w / min_w
    if ratio <= config.MAX_COMBINED_WEIGHT_RATIO:
        return weights

    # Scale so that max/min = MAX_COMBINED_WEIGHT_RATIO
    # We compress toward the geometric mean
    import math

    log_min = math.log(min_w)
    log_max = math.log(max_w)
    log_range = log_max - log_min
    target_log_range = math.log(config.MAX_COMBINED_WEIGHT_RATIO)

    scale = target_log_range / log_range if log_range > 0 else 1.0

    return [math.exp((math.log(w) - log_min) * scale + log_min) for w in weights]


def weighted_rank(
    items: Sequence[T],
    score_fn: Callable[[T], float],
    weight_fn: Callable[[T], float],
    count: int | None = None,
) -> list[T]:
    """Re-rank items by multiplying each item's score by its weight.

    Used for place selection: ``final_score = virality_score * band_weight``.

    The ratio between the highest and lowest combined score is clamped at
    ``MAX_COMBINED_WEIGHT_RATIO`` to prevent extreme distortion.

    Args:
        items: Sequence of items to rank.
        score_fn: Callable returning the base score for an item
            (e.g. virality_score).
        weight_fn: Callable returning the weight for an item
            (e.g. virality band weight).
        count: If provided, return only the top *count* items.

    Returns:
        Items sorted by weighted score descending, optionally truncated.
    """
    if not items:
        return []

    item_list = list(items)

    # Compute raw weighted scores
    raw_scores = [score_fn(item) * weight_fn(item) for item in item_list]

    # Clamp the score ratios
    clamped = _clamp_weights(raw_scores) if any(s > 0 for s in raw_scores) else raw_scores

    # Sort by clamped score descending
    paired = sorted(zip(clamped, range(len(item_list)), item_list), reverse=True)
    ranked = [item for _, _, item in paired]

    if count is not None:
        return ranked[:count]
    return ranked
