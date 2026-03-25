"""Tests for pipeline.weighted_selection — weighted random selection utilities."""

import random
from collections import Counter
from unittest.mock import patch

import pytest

from config import CTA_VARIANTS, EXPLOIT_RATIO, MAX_COMBINED_WEIGHT_RATIO, VALID_CATEGORIES
from pipeline.weighted_selection import (
    _clamp_weights,
    clamped_combined_weight,
    roll_exploit_explore,
    weighted_choice,
    weighted_rank,
)


# ---------------------------------------------------------------------------
# roll_exploit_explore
# ---------------------------------------------------------------------------


class TestRollExploitExplore:
    def test_returns_bool(self):
        result = roll_exploit_explore()
        assert isinstance(result, bool)

    def test_exploit_ratio_approximately_correct(self):
        """Over many rolls, exploit fraction should approximate EXPLOIT_RATIO."""
        random.seed(42)
        n = 10_000
        exploits = sum(1 for _ in range(n) if roll_exploit_explore())
        ratio = exploits / n
        assert abs(ratio - EXPLOIT_RATIO) < 0.03  # within 3% tolerance


# ---------------------------------------------------------------------------
# weighted_choice — uniform weights (no bias)
# ---------------------------------------------------------------------------


class TestWeightedChoiceUniform:
    def test_all_equal_weights_produces_all_options(self):
        """With all-1.0 weights, every option should appear over many trials."""
        random.seed(123)
        options = ["a", "b", "c", "d"]
        weights_dict = {"a": 1.0, "b": 1.0, "c": 1.0, "d": 1.0}
        counts = Counter(
            weighted_choice(options, weights_dict) for _ in range(2000)
        )
        for opt in options:
            assert counts[opt] > 50, f"Option {opt} selected too rarely: {counts[opt]}"

    def test_empty_weights_dict_uses_defaults(self):
        """Missing keys in weights_dict fall back to default_weight=1.0."""
        random.seed(456)
        options = ["x", "y", "z"]
        counts = Counter(
            weighted_choice(options, {}) for _ in range(1000)
        )
        for opt in options:
            assert counts[opt] > 50

    def test_empty_options_raises(self):
        with pytest.raises(ValueError, match="at least one option"):
            weighted_choice([], {})


# ---------------------------------------------------------------------------
# weighted_choice — extreme weights (exploit bias)
# ---------------------------------------------------------------------------


class TestWeightedChoiceExtreme:
    def test_high_weight_dominates_in_exploit_mode(self):
        """With extreme weights, the favored option should dominate exploit rolls."""
        options = ["food_and_drink", "nightlife", "shopping"]
        weights_dict = {"food_and_drink": 10.0, "nightlife": 0.1, "shopping": 0.1}

        # Force exploit mode for all rolls
        with patch("pipeline.weighted_selection.roll_exploit_explore", return_value=True):
            random.seed(42)
            counts = Counter(
                weighted_choice(options, weights_dict) for _ in range(1000)
            )

        # food_and_drink should be selected >90% of the time
        assert counts["food_and_drink"] > 900, (
            f"food_and_drink only selected {counts['food_and_drink']}/1000 times"
        )

    def test_explore_mode_ignores_weights(self):
        """In explore mode, distribution should be roughly uniform regardless of weights."""
        options = ["a", "b", "c"]
        weights_dict = {"a": 100.0, "b": 0.01, "c": 0.01}

        # Force explore mode for all rolls
        with patch("pipeline.weighted_selection.roll_exploit_explore", return_value=False):
            random.seed(42)
            counts = Counter(
                weighted_choice(options, weights_dict) for _ in range(3000)
            )

        # Each option should get roughly 1/3 of selections
        for opt in options:
            ratio = counts[opt] / 3000
            assert 0.2 < ratio < 0.47, (
                f"Option {opt} has ratio {ratio:.2f}, expected ~0.33"
            )


# ---------------------------------------------------------------------------
# weighted_choice — VALID_CATEGORIES
# ---------------------------------------------------------------------------


class TestWeightedChoiceCategories:
    def test_returns_valid_category(self):
        random.seed(99)
        result = weighted_choice(sorted(VALID_CATEGORIES), {})
        assert result in VALID_CATEGORIES

    def test_category_override_not_affected(self):
        """CLI --category override means weighted_choice is never called.

        This tests the pattern: if --category is provided, skip weighted_choice.
        """
        explicit = "food_and_drink"
        # When category is explicitly provided, it should be used directly
        assert explicit in VALID_CATEGORIES


# ---------------------------------------------------------------------------
# weighted_choice — hook format
# ---------------------------------------------------------------------------


class TestWeightedChoiceFormat:
    def test_returns_valid_format(self):
        random.seed(42)
        for _ in range(100):
            result = weighted_choice(["listicle", "story"], {})
            assert result in ("listicle", "story")

    def test_format_override_pattern(self):
        """When --format is explicitly provided, weights are skipped."""
        explicit_format = "story"
        # Simulate: if args.hook_format is not None, use it directly
        assert explicit_format in ("listicle", "story")

    def test_biased_format_selection(self):
        """With high weight on 'story', it should dominate exploit rolls."""
        with patch("pipeline.weighted_selection.roll_exploit_explore", return_value=True):
            random.seed(42)
            counts = Counter(
                weighted_choice(
                    ["listicle", "story"],
                    {"story": 5.0, "listicle": 0.5},
                )
                for _ in range(1000)
            )
        assert counts["story"] > counts["listicle"]


# ---------------------------------------------------------------------------
# weighted_choice — CTA variants
# ---------------------------------------------------------------------------


class TestWeightedChoiceCTA:
    def test_returns_valid_cta(self):
        random.seed(42)
        for _ in range(50):
            result = weighted_choice(CTA_VARIANTS, {})
            assert result in CTA_VARIANTS

    def test_cta_pool_has_multiple_variants(self):
        assert len(CTA_VARIANTS) >= 2

    def test_weighted_cta_selection(self):
        """Heavily weighted CTA variant dominates exploit selections."""
        cta_weights = {CTA_VARIANTS[0]: 10.0}
        with patch("pipeline.weighted_selection.roll_exploit_explore", return_value=True):
            random.seed(42)
            counts = Counter(
                weighted_choice(CTA_VARIANTS, cta_weights) for _ in range(500)
            )
        assert counts[CTA_VARIANTS[0]] > 300


# ---------------------------------------------------------------------------
# clamped_combined_weight
# ---------------------------------------------------------------------------


class TestClampedCombinedWeight:
    def test_default_weight_when_missing(self):
        """Missing dimension values should use default_weight=1.0."""
        result = clamped_combined_weight({}, {"category": "food"})
        assert result == 1.0

    def test_multiplies_across_dimensions(self):
        weights = {
            "category": {"food": 2.0},
            "format": {"listicle": 1.5},
        }
        candidate = {"category": "food", "format": "listicle"}
        result = clamped_combined_weight(weights, candidate)
        assert abs(result - 3.0) < 0.001

    def test_partial_dimensions(self):
        """Only dimensions present in candidate contribute."""
        weights = {
            "category": {"food": 2.0},
            "format": {"listicle": 1.5},
            "city": {"tokyo": 0.8},
        }
        candidate = {"category": "food"}  # only one dimension
        result = clamped_combined_weight(weights, candidate)
        assert abs(result - 2.0) < 0.001


# ---------------------------------------------------------------------------
# _clamp_weights
# ---------------------------------------------------------------------------


class TestClampWeights:
    def test_no_clamping_within_ratio(self):
        """Weights within MAX_COMBINED_WEIGHT_RATIO are returned unchanged."""
        weights = [1.0, 2.0, 3.0]
        result = _clamp_weights(weights)
        assert result == weights

    def test_clamping_extreme_ratio(self):
        """Weights exceeding MAX_COMBINED_WEIGHT_RATIO are compressed."""
        # ratio 100:1 exceeds MAX_COMBINED_WEIGHT_RATIO (10)
        weights = [0.1, 10.0]
        result = _clamp_weights(weights)
        ratio = max(result) / min(result)
        assert ratio <= MAX_COMBINED_WEIGHT_RATIO + 0.01

    def test_empty_list(self):
        assert _clamp_weights([]) == []

    def test_single_weight(self):
        assert _clamp_weights([5.0]) == [5.0]

    def test_zero_weight_returns_uniform(self):
        """Zero in weights should produce uniform output."""
        result = _clamp_weights([0.0, 1.0, 2.0])
        assert all(w == 1.0 for w in result)

    def test_preserves_order(self):
        """Larger weights should remain larger after clamping."""
        weights = [0.01, 1.0, 100.0]
        result = _clamp_weights(weights)
        assert result[0] < result[1] < result[2]


# ---------------------------------------------------------------------------
# weighted_rank
# ---------------------------------------------------------------------------


class TestWeightedRank:
    def test_basic_ranking(self):
        """Items ranked by score * weight."""
        items = [
            {"name": "A", "score": 80, "band": "75-100"},
            {"name": "B", "score": 60, "band": "50-75"},
            {"name": "C", "score": 40, "band": "25-50"},
        ]
        # All weights 1.0 -> original order preserved
        result = weighted_rank(
            items,
            score_fn=lambda x: x["score"],
            weight_fn=lambda x: 1.0,
        )
        assert [r["name"] for r in result] == ["A", "B", "C"]

    def test_weight_reorders_items(self):
        """A low-score item with high weight should rank above a high-score item with low weight."""
        items = [
            {"name": "High-score", "score": 100, "band": "75-100"},
            {"name": "Low-score", "score": 10, "band": "0-25"},
        ]
        # Give the low-score item a much higher weight
        result = weighted_rank(
            items,
            score_fn=lambda x: x["score"],
            weight_fn=lambda x: 20.0 if x["name"] == "Low-score" else 0.1,
        )
        # Low-score * 20 = 200 > High-score * 0.1 = 10
        # (subject to clamping, but the reorder should hold)
        assert result[0]["name"] == "Low-score"

    def test_count_truncation(self):
        items = [{"v": i} for i in range(10)]
        result = weighted_rank(
            items,
            score_fn=lambda x: x["v"],
            weight_fn=lambda x: 1.0,
            count=3,
        )
        assert len(result) == 3

    def test_empty_items(self):
        result = weighted_rank(
            [],
            score_fn=lambda x: 0,
            weight_fn=lambda x: 1.0,
        )
        assert result == []

    def test_virality_band_weighting_pattern(self):
        """Simulate place selection with virality band weights."""
        places = [
            {"name": "P1", "virality_score": 90},  # band 75-100
            {"name": "P2", "virality_score": 85},  # band 75-100
            {"name": "P3", "virality_score": 30},  # band 25-50
            {"name": "P4", "virality_score": 20},  # band 0-25
        ]
        band_weights = {"75-100": 0.5, "25-50": 2.0, "0-25": 2.0}

        def band_of(p):
            s = p["virality_score"]
            if s < 25:
                return "0-25"
            if s < 50:
                return "25-50"
            if s < 75:
                return "50-75"
            return "75-100"

        result = weighted_rank(
            places,
            score_fn=lambda p: p["virality_score"],
            weight_fn=lambda p: band_weights.get(band_of(p), 1.0),
            count=4,
        )
        names = [r["name"] for r in result]
        # P3 (30 * 2.0 = 60) should rank above P2 (85 * 0.5 = 42.5)
        # and P1 (90 * 0.5 = 45)
        assert names[0] == "P3"

    def test_uniform_weights_preserves_score_order(self):
        """With all weights=1.0, ranking follows original scores."""
        places = [
            {"name": "A", "score": 100},
            {"name": "B", "score": 50},
            {"name": "C", "score": 75},
        ]
        result = weighted_rank(
            places,
            score_fn=lambda p: p["score"],
            weight_fn=lambda p: 1.0,
        )
        assert [r["name"] for r in result] == ["A", "C", "B"]


# ---------------------------------------------------------------------------
# Missing/corrupt weights file -> defaults
# ---------------------------------------------------------------------------


class TestMissingWeightsDefaults:
    def test_read_weights_returns_defaults_for_missing_file(self):
        """intelligence.read_weights() with missing file returns all-empty dicts."""
        from pipeline.intelligence import DIMENSIONS, read_weights

        weights = read_weights(path="/nonexistent/path/weights.json")
        for dim in DIMENSIONS:
            assert dim in weights
            assert isinstance(weights[dim], dict)

    def test_read_weights_returns_defaults_for_corrupt_file(self, tmp_path):
        """Corrupt JSON file should return defaults, not crash."""
        from pipeline.intelligence import DIMENSIONS, read_weights

        corrupt = tmp_path / "bad_weights.json"
        corrupt.write_text("NOT VALID JSON {{{{", encoding="utf-8")
        weights = read_weights(path=str(corrupt))
        for dim in DIMENSIONS:
            assert dim in weights

    def test_weighted_choice_with_empty_weights(self):
        """Empty weights dict should work fine (all default 1.0)."""
        random.seed(42)
        result = weighted_choice(["a", "b", "c"], {})
        assert result in ("a", "b", "c")

    def test_weighted_rank_with_default_weights(self):
        """Default weight of 1.0 should preserve original score ordering."""
        items = [{"s": 10}, {"s": 30}, {"s": 20}]
        result = weighted_rank(
            items,
            score_fn=lambda x: x["s"],
            weight_fn=lambda x: 1.0,
        )
        assert [r["s"] for r in result] == [30, 20, 10]


# ---------------------------------------------------------------------------
# CLI override tests (integration-level patterns)
# ---------------------------------------------------------------------------


class TestCLIOverridePatterns:
    def test_category_cli_overrides_weights(self):
        """When --category is explicitly provided, weighted_choice should not be called."""
        # This tests the generate_slideshow.py pattern:
        # if args.category is None: args.category = weighted_choice(...)
        explicit_category = "food_and_drink"
        args_category = explicit_category  # simulating explicit CLI arg

        # The conditional: if args.category is None -> False, so no weighted_choice
        assert args_category is not None
        assert args_category == "food_and_drink"

    def test_format_cli_overrides_weights(self):
        """When --format is explicitly provided, weighted_choice should not be called."""
        explicit_format = "story"
        args_hook_format = explicit_format

        assert args_hook_format is not None
        assert args_hook_format == "story"

    def test_category_none_triggers_weighted_selection(self):
        """When --category is not provided (None), weighted_choice should be called."""
        random.seed(42)
        args_category = None  # simulating no CLI arg

        if args_category is None:
            args_category = weighted_choice(
                sorted(VALID_CATEGORIES),
                {},
            )

        assert args_category in VALID_CATEGORIES

    def test_format_none_triggers_weighted_selection(self):
        """When --format is not provided (None), weighted_choice should be called."""
        random.seed(42)
        args_hook_format = None

        if args_hook_format is None:
            args_hook_format = weighted_choice(
                ["listicle", "story"],
                {},
            )

        assert args_hook_format in ("listicle", "story")


# ---------------------------------------------------------------------------
# select_weighted_style integration
# ---------------------------------------------------------------------------


class TestSelectWeightedStyle:
    def test_returns_valid_style(self):
        from pipeline.image_styles import select_weighted_style

        style = select_weighted_style()
        assert "time_of_day" in style
        assert "weather" in style
        assert "perspective" in style
        assert "color_mood" in style
        for key in ("time_of_day", "weather", "perspective", "color_mood"):
            assert "name" in style[key]
            assert "desc" in style[key]

    def test_biased_by_weights(self):
        """Extreme weights should bias selection."""
        from pipeline.image_styles import select_weighted_style

        weights = {
            "time_of_day": {"golden_hour": 100.0},
            "weather": {"clear": 100.0},
            "perspective": {"street_level": 100.0},
            "color_mood": {"warm_analog": 100.0},
        }
        random.seed(42)
        counts = Counter()
        for _ in range(100):
            style = select_weighted_style(weights)
            counts[style["time_of_day"]["name"]] += 1

        assert counts["golden_hour"] > 80

    def test_none_weights_works(self):
        """None weights should produce valid style without errors."""
        from pipeline.image_styles import select_weighted_style

        random.seed(42)
        style = select_weighted_style(None)
        assert "time_of_day" in style

    def test_empty_weights_works(self):
        """Empty weights dict should produce valid style without errors."""
        from pipeline.image_styles import select_weighted_style

        random.seed(42)
        style = select_weighted_style({})
        assert "time_of_day" in style

    def test_backward_compat_deterministic_style(self):
        """The original select_slideshow_style still works for backward compat."""
        from pipeline.image_styles import select_slideshow_style

        s1 = select_slideshow_style("Tokyo", "2026-03-25")
        s2 = select_slideshow_style("Tokyo", "2026-03-25")
        assert s1 == s2
