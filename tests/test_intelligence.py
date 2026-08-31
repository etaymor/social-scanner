"""Tests for pipeline.intelligence — weight computation, decision rules, I/O."""

import json
import math
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest

from pipeline import db
from pipeline.intelligence import (
    DIMENSIONS,
    _parse_visual_style,
    _virality_band,
    apply_decision_rules,
    check_circuit_breaker,
    compute_dimension_weights,
    evaluate_slideshows,
    read_weights,
    write_weights,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _ts(hours_ago: float = 0) -> str:
    """Return an ISO-ish timestamp *hours_ago* in the past."""
    dt = datetime.now(UTC) - timedelta(hours=hours_ago)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _insert_slideshow(
    conn: sqlite3.Connection,
    city_id: int,
    *,
    category: str = "food_and_drink",
    fmt: str = "listicle",
    posted_hours_ago: float = 72,
    visual_style: str | None = None,
    cta_text: str | None = None,
    publish_status: str = "published",
) -> int:
    """Insert a slideshow and return its id."""
    posted_at = _ts(posted_hours_ago)
    cur = conn.execute(
        """INSERT INTO slideshows
           (city_id, category, format, hook_text, slide_count, output_dir,
            posted_at, publish_status, visual_style, cta_text)
           VALUES (?, ?, ?, 'hook', 5, '/tmp/out', ?, ?, ?, ?)""",
        (city_id, category, fmt, posted_at, publish_status, visual_style, cta_text),
    )
    return cur.lastrowid


def _insert_analytics(
    conn: sqlite3.Connection,
    slideshow_id: int,
    views: int,
    *,
    fetched_hours_ago: float | None = None,
    likes: int = 0,
    comments: int = 0,
    shares: int = 0,
    saves: int = 0,
    views_estimated: bool = False,
) -> None:
    """Insert a slideshow_analytics row."""
    fetched_at = _ts(fetched_hours_ago) if fetched_hours_ago is not None else _ts(0)
    conn.execute(
        """INSERT INTO slideshow_analytics
           (slideshow_id, fetched_at, views, likes, comments, shares, saves, views_estimated)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (slideshow_id, fetched_at, views, likes, comments, shares, saves, views_estimated),
    )


def _insert_performance(
    conn: sqlite3.Connection,
    slideshow_id: int,
    *,
    composite_score: float = 1.0,
    views_at_48h: int = 5000,
    views_confidence: float = 1.0,
    decision_tag: str = "keep",
) -> int:
    """Insert a slideshow_performance row and return its id."""
    cur = conn.execute(
        """INSERT INTO slideshow_performance
           (slideshow_id, evaluated_at, views_at_48h, views_latest,
            composite_score, views_confidence, decision_tag)
           VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)""",
        (slideshow_id, views_at_48h, views_at_48h, composite_score, views_confidence, decision_tag),
    )
    return cur.lastrowid


_place_counter = 0


def _insert_place(conn: sqlite3.Connection, city_id: int, *, virality_score: float = 50.0) -> int:
    global _place_counter
    _place_counter += 1
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, virality_score) VALUES (?, ?, 'restaurant', ?)",
        (city_id, f"Place-{_place_counter}", virality_score),
    )
    return cur.lastrowid


def _link_place(conn: sqlite3.Connection, slideshow_id: int, place_id: int, slide: int = 1) -> None:
    conn.execute(
        "INSERT INTO slideshow_places (slideshow_id, place_id, slide_number) VALUES (?, ?, ?)",
        (slideshow_id, place_id, slide),
    )


# ---------------------------------------------------------------------------
# cold start: empty database -> all-1.0 weights
# ---------------------------------------------------------------------------


class TestColdStart:
    def test_empty_db_produces_all_one_weights(self, conn, city_id):
        weights = compute_dimension_weights(conn)
        assert set(weights.keys()) == set(DIMENSIONS)
        for dim in DIMENSIONS:
            assert weights[dim] == {}

    def test_fewer_than_three_posts_stay_at_one(self, conn, city_id):
        vs = json.dumps({"time_of_day": "golden_hour", "weather": "clear",
                         "perspective": "street_level", "color_mood": "warm_analog"})
        for _ in range(2):
            sid = _insert_slideshow(conn, city_id, visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 10000)
            _insert_performance(conn, sid, composite_score=5.0)
            place_id = _insert_place(conn, city_id)
            _link_place(conn, sid, place_id)
        conn.commit()

        weights = compute_dimension_weights(conn)
        # category "food_and_drink" has 2 posts (< MIN_POSTS_FOR_WEIGHT=3)
        assert weights["category"].get("food_and_drink", 1.0) == 1.0


# ---------------------------------------------------------------------------
# weights diverge with different performance
# ---------------------------------------------------------------------------


class TestWeightDivergence:
    def test_categories_with_different_views_diverge(self, conn, city_id):
        vs = json.dumps({"time_of_day": "golden_hour", "weather": "clear",
                         "perspective": "street_level", "color_mood": "warm_analog"})
        # High-performing category
        for _ in range(5):
            sid = _insert_slideshow(conn, city_id, category="food_and_drink",
                                    visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 20000)
            _insert_performance(conn, sid, composite_score=4.0, views_at_48h=20000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)

        # Low-performing category
        for _ in range(5):
            sid = _insert_slideshow(conn, city_id, category="nightlife",
                                    visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 2000)
            _insert_performance(conn, sid, composite_score=0.5, views_at_48h=2000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)
        conn.commit()

        weights = compute_dimension_weights(conn)
        w_food = weights["category"].get("food_and_drink", 1.0)
        w_night = weights["category"].get("nightlife", 1.0)
        assert w_food > w_night, f"food={w_food}, nightlife={w_night}"


# ---------------------------------------------------------------------------
# Bayesian smoothing
# ---------------------------------------------------------------------------


class TestBayesianSmoothing:
    def test_single_outlier_dampened(self, conn, city_id):
        """One post with extreme score should NOT produce weight near MAX."""
        vs = json.dumps({"time_of_day": "golden_hour", "weather": "clear",
                         "perspective": "street_level", "color_mood": "warm_analog"})
        # 3 posts for category A with extreme score
        for _ in range(3):
            sid = _insert_slideshow(conn, city_id, category="food_and_drink",
                                    visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 100000)
            _insert_performance(conn, sid, composite_score=20.0, views_at_48h=100000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)

        # 3 posts for category B with average score (to have an overall avg)
        for _ in range(3):
            sid = _insert_slideshow(conn, city_id, category="nightlife",
                                    visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 5000)
            _insert_performance(conn, sid, composite_score=1.0, views_at_48h=5000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)
        conn.commit()

        weights = compute_dimension_weights(conn)
        w = weights["category"].get("food_and_drink", 1.0)
        # With Bayesian smoothing + clamp, should be <= MAX_WEIGHT
        assert w <= 2.0, f"weight={w} exceeds MAX_WEIGHT"


# ---------------------------------------------------------------------------
# per-dimension clamp
# ---------------------------------------------------------------------------


class TestClamp:
    def test_no_weight_below_min(self, conn, city_id):
        vs = json.dumps({"time_of_day": "golden_hour", "weather": "clear",
                         "perspective": "street_level", "color_mood": "warm_analog"})
        for _ in range(5):
            sid = _insert_slideshow(conn, city_id, category="food_and_drink",
                                    visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 100000)
            _insert_performance(conn, sid, composite_score=10.0, views_at_48h=100000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)
        for _ in range(5):
            sid = _insert_slideshow(conn, city_id, category="nightlife",
                                    visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 100)
            _insert_performance(conn, sid, composite_score=0.01, views_at_48h=100)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)
        conn.commit()

        weights = compute_dimension_weights(conn)
        for dim in DIMENSIONS:
            for val, w in weights[dim].items():
                assert w >= 0.5, f"{dim}/{val} = {w} < MIN_WEIGHT"
                assert w <= 2.0, f"{dim}/{val} = {w} > MAX_WEIGHT"


# ---------------------------------------------------------------------------
# rate limiter
# ---------------------------------------------------------------------------


class TestRateLimiter:
    def test_weight_change_capped_at_20pct(self, conn, city_id):
        vs = json.dumps({"time_of_day": "golden_hour", "weather": "clear",
                         "perspective": "street_level", "color_mood": "warm_analog"})
        for _ in range(5):
            sid = _insert_slideshow(conn, city_id, category="food_and_drink",
                                    visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 100000)
            _insert_performance(conn, sid, composite_score=10.0, views_at_48h=100000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)
        for _ in range(5):
            sid = _insert_slideshow(conn, city_id, category="nightlife",
                                    visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 100)
            _insert_performance(conn, sid, composite_score=0.01, views_at_48h=100)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)
        conn.commit()

        # Previous weights all at 1.0
        prev = {d: {} for d in DIMENSIONS}
        prev["category"]["food_and_drink"] = 1.0
        prev["category"]["nightlife"] = 1.0

        weights = compute_dimension_weights(conn, previous_weights=prev)
        for dim in DIMENSIONS:
            prev_dim = prev.get(dim, {})
            for val, w in weights[dim].items():
                prev_val = prev_dim.get(val, 1.0)
                delta = abs(w - prev_val)
                assert delta <= 0.2 + 1e-9, (
                    f"{dim}/{val}: delta={delta:.4f} > 0.2 "
                    f"(prev={prev_val}, new={w})"
                )


# ---------------------------------------------------------------------------
# exponential decay
# ---------------------------------------------------------------------------


class TestExponentialDecay:
    def test_old_posts_contribute_less(self, conn, city_id):
        vs = json.dumps({"time_of_day": "golden_hour", "weather": "clear",
                         "perspective": "street_level", "color_mood": "warm_analog"})
        # Recent high-performing posts
        for _ in range(4):
            sid = _insert_slideshow(conn, city_id, category="food_and_drink",
                                    visual_style=vs, posted_hours_ago=50)
            _insert_analytics(conn, sid, 20000)
            _insert_performance(conn, sid, composite_score=4.0, views_at_48h=20000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)

        # Old low-performing posts (28 days old = 672 hours)
        for _ in range(4):
            sid = _insert_slideshow(conn, city_id, category="food_and_drink",
                                    visual_style=vs, posted_hours_ago=672)
            _insert_analytics(conn, sid, 1000)
            _insert_performance(conn, sid, composite_score=0.2, views_at_48h=1000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)

        # Baseline recent posts for comparison
        for _ in range(4):
            sid = _insert_slideshow(conn, city_id, category="nightlife",
                                    visual_style=vs, posted_hours_ago=50)
            _insert_analytics(conn, sid, 5000)
            _insert_performance(conn, sid, composite_score=1.0, views_at_48h=5000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)
        conn.commit()

        weights = compute_dimension_weights(conn)
        w_food = weights["category"].get("food_and_drink", 1.0)
        # The recent high-performing posts should outweigh the old low ones,
        # so food_and_drink should be above baseline
        assert w_food > 1.0, f"Expected >1.0 but got {w_food}"


# ---------------------------------------------------------------------------
# composite score blending
# ---------------------------------------------------------------------------


class TestCompositeScore:
    def test_alpha_one_is_views_only(self, conn, city_id):
        """When RC not configured, alpha=1.0 -> views-only composite."""
        with mock.patch("pipeline.intelligence.config.REVENUECAT_V2_SECRET_KEY", ""):
            from pipeline.intelligence import _get_alpha
            assert _get_alpha() == 1.0

    def test_alpha_point_six_with_rc(self):
        with mock.patch("pipeline.intelligence.config.REVENUECAT_V2_SECRET_KEY", "sk_test"):
            from pipeline.intelligence import _get_alpha
            assert _get_alpha() == 0.6


# ---------------------------------------------------------------------------
# posts younger than 48 h excluded
# ---------------------------------------------------------------------------


class TestMaturation:
    def test_young_posts_excluded(self, conn, city_id):
        """Posts posted < 48h ago should NOT appear in weight computation."""
        vs = json.dumps({"time_of_day": "golden_hour", "weather": "clear",
                         "perspective": "street_level", "color_mood": "warm_analog"})
        # Young post (24 h ago — not matured)
        sid = _insert_slideshow(conn, city_id, posted_hours_ago=24, visual_style=vs)
        _insert_analytics(conn, sid, 100000)
        _insert_performance(conn, sid, composite_score=50.0, views_at_48h=100000)
        p = _insert_place(conn, city_id)
        _link_place(conn, sid, p)
        conn.commit()

        weights = compute_dimension_weights(conn)
        # Should not have any weight entries since the only post is too young
        assert weights["category"].get("food_and_drink", 1.0) == 1.0


# ---------------------------------------------------------------------------
# circuit breaker
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    def test_triggers_when_7d_below_50pct_of_30d(self, conn, city_id):
        # 30-day posts with good views
        for i in range(5):
            sid = _insert_slideshow(conn, city_id, posted_hours_ago=20 * 24)
            _insert_analytics(conn, sid, 20000, fetched_hours_ago=20 * 24)
        # 7-day posts with terrible views
        for i in range(5):
            sid = _insert_slideshow(conn, city_id, posted_hours_ago=3 * 24)
            _insert_analytics(conn, sid, 500, fetched_hours_ago=3 * 24)
        conn.commit()

        assert check_circuit_breaker(conn) is True

    def test_does_not_trigger_when_healthy(self, conn, city_id):
        for i in range(5):
            sid = _insert_slideshow(conn, city_id, posted_hours_ago=3 * 24)
            _insert_analytics(conn, sid, 10000, fetched_hours_ago=3 * 24)
        for i in range(5):
            sid = _insert_slideshow(conn, city_id, posted_hours_ago=20 * 24)
            _insert_analytics(conn, sid, 10000, fetched_hours_ago=20 * 24)
        conn.commit()

        assert check_circuit_breaker(conn) is False

    def test_no_data_does_not_trigger(self, conn, city_id):
        assert check_circuit_breaker(conn) is False


# ---------------------------------------------------------------------------
# decision rules
# ---------------------------------------------------------------------------


class TestDecisionRules:
    def test_scale(self, conn, city_id):
        sid = _insert_slideshow(conn, city_id)
        conn.commit()
        assert apply_decision_rules(60000, conn, sid) == "scale"

    def test_keep(self, conn, city_id):
        sid = _insert_slideshow(conn, city_id)
        conn.commit()
        assert apply_decision_rules(15000, conn, sid) == "keep"

    def test_test_tag(self, conn, city_id):
        sid = _insert_slideshow(conn, city_id)
        conn.commit()
        assert apply_decision_rules(5000, conn, sid) == "test"

    def test_first_sub_1k_is_test(self, conn, city_id):
        sid = _insert_slideshow(conn, city_id)
        conn.commit()
        assert apply_decision_rules(500, conn, sid) == "test"

    def test_second_sub_1k_same_dims_is_drop(self, conn, city_id):
        # First slideshow with <1K already evaluated
        sid1 = _insert_slideshow(conn, city_id, category="food_and_drink", fmt="listicle")
        _insert_performance(conn, sid1, views_at_48h=400, decision_tag="test")

        # Second slideshow with same dims
        sid2 = _insert_slideshow(conn, city_id, category="food_and_drink", fmt="listicle")
        conn.commit()

        assert apply_decision_rules(300, conn, sid2) == "drop"

    def test_sub_1k_different_dims_is_test(self, conn, city_id):
        # First slideshow with <1K in different category
        sid1 = _insert_slideshow(conn, city_id, category="nightlife", fmt="listicle")
        _insert_performance(conn, sid1, views_at_48h=400, decision_tag="test")

        # Second slideshow in food_and_drink
        sid2 = _insert_slideshow(conn, city_id, category="food_and_drink", fmt="listicle")
        conn.commit()

        assert apply_decision_rules(300, conn, sid2) == "test"


# ---------------------------------------------------------------------------
# visual style decomposition into 4 sub-dimensions
# ---------------------------------------------------------------------------


class TestVisualStyleDecomposition:
    def test_parses_full_style_json(self):
        raw = json.dumps({
            "time_of_day": {"name": "golden_hour", "desc": "warm light"},
            "weather": {"name": "clear", "desc": "blue sky"},
            "perspective": {"name": "street_level", "desc": "low angle"},
            "color_mood": {"name": "warm_analog", "desc": "film tones"},
        })
        result = _parse_visual_style(raw)
        assert result == {
            "time_of_day": "golden_hour",
            "weather": "clear",
            "perspective": "street_level",
            "color_mood": "warm_analog",
        }

    def test_parses_flat_string_values(self):
        raw = json.dumps({
            "time_of_day": "golden_hour",
            "weather": "clear",
            "perspective": "street_level",
            "color_mood": "warm_analog",
        })
        result = _parse_visual_style(raw)
        assert result["time_of_day"] == "golden_hour"

    def test_returns_empty_for_none(self):
        assert _parse_visual_style(None) == {}

    def test_returns_empty_for_invalid_json(self):
        assert _parse_visual_style("{bad json") == {}

    def test_four_style_dimensions_in_weights(self, conn, city_id):
        vs = json.dumps({
            "time_of_day": "golden_hour",
            "weather": "clear",
            "perspective": "street_level",
            "color_mood": "warm_analog",
        })
        for _ in range(4):
            sid = _insert_slideshow(conn, city_id, visual_style=vs, posted_hours_ago=72)
            _insert_analytics(conn, sid, 10000)
            _insert_performance(conn, sid, composite_score=2.0, views_at_48h=10000)
            p = _insert_place(conn, city_id)
            _link_place(conn, sid, p)
        conn.commit()

        weights = compute_dimension_weights(conn)
        for key in ("time_of_day", "weather", "perspective", "color_mood"):
            assert key in weights, f"Missing dimension: {key}"


# ---------------------------------------------------------------------------
# 9 dimension keys in output
# ---------------------------------------------------------------------------


class TestDimensionKeys:
    def test_nine_dimensions_present(self, conn, city_id):
        weights = compute_dimension_weights(conn)
        assert set(weights.keys()) == set(DIMENSIONS)
        assert len(DIMENSIONS) == 9


# ---------------------------------------------------------------------------
# atomic write: tmp + rename
# ---------------------------------------------------------------------------


class TestAtomicWrite:
    def test_write_and_read_roundtrip(self, tmp_path):
        target = tmp_path / "perf_weights.json"
        data = {"category": {"food_and_drink": 1.5}}
        write_weights(data, path=target, post_count=10)

        assert target.exists()
        loaded = json.loads(target.read_text())
        assert loaded["category"]["food_and_drink"] == 1.5
        assert "_meta" in loaded
        assert loaded["_meta"]["post_count"] == 10

    def test_tmp_file_cleaned_up(self, tmp_path):
        target = tmp_path / "perf_weights.json"
        write_weights({"category": {}}, path=target)
        tmp_file = target.with_suffix(".tmp")
        assert not tmp_file.exists()


# ---------------------------------------------------------------------------
# read_weights returns defaults for missing / corrupt file
# ---------------------------------------------------------------------------


class TestReadWeights:
    def test_missing_file_returns_defaults(self, tmp_path):
        result = read_weights(path=tmp_path / "nonexistent.json")
        assert set(result.keys()) == set(DIMENSIONS)
        for dim in DIMENSIONS:
            assert result[dim] == {}

    def test_corrupt_file_returns_defaults(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("{not valid json!!!")
        result = read_weights(path=bad)
        assert set(result.keys()) == set(DIMENSIONS)

    def test_valid_file_loads_correctly(self, tmp_path):
        target = tmp_path / "weights.json"
        data = {
            "_meta": {"updated_at": "2026-01-01T00:00:00"},
            "category": {"food_and_drink": 1.3},
            "city": {},
            "format": {},
            "time_of_day": {},
            "weather": {},
            "perspective": {},
            "color_mood": {},
            "cta": {},
            "virality_band": {},
        }
        target.write_text(json.dumps(data))
        result = read_weights(path=target)
        assert result["category"]["food_and_drink"] == 1.3


# ---------------------------------------------------------------------------
# empty database produces all-1.0 weights without error
# ---------------------------------------------------------------------------


class TestEmptyDatabase:
    def test_no_slideshows_no_crash(self, conn, city_id):
        weights = compute_dimension_weights(conn)
        for dim in DIMENSIONS:
            for val, w in weights[dim].items():
                assert w == 1.0

    def test_evaluate_empty_returns_zero(self, conn, city_id):
        assert evaluate_slideshows(conn) == 0


# ---------------------------------------------------------------------------
# stale drafts excluded
# ---------------------------------------------------------------------------


class TestStaleDrafts:
    def test_stale_excluded_from_weights(self, conn, city_id):
        vs = json.dumps({"time_of_day": "golden_hour", "weather": "clear",
                         "perspective": "street_level", "color_mood": "warm_analog"})
        # Stale slideshow
        sid = _insert_slideshow(conn, city_id, publish_status="stale",
                                visual_style=vs, posted_hours_ago=72)
        _insert_analytics(conn, sid, 100000)
        _insert_performance(conn, sid, composite_score=50.0, views_at_48h=100000)
        p = _insert_place(conn, city_id)
        _link_place(conn, sid, p)
        conn.commit()

        weights = compute_dimension_weights(conn)
        # Should not contribute
        assert weights["category"].get("food_and_drink", 1.0) == 1.0

    def test_stale_excluded_from_evaluation(self, conn, city_id):
        sid = _insert_slideshow(conn, city_id, publish_status="stale", posted_hours_ago=72)
        _insert_analytics(conn, sid, 5000)
        conn.commit()

        count = evaluate_slideshows(conn)
        assert count == 0


# ---------------------------------------------------------------------------
# evaluate_slideshows
# ---------------------------------------------------------------------------


class TestEvaluateSlideshows:
    def test_evaluates_matured_slideshow(self, conn, city_id):
        sid = _insert_slideshow(conn, city_id, posted_hours_ago=72)
        _insert_analytics(conn, sid, 15000)
        conn.commit()

        count = evaluate_slideshows(conn)
        assert count == 1

        perf = conn.execute(
            "SELECT * FROM slideshow_performance WHERE slideshow_id = ?", (sid,)
        ).fetchone()
        assert perf is not None
        assert perf["views_at_48h"] == 15000
        assert perf["decision_tag"] == "keep"

    def test_does_not_reevaluate(self, conn, city_id):
        sid = _insert_slideshow(conn, city_id, posted_hours_ago=72)
        _insert_analytics(conn, sid, 15000)
        conn.commit()

        assert evaluate_slideshows(conn) == 1
        conn.commit()
        assert evaluate_slideshows(conn) == 0

    def test_skips_unmatured(self, conn, city_id):
        sid = _insert_slideshow(conn, city_id, posted_hours_ago=24)
        _insert_analytics(conn, sid, 15000)
        conn.commit()

        assert evaluate_slideshows(conn) == 0


# ---------------------------------------------------------------------------
# virality band bucketing
# ---------------------------------------------------------------------------


class TestViralityBand:
    def test_bands(self):
        assert _virality_band(0) == "0-25"
        assert _virality_band(10) == "0-25"
        assert _virality_band(25) == "25-50"
        assert _virality_band(49.9) == "25-50"
        assert _virality_band(50) == "50-75"
        assert _virality_band(74.9) == "50-75"
        assert _virality_band(75) == "75-100"
        assert _virality_band(100) == "75-100"


# ---------------------------------------------------------------------------
# write_weights metadata
# ---------------------------------------------------------------------------


class TestWriteWeightsMeta:
    def test_circuit_breaker_flag(self, tmp_path):
        target = tmp_path / "w.json"
        write_weights({"category": {}}, path=target, circuit_breaker=True)
        data = json.loads(target.read_text())
        assert data["_meta"]["circuit_breaker"] is True

    def test_updated_at_is_iso(self, tmp_path):
        target = tmp_path / "w.json"
        write_weights({"category": {}}, path=target)
        data = json.loads(target.read_text())
        # Should parse as ISO
        datetime.fromisoformat(data["_meta"]["updated_at"])
