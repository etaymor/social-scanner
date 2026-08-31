"""Tests for the city saved-places ranking and export functionality.

Tests verify honest method labeling, window filtering, repeat-save gating,
and Row-safe export functionality for the monthly Tokyo food list.
"""

import csv
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline import db
from pipeline.city_saved_places import (
    METHOD_LIMITATIONS,
    RANK_METHOD,
    SCHEMA_VERSION,
    CityGuideExport,
    DurablePlace,
    PlaceRepeatStats,
    RankedPlace,
    compute_repeat_save_score,
    export_city_guide,
    rank_repeating_saves,
)


def test_schema_and_method_label_are_honest():
    assert SCHEMA_VERSION == 1
    assert "most saved" not in RANK_METHOD.lower()
    assert any("bookmark" in item.lower() or "collection" in item.lower() for item in METHOD_LIMITATIONS)


def test_compute_repeat_save_score():
    """Test the repeat save score formula matches the design."""
    stats = PlaceRepeatStats(
        distinct_posts=3,
        distinct_authors=2,
        total_collect_count=100,
        mention_count=3,
        virality_score=0.1,
    )
    score = compute_repeat_save_score(stats)
    
    # Expected: 2*3 + 3*1 + log1p(100)*0.5 = 6 + 3 + 2.32 = 11.32
    expected = 2 * 3.0 + 3 * 1.0 + math.log1p(100) * 0.5
    assert abs(score - expected) < 0.01


def test_rank_repeating_saves_with_window(conn, city_id):
    """Test that rank_repeating_saves filters by window_days correctly."""
    # Insert test data: 3 places with posts at different dates
    now = datetime.now(timezone.utc)
    old_date = (now - timedelta(days=60)).isoformat()
    recent_date = (now - timedelta(days=15)).isoformat()
    
    # Place 1: Old posts only (outside 30-day window)
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, category, mention_count, virality_score) "
        "VALUES (?, 'Old Cafe', 'cafe', 'food_and_drink', 3, 0.5)",
        (city_id,),
    )
    place1_id = cur.lastrowid
    for i in range(3):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at) "
            "VALUES (?, 'tiktok', ?, ?, 10, ?)",
            (city_id, f"old_{i}", f"author_{i}", old_date),
        )
        post_id = cur.lastrowid
        conn.execute(
            "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
            (place1_id, post_id),
        )
    
    # Place 2: Recent posts (within 30-day window)
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, category, mention_count, virality_score) "
        "VALUES (?, 'Recent Ramen', 'restaurant', 'food_and_drink', 3, 0.8)",
        (city_id,),
    )
    place2_id = cur.lastrowid
    for i in range(3):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at) "
            "VALUES (?, 'tiktok', ?, ?, 20, ?)",
            (city_id, f"recent_{i}", f"author_{i}", recent_date),
        )
        post_id = cur.lastrowid
        conn.execute(
            "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
            (place2_id, post_id),
        )
    
    # Place 3: Only 1 post (should be filtered by min_distinct_posts gate)
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, category, mention_count, virality_score) "
        "VALUES (?, 'Single Post Cafe', 'cafe', 'food_and_drink', 1, 0.3)",
        (city_id,),
    )
    place3_id = cur.lastrowid
    cur = conn.execute(
        "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at) "
        "VALUES (?, 'tiktok', 'single', 'author_x', 50, ?)",
        (city_id, recent_date),
    )
    post_id = cur.lastrowid
    conn.execute(
        "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
        (place3_id, post_id),
    )
    
    conn.commit()
    
    # Get city name
    city_name = conn.execute("SELECT name FROM cities WHERE id = ?", (city_id,)).fetchone()["name"]
    
    # Rank with 30-day window
    result = rank_repeating_saves(city_name, category="food_and_drink", window_days=30, conn=conn)
    
    # Should only have Place 2 (recent posts, meets gates)
    assert len(result.places) == 1
    assert result.places[0].place.name == "Recent Ramen"
    assert result.places[0].stats.distinct_posts == 3
    assert result.places[0].stats.distinct_authors == 3
    assert "last 30 days" in result.method


def test_rank_repeating_saves_gates(conn, city_id):
    """Test author/post gates. Default author gate is 1; pass 2 for strict mode."""
    now = datetime.now(timezone.utc)
    recent_date = (now - timedelta(days=5)).isoformat()

    # Place 1: 3 posts but only 1 author
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, category, mention_count, virality_score) "
        "VALUES (?, 'Single Author Cafe', 'cafe', 'food_and_drink', 3, 0.5)",
        (city_id,),
    )
    place1_id = cur.lastrowid
    for i in range(3):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at) "
            "VALUES (?, 'tiktok', ?, 'same_author', 10, ?)",
            (city_id, f"post_{i}", recent_date),
        )
        post_id = cur.lastrowid
        conn.execute(
            "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
            (place1_id, post_id),
        )

    # Place 2: 3 posts with 3 authors (should always pass)
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, category, mention_count, virality_score) "
        "VALUES (?, 'Multi Author Ramen', 'restaurant', 'food_and_drink', 3, 0.8)",
        (city_id,),
    )
    place2_id = cur.lastrowid
    for i in range(3):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at) "
            "VALUES (?, 'tiktok', ?, ?, 20, ?)",
            (city_id, f"multi_{i}", f"author_{i}", recent_date),
        )
        post_id = cur.lastrowid
        conn.execute(
            "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
            (place2_id, post_id),
        )

    conn.commit()

    city_name = conn.execute("SELECT name FROM cities WHERE id = ?", (city_id,)).fetchone()["name"]

    # Strict independent-author gate (legacy Seoul/Tokyo runs)
    strict = rank_repeating_saves(
        city_name, category="food_and_drink", min_distinct_authors=2, conn=conn
    )
    assert len(strict.places) == 1
    assert strict.places[0].place.name == "Multi Author Ramen"

    # Default gate (min authors=1) keeps real multi-post repeats from one guide
    default = rank_repeating_saves(city_name, category="food_and_drink", conn=conn)
    names = {p.place.name for p in default.places}
    assert names == {"Single Author Cafe", "Multi Author Ramen"}



def test_rank_repeating_saves_excludes_neighborhood_and_street(conn, city_id):
    """Test that neighborhood and street types are excluded from ranking."""
    now = datetime.now(timezone.utc)
    recent_date = (now - timedelta(days=5)).isoformat()
    
    # Create a neighborhood and a street with qualifying posts
    for place_type in ["neighborhood", "street"]:
        cur = conn.execute(
            "INSERT INTO places (city_id, name, type, category, mention_count, virality_score) "
            f"VALUES (?, 'Test {place_type}', ?, 'sights_and_attractions', 3, 0.5)",
            (city_id, place_type),
        )
        place_id = cur.lastrowid
        for i in range(3):
            cur = conn.execute(
                "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at) "
                "VALUES (?, 'tiktok', ?, ?, 10, ?)",
                (city_id, f"{place_type}_{i}", f"author_{i}", recent_date),
            )
            post_id = cur.lastrowid
            conn.execute(
                "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
                (place_id, post_id),
            )
    
    # Create a restaurant that should be included
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, category, mention_count, virality_score) "
        "VALUES (?, 'Test Restaurant', 'restaurant', 'food_and_drink', 3, 0.8)",
        (city_id,),
    )
    place_id = cur.lastrowid
    for i in range(3):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at) "
            "VALUES (?, 'tiktok', ?, ?, 20, ?)",
            (city_id, f"rest_{i}", f"author_{i}", recent_date),
        )
        post_id = cur.lastrowid
        conn.execute(
            "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
            (place_id, post_id),
        )
    
    conn.commit()
    
    city_name = conn.execute("SELECT name FROM cities WHERE id = ?", (city_id,)).fetchone()["name"]
    result = rank_repeating_saves(city_name, conn=conn)
    
    # Should only include the restaurant, not the neighborhood or street
    assert len(result.places) == 1
    assert result.places[0].place.name == "Test Restaurant"


def test_rank_repeating_saves_excludes_tourist_traps(conn, city_id):
    """Test that places marked as is_tourist_trap are excluded from ranking.
    
    This covers:
    - Generic city+cuisine names (Tokyo Sushi, Tokyo Ramen)
    - Places with off-city location tags (Tucson, Aachen, Brisbane)
    - Places already marked is_tourist_trap by filter_generic_and_off_city
    """
    now = datetime.now(timezone.utc)
    recent_date = (now - timedelta(days=5)).isoformat()
    
    # Place 1: Tokyo Sushi (generic name, marked as tourist trap)
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, category, mention_count, virality_score, is_tourist_trap) "
        "VALUES (?, 'Tokyo Sushi', 'restaurant', 'food_and_drink', 5, 0.9, TRUE)",
        (city_id,),
    )
    place1_id = cur.lastrowid
    for i in range(3):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at) "
            "VALUES (?, 'tiktok', ?, ?, 100, ?)",
            (city_id, f"sushi_{i}", f"author_{i}", recent_date),
        )
        post_id = cur.lastrowid
        conn.execute(
            "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
            (place1_id, post_id),
        )
    
    # Place 2: CHERMSIDE SANDWICH (off-city, marked as tourist trap)
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, category, mention_count, virality_score, is_tourist_trap) "
        "VALUES (?, 'CHERMSIDE SANDWICH', 'restaurant', 'food_and_drink', 3, 0.7, TRUE)",
        (city_id,),
    )
    place2_id = cur.lastrowid
    for i in range(3):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at, caption) "
            "VALUES (?, 'tiktok', ?, ?, 50, ?, ?)",
            (city_id, f"chermside_{i}", f"author_{i+10}", recent_date, "📍 Location tag: Chermside, Brisbane"),
        )
        post_id = cur.lastrowid
        conn.execute(
            "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
            (place2_id, post_id),
        )
    
    # Place 3: Real Tokyo venue (should be included)
    cur = conn.execute(
        "INSERT INTO places (city_id, name, type, category, mention_count, virality_score, is_tourist_trap) "
        "VALUES (?, 'Nakiryu Ramen', 'restaurant', 'food_and_drink', 4, 0.6, FALSE)",
        (city_id,),
    )
    place3_id = cur.lastrowid
    for i in range(4):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, posted_at) "
            "VALUES (?, 'tiktok', ?, ?, 30, ?)",
            (city_id, f"nakiryu_{i}", f"author_{i+20}", recent_date),
        )
        post_id = cur.lastrowid
        conn.execute(
            "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
            (place3_id, post_id),
        )
    
    conn.commit()
    
    city_name = conn.execute("SELECT name FROM cities WHERE id = ?", (city_id,)).fetchone()["name"]
    result = rank_repeating_saves(city_name, category="food_and_drink", conn=conn)
    
    # Should only include Place 3 (real Tokyo venue), not the tourist traps
    assert len(result.places) == 1
    assert result.places[0].place.name == "Nakiryu Ramen"
    assert result.places[0].rank == 1
    
    # Verify Tokyo Sushi and CHERMSIDE are NOT in results
    place_names = {p.place.name for p in result.places}
    assert "Tokyo Sushi" not in place_names
    assert "CHERMSIDE SANDWICH" not in place_names


def test_export_city_guide_json(tmp_path: Path):
    """Test JSON export with honest method labels."""
    place = DurablePlace(
        place_id=1,
        name="Test Cafe",
        city="Tokyo",
        category="food_and_drink",
        place_type="cafe",
    )
    stats = PlaceRepeatStats(
        distinct_posts=3,
        distinct_authors=2,
        total_collect_count=100,
        mention_count=3,
        virality_score=0.5,
        sample_urls=("url1", "url2"),
    )
    ranked = RankedPlace(
        rank=1,
        place=place,
        stats=stats,
        repeat_save_score=10.5,
        method=RANK_METHOD.format(city="Tokyo"),
    )
    
    payload = CityGuideExport(
        schema_version=SCHEMA_VERSION,
        city="Tokyo",
        category="food_and_drink",
        method=RANK_METHOD.format(city="Tokyo"),
        method_limitations=METHOD_LIMITATIONS,
        generated_at="2026-08-25T00:00:00+00:00",
        places=(ranked,),
    )
    
    dest = tmp_path / "tokyo.json"
    result = export_city_guide(payload, dest, fmt="json")
    
    assert result == dest
    assert dest.exists()
    
    with open(dest) as f:
        data = json.load(f)
    
    assert data["schema_version"] == 1
    assert data["city"] == "Tokyo"
    assert data["method"] == RANK_METHOD.format(city="Tokyo")
    assert len(data["method_limitations"]) > 0
    assert "bookmark" in " ".join(data["method_limitations"]).lower()
    
    assert len(data["places"]) == 1
    p = data["places"][0]
    assert p["rank"] == 1
    assert p["name"] == "Test Cafe"
    assert p["distinct_posts"] == 3
    assert p["distinct_authors"] == 2
    assert p["repeat_save_score"] == 10.5


def test_export_city_guide_csv(tmp_path: Path):
    """Test CSV export is Row-safe and includes honest method components."""
    place = DurablePlace(
        place_id=1,
        name="Test Ramen",
        city="Tokyo",
        category="food_and_drink",
        place_type="restaurant",
    )
    stats = PlaceRepeatStats(
        distinct_posts=5,
        distinct_authors=4,
        total_collect_count=200,
        mention_count=5,
        virality_score=0.75,
    )
    ranked = RankedPlace(
        rank=1,
        place=place,
        stats=stats,
        repeat_save_score=20.3,
        method=RANK_METHOD.format(city="Tokyo"),
    )
    
    payload = CityGuideExport(
        schema_version=SCHEMA_VERSION,
        city="Tokyo",
        category="food_and_drink",
        method=RANK_METHOD.format(city="Tokyo"),
        method_limitations=METHOD_LIMITATIONS,
        generated_at="2026-08-25T00:00:00+00:00",
        places=(ranked,),
    )
    
    dest = tmp_path / "tokyo.csv"
    result = export_city_guide(payload, dest, fmt="csv")
    
    assert result == dest
    assert dest.exists()
    
    with open(dest, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    assert len(rows) == 1
    row = rows[0]
    assert row["rank"] == "1"
    assert row["name"] == "Test Ramen"
    assert row["place_type"] == "restaurant"
    assert row["category"] == "food_and_drink"
    assert row["distinct_posts"] == "5"
    assert row["distinct_authors"] == "4"
    assert row["total_collect_count"] == "200"
    assert row["repeat_save_score"] == "20.3"
    
    # Verify virality_score is included but NOT used as the rank
    assert row["virality_score"] == "0.75"
