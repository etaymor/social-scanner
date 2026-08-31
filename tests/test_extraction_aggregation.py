"""Tests for listicle / on-screen extraction and mention aggregation."""

from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline import db
from pipeline.extractor import _build_numbered_captions, extract_places
from pipeline.listicle import extract_named_places_heuristic
from pipeline.city_saved_places import rank_repeating_saves
from pipeline.replay import import_apify_json
from pipeline.scorer import _find_candidate_pairs


FIXTURE = Path(__file__).parent / "fixtures" / "seoul_apify_sample.json"


def test_listicle_caption_extracts_all_restaurants():
    caption = """
10 RESTAURANTS TO TRY IN SEOUL

1. Isaac Toast — Breakfast Sandwich
2. Mongchon Dakgalbi — Spicy Stir-Fried Chicken
3. Hanmiok — Korean Beef
4. Odarijip Ganjang — Soy Marinated Raw Crab
5. Yoogane — Dakgalbi
6. Ttukbaegi-jip — Traditional Korean Comfort Food
7. Pizzeria O — Authentic Italian Pizza
8. Kyochon Pilbang — Traditional Korean Food
9. Bornga Seolleongtang — Traditional Ox Bone Soup
10. Ilpyeon Sirloin — Korean Beef BBQ
"""
    places = extract_named_places_heuristic(caption)
    names = {p["name"] for p in places}
    assert "Hanmiok" in names
    assert "Odarijip Ganjang" in names
    assert "Ilpyeon Sirloin" in names
    assert "Isaac Toast" in names
    assert len(places) >= 10


def test_onscreen_ocr_block_extracts_venues():
    caption = (
        "Seoul food guide 🍜\n"
        "🔤 On-screen text: 1. Solsot\n2. Cheonsudang\n3. Puddifle Salt Bread"
    )
    places = extract_named_places_heuristic(caption)
    names = {p["name"] for p in places}
    assert "Solsot" in names
    assert "Cheonsudang" in names
    assert "Puddifle Salt Bread" in names


def test_onscreen_and_bullet_formats():
    bullet = (
        "Here are the places I visited:\n\n"
        "Hanmiok Brisket BBQ • HangJungSun • Solsot Pot Rice • Odarijip\n"
    )
    names = {p["name"] for p in extract_named_places_heuristic(bullet)}
    assert "Hanmiok Brisket BBQ" in names
    assert "Odarijip" in names

    decorative = "˖ ࣪⭑ Solsot: with 10+ pot rice flavor options\n˖ ࣪⭑ Oreno Ramen: michelin"
    names2 = {p["name"] for p in extract_named_places_heuristic(decorative)}
    assert "Solsot" in names2
    assert "Oreno Ramen" in names2


def test_alias_shared_token_merges_ilpyeon_variants(conn, city_id):
    for name in (
        "Ilpyeon Sirloin",
        "Korean BBQ Ilpyeon Deungsim Hongdae Main Branch",
        "Salt Bread",
        "Soha Salt Pond",
    ):
        conn.execute(
            "INSERT INTO places (city_id, name, type, mention_count) VALUES (?, ?, 'restaurant', 1)",
            (city_id, name),
        )
    conn.commit()
    places = db.get_all_places(conn, city_id)
    pairs = _find_candidate_pairs(places)
    id_by_name = {p["name"]: p["id"] for p in places}
    pair_set = {frozenset(p) for p in pairs}
    assert (
        frozenset(
            {
                id_by_name["Ilpyeon Sirloin"],
                id_by_name["Korean BBQ Ilpyeon Deungsim Hongdae Main Branch"],
            }
        )
        in pair_set
    )
    assert frozenset({id_by_name["Salt Bread"], id_by_name["Soha Salt Pond"]}) not in pair_set



def test_build_numbered_captions_keeps_long_listicle(conn, city_id):
    """Regression: 500-char truncation dropped listicle tails (Ilpyeon etc.)."""
    long = "10 RESTAURANTS TO TRY IN SEOUL\n\n" + "\n\n".join(
        f"{i}. Very Long Restaurant Name Number {i} — Traditional Korean Food Specialty Dish"
        for i in range(1, 20)
    )
    assert len(long) > 500
    conn.execute(
        "INSERT INTO raw_posts (city_id, platform, post_id, caption) VALUES (?, 'tiktok', '1', ?)",
        (city_id, long),
    )
    conn.commit()
    posts = conn.execute("SELECT * FROM raw_posts").fetchall()
    text, mapping = _build_numbered_captions(posts)
    assert "Very Long Restaurant Name Number 19" in text
    assert "Very Long Restaurant Name Number 1" in text
    assert len(mapping) == 1
    # Ensure we kept more than the old 500-char budget
    assert len(text) > 500


def test_ten_posts_count_as_ten_mentions(conn, city_id):
    """Same restaurant across 10 posts → mention_count 10 and ranker distinct_posts 10."""
    for i in range(10):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, author, saves, caption, posted_at) "
            "VALUES (?, 'tiktok', ?, ?, 100, '1. Hanmiok — BBQ', datetime('now'))",
            (city_id, f"p{i}", f"author_{i}"),
        )
        db.upsert_place(conn, city_id, "Hanmiok", "restaurant", cur.lastrowid, category="food_and_drink")
    conn.commit()

    place = conn.execute(
        "SELECT * FROM places WHERE city_id = ? AND name = 'Hanmiok'", (city_id,)
    ).fetchone()
    assert place["mention_count"] == 10

    city = conn.execute("SELECT name FROM cities WHERE id = ?", (city_id,)).fetchone()["name"]
    ranked = rank_repeating_saves(city, category="food_and_drink", conn=conn)
    assert len(ranked.places) == 1
    assert ranked.places[0].stats.distinct_posts == 10
    assert ranked.places[0].stats.distinct_authors == 10


def test_alias_containment_does_not_merge_unrelated_salt_names(conn, city_id):
    """Salt Bread vs Soha Salt Pond must not become fuzzy containment candidates."""
    for name in ("Salt Bread", "Soha Salt Pond", "Cheonsudang"):
        conn.execute(
            "INSERT INTO places (city_id, name, type, mention_count) VALUES (?, ?, 'bakery', 1)",
            (city_id, name),
        )
    conn.commit()
    places = db.get_all_places(conn, city_id)
    pairs = _find_candidate_pairs(places)
    id_by_name = {p["name"]: p["id"] for p in places}
    pair_set = {frozenset(p) for p in pairs}
    assert frozenset({id_by_name["Salt Bread"], id_by_name["Soha Salt Pond"]}) not in pair_set


def test_heuristic_extract_from_slideshow_style_caption(conn, city_id):
    caption = (
        "Top Seoul eats\n"
        "🔤 On-screen text:\n"
        "1. Solsot\n"
        "2. Hanmiok\n"
        "3. Odarijip Ganjang\n"
        "🎙 Subtitles: Next up we have Solsot for salt bread"
    )
    conn.execute(
        "INSERT INTO raw_posts (city_id, platform, post_id, caption, author, saves) "
        "VALUES (?, 'tiktok', 's1', ?, 'guide1', 50)",
        (city_id, caption),
    )
    conn.commit()
    count = extract_places(conn, city_id, "Seoul", heuristic_only=True)
    assert count >= 3
    names = {
        r["name"]
        for r in conn.execute("SELECT name FROM places WHERE city_id = ?", (city_id,)).fetchall()
    }
    assert "Solsot" in names
    assert "Hanmiok" in names
    assert "Odarijip Ganjang" in names


@pytest.mark.skipif(not FIXTURE.exists(), reason="Seoul fixture missing")
def test_replay_seoul_fixture_inserts_posts(conn, city_id):
    # Rename city for realism
    conn.execute("UPDATE cities SET name = 'Seoul' WHERE id = ?", (city_id,))
    conn.commit()
    inserted = import_apify_json(conn, city_id, FIXTURE, window_days=None)
    assert inserted >= 5
    posts = conn.execute(
        "SELECT COUNT(*) AS c FROM raw_posts WHERE city_id = ?", (city_id,)
    ).fetchone()["c"]
    assert posts == inserted


def test_ocr_fail_closed(monkeypatch, conn, city_id):
    from pipeline import ocr

    conn.execute(
        "INSERT INTO raw_posts (city_id, platform, post_id, cover_url, caption, ocr_status) "
        "VALUES (?, 'tiktok', ?, 'http://example.com/x.jpg', 'hi', 'pending')",
        (city_id, "o1"),
    )
    # Need enough attempts — insert 5
    for i in range(2, 7):
        conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, cover_url, caption, ocr_status) "
            "VALUES (?, 'tiktok', ?, 'http://example.com/x.jpg', 'hi', 'pending')",
            (city_id, f"o{i}"),
        )
    conn.commit()

    monkeypatch.setattr(ocr, "_process_one", lambda pid, urls: ocr._OCRAttempt(pid, http_error=True))

    with pytest.raises(ocr.OCRError):
        ocr.extract_cover_text(conn, city_id, "Seoul", fail_closed=True)
