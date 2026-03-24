"""Integration test for the full pipeline with mocked API responses."""

import json
import sqlite3
from unittest.mock import patch, MagicMock

import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db


@pytest.fixture
def conn():
    connection = db.get_connection(":memory:")
    db.init_db(connection)
    yield connection
    connection.close()


@pytest.fixture
def city_id(conn):
    return db.get_or_create_city(conn, "TestCity")


# ---------------------------------------------------------------------------
# DB layer tests
# ---------------------------------------------------------------------------

class TestDatabase:
    def test_create_and_get_city(self, conn):
        cid = db.get_or_create_city(conn, "Istanbul")
        assert cid is not None
        # Getting same city returns same id
        assert db.get_or_create_city(conn, "Istanbul") == cid

    def test_reset_city_cascades(self, conn, city_id):
        db.insert_hashtags(conn, city_id, ["test"])
        db.reset_city(conn, city_id)
        # City should be gone
        row = conn.execute("SELECT COUNT(*) as cnt FROM cities WHERE id = ?",
                           (city_id,)).fetchone()
        assert row["cnt"] == 0
        # Hashtags should cascade-delete
        row = conn.execute("SELECT COUNT(*) as cnt FROM hashtags WHERE city_id = ?",
                           (city_id,)).fetchone()
        assert row["cnt"] == 0

    def test_insert_hashtags_creates_both_platforms(self, conn, city_id):
        db.insert_hashtags(conn, city_id, ["foodie"])
        rows = conn.execute(
            "SELECT * FROM hashtags WHERE city_id = ?", (city_id,),
        ).fetchall()
        assert len(rows) == 2
        platforms = {r["platform"] for r in rows}
        assert platforms == {"tiktok", "instagram"}

    def test_insert_hashtags_idempotent(self, conn, city_id):
        db.insert_hashtags(conn, city_id, ["foodie"])
        db.insert_hashtags(conn, city_id, ["foodie"])
        rows = conn.execute(
            "SELECT * FROM hashtags WHERE city_id = ?", (city_id,),
        ).fetchall()
        assert len(rows) == 2  # Still just 2 (one per platform)

    def test_insert_post_and_link_hashtag(self, conn, city_id):
        db.insert_hashtags(conn, city_id, ["test"])
        hashtag = conn.execute(
            "SELECT id FROM hashtags WHERE city_id = ? AND platform = 'tiktok'",
            (city_id,),
        ).fetchone()

        raw_id = db.insert_post(conn, city_id, "tiktok", {
            "post_id": "abc123",
            "caption": "Great cafe!",
            "likes": 100,
            "views": 5000,
        }, hashtag["id"])
        conn.commit()

        assert raw_id is not None
        post = conn.execute("SELECT * FROM raw_posts WHERE id = ?", (raw_id,)).fetchone()
        assert post["caption"] == "Great cafe!"
        assert post["likes"] == 100

    def test_duplicate_post_links_hashtag(self, conn, city_id):
        db.insert_hashtags(conn, city_id, ["tag1", "tag2"])
        hashtags = conn.execute(
            "SELECT id, tag FROM hashtags WHERE city_id = ? AND platform = 'tiktok'",
            (city_id,),
        ).fetchall()

        post_data = {"post_id": "dup123", "caption": "test"}
        id1 = db.insert_post(conn, city_id, "tiktok", post_data, hashtags[0]["id"])
        id2 = db.insert_post(conn, city_id, "tiktok", post_data, hashtags[1]["id"])
        conn.commit()

        # Both should return the same raw_posts id
        assert id1 == id2

        # Both hashtag links should exist
        links = conn.execute(
            "SELECT * FROM post_hashtags WHERE post_id = ?", (id1,),
        ).fetchall()
        assert len(links) == 2

    def test_upsert_place_increments_mention(self, conn, city_id):
        # Insert a fake post first
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, caption) VALUES (?, 'tiktok', 'p1', 'test')",
            (city_id,),
        )
        pid1 = cur.lastrowid
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, caption) VALUES (?, 'tiktok', 'p2', 'test2')",
            (city_id,),
        )
        pid2 = cur.lastrowid
        conn.commit()

        place_id1 = db.upsert_place(conn, city_id, "Mikla", "restaurant", pid1)
        place_id2 = db.upsert_place(conn, city_id, "Mikla", "restaurant", pid2)
        conn.commit()

        assert place_id1 == place_id2

        place = conn.execute("SELECT * FROM places WHERE id = ?", (place_id1,)).fetchone()
        assert place["mention_count"] == 2

        # Both posts should be linked
        links = db.get_place_post_ids(conn, place_id1)
        assert set(links) == {pid1, pid2}

    def test_merge_places(self, conn, city_id):
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, caption) VALUES (?, 'tiktok', 'p1', 'a')",
            (city_id,),
        )
        pid1 = cur.lastrowid
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, caption) VALUES (?, 'tiktok', 'p2', 'b')",
            (city_id,),
        )
        pid2 = cur.lastrowid
        conn.commit()

        place_a = db.upsert_place(conn, city_id, "Mikla", "restaurant", pid1)
        place_b = db.upsert_place(conn, city_id, "Mikla Restaurant", "restaurant", pid2)
        conn.commit()

        db.merge_places(conn, place_a, [place_b])
        conn.commit()

        # Place B should be deleted
        row = conn.execute("SELECT * FROM places WHERE id = ?", (place_b,)).fetchone()
        assert row is None

        # Place A should have both posts
        post_ids = db.get_place_post_ids(conn, place_a)
        assert set(post_ids) == {pid1, pid2}

        # Place A mention count should include B's
        place = conn.execute("SELECT * FROM places WHERE id = ?", (place_a,)).fetchone()
        assert place["mention_count"] == 2  # 1 original + 1 from merge

    def test_city_stats(self, conn, city_id):
        stats = db.get_city_stats(conn, city_id)
        assert stats["posts"] == 0
        assert stats["hashtags"] == 0
        assert stats["places"] == 0
        assert stats["tourist_traps"] == 0


# ---------------------------------------------------------------------------
# Hashtag generation integration test
# ---------------------------------------------------------------------------

class TestHashtagGeneration:
    @patch("hashtags.call_llm_json")
    def test_generate_hashtags(self, mock_llm, conn, city_id):
        mock_llm.return_value = {
            "hashtags": ["istanbulhidden", "istanbulfood", "istanbulnightlife"]
        }

        from hashtags import generate_hashtags
        tags = generate_hashtags(conn, city_id, "Istanbul")

        # Should have LLM tags + hardcoded (deduped)
        assert "istanbulhidden" in tags
        assert "istanbulhiddengems" in tags  # hardcoded
        assert "istanbullocals" in tags  # hardcoded
        assert len(tags) >= 5  # at least the 5 hardcoded

        # Should be stored in DB
        rows = conn.execute(
            "SELECT DISTINCT tag FROM hashtags WHERE city_id = ?", (city_id,),
        ).fetchall()
        assert len(rows) == len(tags)


# ---------------------------------------------------------------------------
# Extractor integration test
# ---------------------------------------------------------------------------

class TestExtraction:
    @patch("extractor.call_llm_json")
    def test_extract_places_from_posts(self, mock_llm, conn, city_id):
        # Insert some raw posts
        for i in range(3):
            conn.execute(
                """INSERT INTO raw_posts
                   (city_id, platform, post_id, caption, likes, views, processed)
                   VALUES (?, 'tiktok', ?, ?, 100, 5000, FALSE)""",
                (city_id, f"post_{i}", f"Check out Mikla restaurant in Beyoglu! Post {i}"),
            )
        conn.commit()

        mock_llm.return_value = {
            "results": [
                {
                    "caption_index": 1,
                    "places": [{"name": "Mikla", "type": "restaurant"}],
                },
                {
                    "caption_index": 2,
                    "places": [{"name": "Mikla", "type": "restaurant"}],
                },
                {
                    "caption_index": 3,
                    "places": [],
                },
            ]
        }

        from extractor import extract_places
        count = extract_places(conn, city_id, "Istanbul")

        assert count == 2  # Two posts mentioned Mikla

        # Mikla should exist with mention_count = 2
        place = conn.execute(
            "SELECT * FROM places WHERE city_id = ? AND name = 'Mikla'",
            (city_id,),
        ).fetchone()
        assert place is not None
        assert place["mention_count"] == 2
        assert place["type"] == "restaurant"

        # All posts should be marked processed
        unprocessed = db.get_unprocessed_posts(conn, city_id, 100)
        assert len(unprocessed) == 0


# ---------------------------------------------------------------------------
# Filter integration test
# ---------------------------------------------------------------------------

class TestFilter:
    @patch("filter.call_llm_json")
    def test_filter_tourist_traps(self, mock_llm, conn, city_id):
        # Insert places
        conn.execute(
            "INSERT INTO places (city_id, name, type) VALUES (?, 'Grand Bazaar', 'market')",
            (city_id,),
        )
        conn.execute(
            "INSERT INTO places (city_id, name, type) VALUES (?, 'Secret Rooftop Bar', 'bar')",
            (city_id,),
        )
        conn.commit()

        mock_llm.return_value = {
            "results": [
                {"index": 0, "is_tourist_trap": True, "reason": "Every guidebook"},
                {"index": 1, "is_tourist_trap": False, "reason": "Hidden gem"},
            ]
        }

        from filter import filter_tourist_traps
        filter_tourist_traps(conn, city_id, "Istanbul")

        places = conn.execute(
            "SELECT name, is_tourist_trap FROM places WHERE city_id = ? ORDER BY name",
            (city_id,),
        ).fetchall()

        trap_map = {p["name"]: bool(p["is_tourist_trap"]) for p in places}
        assert trap_map["Grand Bazaar"] is True
        assert trap_map["Secret Rooftop Bar"] is False
