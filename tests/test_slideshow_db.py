"""Tests for slideshow database schema extensions and query helpers."""

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipeline import db
import config


# ---------------------------------------------------------------------------
# Helpers to insert test data
# ---------------------------------------------------------------------------

def _insert_place(conn, city_id, name, virality_score=10.0, is_tourist_trap=False,
                  category=None):
    """Insert a place directly and return its id."""
    cur = conn.execute(
        """INSERT INTO places (city_id, name, type, virality_score, is_tourist_trap, category)
           VALUES (?, ?, 'restaurant', ?, ?, ?)""",
        (city_id, name, virality_score, is_tourist_trap, category),
    )
    conn.commit()
    return cur.lastrowid


def _insert_slideshow_with_place(conn, city_id, place_id, created_at=None):
    """Create a slideshow and link a place to it. Optionally backdate created_at."""
    cur = conn.execute(
        """INSERT INTO slideshows (city_id, format, hook_text, slide_count, output_dir)
           VALUES (?, 'listicle', 'hook', 5, '/tmp/slides')""",
        (city_id,),
    )
    slideshow_id = cur.lastrowid
    conn.execute(
        "INSERT INTO slideshow_places (slideshow_id, place_id, slide_number) VALUES (?, ?, 1)",
        (slideshow_id, place_id),
    )
    if created_at is not None:
        conn.execute(
            "UPDATE slideshows SET created_at = ? WHERE id = ?",
            (created_at, slideshow_id),
        )
    conn.commit()
    return slideshow_id


# ---------------------------------------------------------------------------
# Schema and migration tests
# ---------------------------------------------------------------------------

class TestSlideshowSchema:
    def test_init_db_creates_slideshows_table(self, conn):
        """init_db should create the slideshows table."""
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='slideshows'",
        ).fetchall()
        assert len(tables) == 1

    def test_init_db_creates_slideshow_places_table(self, conn):
        """init_db should create the slideshow_places table."""
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='slideshow_places'",
        ).fetchall()
        assert len(tables) == 1

    def test_init_db_idempotent(self, conn):
        """Calling init_db twice should not raise errors."""
        db.init_db(conn)
        db.init_db(conn)

        # Tables still exist with correct structure
        tables = {
            r["name"] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
            ).fetchall()
        }
        assert "slideshows" in tables
        assert "slideshow_places" in tables

    def test_slideshows_format_check_constraint(self, conn, city_id):
        """slideshows.format should only allow 'listicle' or 'story'."""
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO slideshows (city_id, format, hook_text, slide_count, output_dir)
                   VALUES (?, 'invalid', 'hook', 5, '/tmp')""",
                (city_id,),
            )

    def test_slideshow_places_primary_key(self, conn, city_id):
        """slideshow_places should enforce composite primary key (slideshow_id, place_id)."""
        place_id = _insert_place(conn, city_id, "Dup Place")
        sid = db.create_slideshow(conn, city_id, None, "listicle", "hook", 5, "/tmp")
        db.add_slideshow_place(conn, sid, place_id, 1)
        with pytest.raises(sqlite3.IntegrityError):
            db.add_slideshow_place(conn, sid, place_id, 2)


class TestNeighborhoodImagePromptColumns:
    def test_neighborhood_column_exists(self, conn):
        """places table should have a neighborhood column after init_db."""
        cols = conn.execute("PRAGMA table_info(places)").fetchall()
        col_names = {c["name"] for c in cols}
        assert "neighborhood" in col_names

    def test_image_prompt_column_exists(self, conn):
        """places table should have an image_prompt column after init_db."""
        cols = conn.execute("PRAGMA table_info(places)").fetchall()
        col_names = {c["name"] for c in cols}
        assert "image_prompt" in col_names

    def test_columns_default_to_null(self, conn, city_id):
        """neighborhood and image_prompt should default to NULL."""
        place_id = _insert_place(conn, city_id, "TestPlace")
        row = conn.execute("SELECT neighborhood, image_prompt FROM places WHERE id = ?",
                           (place_id,)).fetchone()
        assert row["neighborhood"] is None
        assert row["image_prompt"] is None

    def test_columns_can_be_set(self, conn, city_id):
        """neighborhood and image_prompt should be settable."""
        place_id = _insert_place(conn, city_id, "TestPlace")
        conn.execute(
            "UPDATE places SET neighborhood = ?, image_prompt = ? WHERE id = ?",
            ("Karakoy", "A cozy cafe in Istanbul", place_id),
        )
        conn.commit()
        row = conn.execute("SELECT neighborhood, image_prompt FROM places WHERE id = ?",
                           (place_id,)).fetchone()
        assert row["neighborhood"] == "Karakoy"
        assert row["image_prompt"] == "A cozy cafe in Istanbul"

    def test_migration_idempotent(self, conn):
        """Running init_db multiple times should not duplicate columns."""
        db.init_db(conn)
        db.init_db(conn)
        cols = conn.execute("PRAGMA table_info(places)").fetchall()
        col_names = [c["name"] for c in cols]
        assert col_names.count("neighborhood") == 1
        assert col_names.count("image_prompt") == 1

    def test_migration_on_legacy_schema(self):
        """Columns should be added safely to a pre-existing places table."""
        legacy_conn = db.get_connection(":memory:")
        # Create a minimal legacy schema without neighborhood/image_prompt
        legacy_conn.executescript("""
            CREATE TABLE cities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE hashtags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city_id INTEGER NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
                tag TEXT NOT NULL,
                platform TEXT NOT NULL CHECK(platform IN ('tiktok', 'instagram')),
                scrape_status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(city_id, tag, platform)
            );
            CREATE TABLE raw_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city_id INTEGER NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
                platform TEXT NOT NULL CHECK(platform IN ('tiktok', 'instagram')),
                post_id TEXT NOT NULL,
                caption TEXT,
                likes INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0,
                shares INTEGER DEFAULT 0,
                saves INTEGER DEFAULT 0,
                views INTEGER DEFAULT 0,
                url TEXT,
                author TEXT,
                created_at TIMESTAMP,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE,
                UNIQUE(platform, post_id)
            );
            CREATE TABLE post_hashtags (
                post_id INTEGER NOT NULL REFERENCES raw_posts(id) ON DELETE CASCADE,
                hashtag_id INTEGER NOT NULL REFERENCES hashtags(id) ON DELETE CASCADE,
                PRIMARY KEY (post_id, hashtag_id)
            );
            CREATE TABLE places (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city_id INTEGER NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'other',
                mention_count INTEGER DEFAULT 1,
                virality_score REAL DEFAULT 0.0,
                is_tourist_trap BOOLEAN DEFAULT FALSE,
                sample_caption TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(city_id, name)
            );
            CREATE TABLE place_posts (
                place_id INTEGER NOT NULL REFERENCES places(id) ON DELETE CASCADE,
                post_id INTEGER NOT NULL REFERENCES raw_posts(id) ON DELETE CASCADE,
                PRIMARY KEY (place_id, post_id)
            );
        """)
        # Insert some pre-existing data
        legacy_conn.execute("INSERT INTO cities (name) VALUES ('Istanbul')")
        legacy_conn.execute(
            "INSERT INTO places (city_id, name, type) VALUES (1, 'Blue Mosque', 'temple')"
        )
        legacy_conn.commit()

        # Run init_db which should add the columns via migration
        db.init_db(legacy_conn)

        # Verify columns exist
        cols = legacy_conn.execute("PRAGMA table_info(places)").fetchall()
        col_names = {c["name"] for c in cols}
        assert "neighborhood" in col_names
        assert "image_prompt" in col_names

        # Verify existing data survived
        place = legacy_conn.execute("SELECT * FROM places WHERE name = 'Blue Mosque'").fetchone()
        assert place is not None
        assert place["neighborhood"] is None
        assert place["image_prompt"] is None

        # Idempotent on second run
        db.init_db(legacy_conn)
        cols = legacy_conn.execute("PRAGMA table_info(places)").fetchall()
        col_names = [c["name"] for c in cols]
        assert col_names.count("neighborhood") == 1
        assert col_names.count("image_prompt") == 1

        legacy_conn.close()


# ---------------------------------------------------------------------------
# get_available_places tests
# ---------------------------------------------------------------------------

class TestGetAvailablePlaces:
    def test_excludes_tourist_traps(self, conn, city_id):
        """Tourist-trap places should never be returned."""
        _insert_place(conn, city_id, "Hidden Gem", virality_score=50.0)
        _insert_place(conn, city_id, "Tourist Trap", virality_score=90.0, is_tourist_trap=True)

        results = db.get_available_places(conn, city_id)
        names = [r["name"] for r in results]
        assert "Hidden Gem" in names
        assert "Tourist Trap" not in names

    def test_excludes_recently_used_places(self, conn, city_id):
        """Places used in recent slideshows (within cooldown) should be excluded."""
        p1 = _insert_place(conn, city_id, "Fresh Place", virality_score=50.0)
        p2 = _insert_place(conn, city_id, "Used Place", virality_score=40.0)

        # Use p2 in a slideshow created now (within cooldown window)
        _insert_slideshow_with_place(conn, city_id, p2)

        results = db.get_available_places(conn, city_id)
        names = [r["name"] for r in results]
        assert "Fresh Place" in names
        assert "Used Place" not in names

    def test_returns_places_after_cooldown_expires(self, conn, city_id):
        """Places used in old slideshows (beyond cooldown) should be available again."""
        p1 = _insert_place(conn, city_id, "Old Used Place", virality_score=50.0)

        # Backdate the slideshow to beyond the cooldown period
        old_date = (datetime.now() - timedelta(days=config.PLACE_REUSE_COOLDOWN_DAYS + 1)).isoformat()
        _insert_slideshow_with_place(conn, city_id, p1, created_at=old_date)

        results = db.get_available_places(conn, city_id)
        names = [r["name"] for r in results]
        assert "Old Used Place" in names

    def test_allow_reuse_returns_all_non_traps(self, conn, city_id):
        """With allow_reuse=True, recently-used non-trap places should be returned."""
        p1 = _insert_place(conn, city_id, "Reusable Place", virality_score=50.0)
        p2 = _insert_place(conn, city_id, "Also Reusable", virality_score=30.0)
        p3 = _insert_place(conn, city_id, "Still a Trap", virality_score=90.0, is_tourist_trap=True)

        # Use p1 in a recent slideshow
        _insert_slideshow_with_place(conn, city_id, p1)

        results = db.get_available_places(conn, city_id, allow_reuse=True)
        names = [r["name"] for r in results]
        assert "Reusable Place" in names
        assert "Also Reusable" in names
        assert "Still a Trap" not in names

    def test_category_filter(self, conn, city_id):
        """Category filter should return only places with matching category."""
        _insert_place(conn, city_id, "Fancy Restaurant", virality_score=50.0,
                      category="food_and_drink")
        _insert_place(conn, city_id, "Cool Bar", virality_score=40.0,
                      category="nightlife")
        _insert_place(conn, city_id, "Another Cafe", virality_score=30.0,
                      category="food_and_drink")

        results = db.get_available_places(conn, city_id, category="food_and_drink")
        names = [r["name"] for r in results]
        assert len(results) == 2
        assert "Fancy Restaurant" in names
        assert "Another Cafe" in names
        assert "Cool Bar" not in names

    def test_category_filter_with_reuse_exclusion(self, conn, city_id):
        """Category filter and reuse exclusion should work together."""
        p1 = _insert_place(conn, city_id, "Used Restaurant", virality_score=50.0,
                           category="food_and_drink")
        _insert_place(conn, city_id, "Fresh Restaurant", virality_score=40.0,
                      category="food_and_drink")
        _insert_place(conn, city_id, "Fresh Bar", virality_score=30.0,
                      category="nightlife")

        _insert_slideshow_with_place(conn, city_id, p1)

        results = db.get_available_places(conn, city_id, category="food_and_drink")
        names = [r["name"] for r in results]
        assert len(results) == 1
        assert "Fresh Restaurant" in names

    def test_ordered_by_virality_score_desc(self, conn, city_id):
        """Results should be ordered by virality_score descending."""
        _insert_place(conn, city_id, "Low Score", virality_score=10.0)
        _insert_place(conn, city_id, "High Score", virality_score=90.0)
        _insert_place(conn, city_id, "Mid Score", virality_score=50.0)

        results = db.get_available_places(conn, city_id)
        scores = [r["virality_score"] for r in results]
        assert scores == sorted(scores, reverse=True)
        assert results[0]["name"] == "High Score"

    def test_empty_when_all_traps(self, conn, city_id):
        """Should return empty list when all places are tourist traps."""
        _insert_place(conn, city_id, "Trap 1", is_tourist_trap=True)
        _insert_place(conn, city_id, "Trap 2", is_tourist_trap=True)

        results = db.get_available_places(conn, city_id)
        assert results == []


# ---------------------------------------------------------------------------
# create_slideshow + add_slideshow_place tests
# ---------------------------------------------------------------------------

class TestCreateSlideshow:
    def test_create_slideshow_returns_id(self, conn, city_id):
        """create_slideshow should return the new slideshow's id."""
        sid = db.create_slideshow(conn, city_id, "food_and_drink", "listicle",
                                  "Top 5 hidden eats!", 5, "/tmp/slides")
        assert isinstance(sid, int)
        assert sid > 0

    def test_create_slideshow_stores_data(self, conn, city_id):
        """create_slideshow should persist all fields."""
        sid = db.create_slideshow(conn, city_id, "nightlife", "story",
                                  "Best bars!", 3, "/out/bars")
        row = conn.execute("SELECT * FROM slideshows WHERE id = ?", (sid,)).fetchone()
        assert row["city_id"] == city_id
        assert row["category"] == "nightlife"
        assert row["format"] == "story"
        assert row["hook_text"] == "Best bars!"
        assert row["slide_count"] == 3
        assert row["output_dir"] == "/out/bars"
        assert row["created_at"] is not None
        assert row["posted_at"] is None
        assert row["postiz_post_id"] is None

    def test_create_slideshow_with_null_category(self, conn, city_id):
        """create_slideshow should allow NULL category."""
        sid = db.create_slideshow(conn, city_id, None, "listicle",
                                  "Mixed gems!", 5, "/tmp")
        row = conn.execute("SELECT category FROM slideshows WHERE id = ?", (sid,)).fetchone()
        assert row["category"] is None

    def test_create_slideshow_atomic(self, conn, city_id):
        """create_slideshow uses 'with conn:' for atomic transaction."""
        sid = db.create_slideshow(conn, city_id, None, "listicle", "hook", 5, "/tmp")
        # If it was committed atomically, the row should be visible
        row = conn.execute("SELECT COUNT(*) as cnt FROM slideshows WHERE id = ?",
                           (sid,)).fetchone()
        assert row["cnt"] == 1


class TestAddSlideshowPlace:
    def test_add_slideshow_place_links_correctly(self, conn, city_id):
        """add_slideshow_place should create a link with the correct slide_number."""
        place_id = _insert_place(conn, city_id, "Linked Place")
        sid = db.create_slideshow(conn, city_id, None, "listicle", "hook", 5, "/tmp")
        db.add_slideshow_place(conn, sid, place_id, 1)

        row = conn.execute(
            "SELECT * FROM slideshow_places WHERE slideshow_id = ? AND place_id = ?",
            (sid, place_id),
        ).fetchone()
        assert row is not None
        assert row["slide_number"] == 1

    def test_add_multiple_places_to_slideshow(self, conn, city_id):
        """Multiple places can be linked to one slideshow at different positions."""
        p1 = _insert_place(conn, city_id, "Place 1")
        p2 = _insert_place(conn, city_id, "Place 2")
        p3 = _insert_place(conn, city_id, "Place 3")

        sid = db.create_slideshow(conn, city_id, None, "listicle", "hook", 3, "/tmp")
        db.add_slideshow_place(conn, sid, p1, 1)
        db.add_slideshow_place(conn, sid, p2, 2)
        db.add_slideshow_place(conn, sid, p3, 3)

        rows = conn.execute(
            "SELECT * FROM slideshow_places WHERE slideshow_id = ? ORDER BY slide_number",
            (sid,),
        ).fetchall()
        assert len(rows) == 3
        assert [r["slide_number"] for r in rows] == [1, 2, 3]


# ---------------------------------------------------------------------------
# mark_slideshow_posted tests
# ---------------------------------------------------------------------------

class TestMarkSlideshowPosted:
    def test_sets_posted_at_and_postiz_id(self, conn, city_id):
        """mark_slideshow_posted should set posted_at and postiz_post_id."""
        sid = db.create_slideshow(conn, city_id, None, "listicle", "hook", 5, "/tmp")

        # Before marking
        row = conn.execute("SELECT posted_at, postiz_post_id FROM slideshows WHERE id = ?",
                           (sid,)).fetchone()
        assert row["posted_at"] is None
        assert row["postiz_post_id"] is None

        # Mark as posted
        db.mark_slideshow_posted(conn, sid, "postiz_abc123")

        row = conn.execute("SELECT posted_at, postiz_post_id FROM slideshows WHERE id = ?",
                           (sid,)).fetchone()
        assert row["posted_at"] is not None
        assert row["postiz_post_id"] == "postiz_abc123"

    def test_posted_at_is_recent_timestamp(self, conn, city_id):
        """posted_at should be close to the current time (UTC)."""
        sid = db.create_slideshow(conn, city_id, None, "listicle", "hook", 5, "/tmp")
        db.mark_slideshow_posted(conn, sid, "xyz")

        row = conn.execute("SELECT posted_at FROM slideshows WHERE id = ?", (sid,)).fetchone()
        posted = datetime.fromisoformat(row["posted_at"])
        # CURRENT_TIMESTAMP in SQLite is UTC (naive)
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        assert abs((now_utc - posted).total_seconds()) < 5
