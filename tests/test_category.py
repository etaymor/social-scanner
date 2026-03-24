"""Tests for category-based search filter feature."""

import sqlite3
from unittest.mock import patch

import pytest

import config
from pipeline import db
from pipeline.extractor import _validate_category


# ---------------------------------------------------------------------------
# _validate_category unit tests
# ---------------------------------------------------------------------------

class TestValidateCategory:
    def test_valid_category_passes_through(self):
        assert _validate_category("food_and_drink", "restaurant") == "food_and_drink"

    def test_valid_category_case_insensitive(self):
        assert _validate_category("Food_And_Drink", "restaurant") == "food_and_drink"

    def test_valid_category_with_spaces(self):
        assert _validate_category("food and drink", "restaurant") == "food_and_drink"

    def test_invalid_category_falls_back_to_type(self):
        assert _validate_category("invalid_cat", "restaurant") == "food_and_drink"
        assert _validate_category("invalid_cat", "bar") == "nightlife"
        assert _validate_category("invalid_cat", "museum") == "arts_and_culture"

    def test_none_category_falls_back_to_type(self):
        assert _validate_category(None, "cafe") == "food_and_drink"
        assert _validate_category(None, "park") == "outdoors_and_nature"

    def test_empty_category_falls_back_to_type(self):
        assert _validate_category("", "hotel") == "places_to_stay"

    def test_unknown_type_falls_back_to_default(self):
        assert _validate_category(None, "other") == "sights_and_attractions"
        assert _validate_category("", "other") == "sights_and_attractions"

    def test_all_valid_categories(self):
        for cat in config.VALID_CATEGORIES:
            assert _validate_category(cat, "other") == cat


# ---------------------------------------------------------------------------
# TYPE_TO_CATEGORY mapping tests
# ---------------------------------------------------------------------------

class TestTypeToCategoryMapping:
    def test_all_expanded_types_have_mapping(self):
        """Every type in VALID_PLACE_TYPES (except 'other') should have a category mapping."""
        unmapped = []
        for t in config.VALID_PLACE_TYPES:
            if t != "other" and t not in config.TYPE_TO_CATEGORY:
                unmapped.append(t)
        assert unmapped == [], f"Types without category mapping: {unmapped}"

    def test_all_mapped_types_are_valid(self):
        """Every type in TYPE_TO_CATEGORY should be in VALID_PLACE_TYPES."""
        for t in config.TYPE_TO_CATEGORY:
            assert t in config.VALID_PLACE_TYPES, f"{t} not in VALID_PLACE_TYPES"

    def test_all_mapped_categories_are_valid(self):
        """Every category value in TYPE_TO_CATEGORY should be in VALID_CATEGORIES."""
        for t, cat in config.TYPE_TO_CATEGORY.items():
            assert cat in config.VALID_CATEGORIES, f"{t} -> {cat} not in VALID_CATEGORIES"

    def test_specific_mappings(self):
        assert config.TYPE_TO_CATEGORY["restaurant"] == "food_and_drink"
        assert config.TYPE_TO_CATEGORY["bar"] == "nightlife"
        assert config.TYPE_TO_CATEGORY["hotel"] == "places_to_stay"
        assert config.TYPE_TO_CATEGORY["park"] == "outdoors_and_nature"
        assert config.TYPE_TO_CATEGORY["museum"] == "arts_and_culture"
        assert config.TYPE_TO_CATEGORY["tour"] == "activities_and_experiences"
        assert config.TYPE_TO_CATEGORY["shop"] == "shopping"
        assert config.TYPE_TO_CATEGORY["viewpoint"] == "sights_and_attractions"


# ---------------------------------------------------------------------------
# Category-aware hashtag generation integration test
# ---------------------------------------------------------------------------

class TestCategoryHashtagGeneration:
    @patch("pipeline.hashtags.call_llm_json")
    def test_category_generates_mixed_hashtags(self, mock_llm, conn, city_id):
        """Category hashtags should include category-specific + generic + universal."""
        call_count = 0

        def side_effect(prompt, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Category-specific call
                return {"hashtags": [f"tag_cat_{i}" for i in range(12)]}
            else:
                # Generic call
                return {"hashtags": [f"tag_gen_{i}" for i in range(3)]}

        mock_llm.side_effect = side_effect

        from pipeline.hashtags import generate_hashtags
        tags = generate_hashtags(conn, city_id, "Istanbul", category="nightlife")

        # Should have LLM category tags + generic tags + seed tags + universal
        assert len(tags) > 15
        assert "tag_cat_0" in tags
        assert "tag_gen_0" in tags
        assert "istanbulhiddengems" in tags  # universal
        assert "istanbulnightlife" in tags  # category seed suffix

        # 2 LLM calls (category + generic)
        assert mock_llm.call_count == 2

        # Hashtags should be stored with category
        rows = conn.execute(
            "SELECT DISTINCT category FROM hashtags WHERE city_id = ?", (city_id,),
        ).fetchall()
        categories = {r["category"] for r in rows}
        assert categories == {"nightlife"}

    @patch("pipeline.hashtags.call_llm_json")
    def test_no_category_uses_original_behavior(self, mock_llm, conn, city_id):
        """Without category, should use single 15-tag prompt + universal."""
        mock_llm.return_value = {
            "hashtags": ["istanbulhidden", "istanbulfood", "istanbulnightlife"]
        }

        from pipeline.hashtags import generate_hashtags
        tags = generate_hashtags(conn, city_id, "Istanbul")

        # Single LLM call
        assert mock_llm.call_count == 1

        # Hashtags stored without category
        rows = conn.execute(
            "SELECT DISTINCT category FROM hashtags WHERE city_id = ?", (city_id,),
        ).fetchall()
        categories = {r["category"] for r in rows}
        assert categories == {None}


# ---------------------------------------------------------------------------
# Category-aware extraction integration test
# ---------------------------------------------------------------------------

class TestCategoryExtraction:
    @patch("pipeline.extractor.call_llm_json")
    def test_extract_assigns_category(self, mock_llm, conn, city_id):
        """Extraction should assign category to each place."""
        conn.execute(
            """INSERT INTO raw_posts
               (city_id, platform, post_id, caption, likes, views, processed)
               VALUES (?, 'tiktok', 'cat_post_1', 'Best rooftop bar in Karakoy!', 100, 5000, FALSE)""",
            (city_id,),
        )
        conn.commit()

        mock_llm.return_value = {
            "results": [
                {
                    "caption_index": 1,
                    "places": [
                        {"name": "Sky Bar", "type": "bar", "category": "nightlife"}
                    ],
                }
            ]
        }

        from pipeline.extractor import extract_places
        count = extract_places(conn, city_id, "Istanbul")

        assert count == 1
        place = conn.execute(
            "SELECT * FROM places WHERE city_id = ? AND name = 'Sky Bar'",
            (city_id,),
        ).fetchone()
        assert place is not None
        assert place["category"] == "nightlife"

    @patch("pipeline.extractor.call_llm_json")
    def test_extract_validates_invalid_category(self, mock_llm, conn, city_id):
        """Invalid category from LLM should fall back to TYPE_TO_CATEGORY."""
        conn.execute(
            """INSERT INTO raw_posts
               (city_id, platform, post_id, caption, likes, views, processed)
               VALUES (?, 'tiktok', 'cat_post_2', 'Visit the Blue Mosque!', 100, 5000, FALSE)""",
            (city_id,),
        )
        conn.commit()

        mock_llm.return_value = {
            "results": [
                {
                    "caption_index": 1,
                    "places": [
                        {"name": "Blue Mosque", "type": "temple", "category": "invalid_cat"}
                    ],
                }
            ]
        }

        from pipeline.extractor import extract_places
        count = extract_places(conn, city_id, "Istanbul")

        assert count == 1
        place = conn.execute(
            "SELECT * FROM places WHERE city_id = ? AND name = 'Blue Mosque'",
            (city_id,),
        ).fetchone()
        assert place["category"] == "sights_and_attractions"

    @patch("pipeline.extractor.call_llm_json")
    def test_extract_missing_category_falls_back(self, mock_llm, conn, city_id):
        """Missing category from LLM should fall back to TYPE_TO_CATEGORY."""
        conn.execute(
            """INSERT INTO raw_posts
               (city_id, platform, post_id, caption, likes, views, processed)
               VALUES (?, 'tiktok', 'cat_post_3', 'Great cafe!', 100, 5000, FALSE)""",
            (city_id,),
        )
        conn.commit()

        mock_llm.return_value = {
            "results": [
                {
                    "caption_index": 1,
                    "places": [
                        {"name": "Petra Cafe", "type": "cafe"}
                    ],
                }
            ]
        }

        from pipeline.extractor import extract_places
        count = extract_places(conn, city_id, "Istanbul")

        assert count == 1
        place = conn.execute(
            "SELECT * FROM places WHERE city_id = ? AND name = 'Petra Cafe'",
            (city_id,),
        ).fetchone()
        assert place["category"] == "food_and_drink"


# ---------------------------------------------------------------------------
# CLI argument parsing tests
# ---------------------------------------------------------------------------

class TestCLICategory:
    def test_category_argument_accepted(self):
        """--category with valid value should parse successfully."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--city", required=True)
        parser.add_argument("--category", choices=sorted(config.VALID_CATEGORIES))
        args = parser.parse_args(["--city", "Istanbul", "--category", "nightlife"])
        assert args.category == "nightlife"

    def test_category_argument_optional(self):
        """--category omitted should default to None."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--city", required=True)
        parser.add_argument("--category", choices=sorted(config.VALID_CATEGORIES), default=None)
        args = parser.parse_args(["--city", "Istanbul"])
        assert args.category is None

    def test_invalid_category_rejected(self):
        """--category with invalid value should raise error."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--city", required=True)
        parser.add_argument("--category", choices=sorted(config.VALID_CATEGORIES))
        with pytest.raises(SystemExit):
            parser.parse_args(["--city", "Istanbul", "--category", "invalid"])


# ---------------------------------------------------------------------------
# Dashboard category filter tests
# ---------------------------------------------------------------------------

class TestDashboardCategoryFilter:
    def test_get_places_page_with_category(self, conn, city_id):
        """get_places_page with category should filter results."""
        # Insert places with different categories
        conn.execute(
            "INSERT INTO places (city_id, name, type, category) VALUES (?, 'Cafe A', 'cafe', 'food_and_drink')",
            (city_id,),
        )
        conn.execute(
            "INSERT INTO places (city_id, name, type, category) VALUES (?, 'Bar B', 'bar', 'nightlife')",
            (city_id,),
        )
        conn.execute(
            "INSERT INTO places (city_id, name, type, category) VALUES (?, 'Cafe C', 'cafe', 'food_and_drink')",
            (city_id,),
        )
        conn.commit()

        # Filter by food_and_drink
        places, total = db.get_places_page(conn, city_id, category="food_and_drink")
        assert total == 2
        assert len(places) == 2
        assert all(p["category"] == "food_and_drink" for p in places)

        # Filter by nightlife
        places, total = db.get_places_page(conn, city_id, category="nightlife")
        assert total == 1
        assert places[0]["name"] == "Bar B"

        # No filter returns all
        places, total = db.get_places_page(conn, city_id)
        assert total == 3

    def test_get_places_page_category_pagination(self, conn, city_id):
        """Category filter should work correctly with pagination."""
        for i in range(5):
            conn.execute(
                "INSERT INTO places (city_id, name, type, category, virality_score) VALUES (?, ?, 'cafe', 'food_and_drink', ?)",
                (city_id, f"Cafe {i}", 10 - i),
            )
        conn.commit()

        # Page 1 of 2 (per_page=3)
        places, total = db.get_places_page(conn, city_id, page=1, per_page=3, category="food_and_drink")
        assert total == 5
        assert len(places) == 3

        # Page 2 of 2
        places, total = db.get_places_page(conn, city_id, page=2, per_page=3, category="food_and_drink")
        assert total == 5
        assert len(places) == 2


# ---------------------------------------------------------------------------
# Schema migration tests
# ---------------------------------------------------------------------------

class TestSchemaMigration:
    def test_category_columns_created(self, conn):
        """init_db should create category columns on both tables."""
        # Check places table has category column
        cols = conn.execute("PRAGMA table_info(places)").fetchall()
        col_names = {c["name"] for c in cols}
        assert "category" in col_names

        # Check hashtags table has category column
        cols = conn.execute("PRAGMA table_info(hashtags)").fetchall()
        col_names = {c["name"] for c in cols}
        assert "category" in col_names

    def test_migration_idempotent(self, conn):
        """Running init_db twice should not fail."""
        db.init_db(conn)  # Already called once by fixture
        db.init_db(conn)  # Second call should be safe

        # Verify columns still exist
        cols = conn.execute("PRAGMA table_info(places)").fetchall()
        col_names = [c["name"] for c in cols]
        assert col_names.count("category") == 1


# ---------------------------------------------------------------------------
# Upsert category behavior
# ---------------------------------------------------------------------------

class TestUpsertCategory:
    def test_upsert_stores_category(self, conn, city_id):
        """New place should store category."""
        cur = conn.execute(
            "INSERT INTO raw_posts (city_id, platform, post_id, caption) VALUES (?, 'tiktok', 'p1', 'test')",
            (city_id,),
        )
        post_id = cur.lastrowid
        conn.commit()

        db.upsert_place(conn, city_id, "Mikla", "restaurant", post_id, category="food_and_drink")
        conn.commit()

        place = conn.execute(
            "SELECT category FROM places WHERE city_id = ? AND name = 'Mikla'", (city_id,),
        ).fetchone()
        assert place["category"] == "food_and_drink"

    def test_upsert_updates_category_when_not_null(self, conn, city_id):
        """Upserting with a non-null category should update it."""
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

        db.upsert_place(conn, city_id, "Mikla", "restaurant", pid1, category="food_and_drink")
        db.upsert_place(conn, city_id, "Mikla", "restaurant", pid2, category="nightlife")
        conn.commit()

        place = conn.execute(
            "SELECT category FROM places WHERE city_id = ? AND name = 'Mikla'", (city_id,),
        ).fetchone()
        assert place["category"] == "nightlife"

    def test_upsert_preserves_category_when_null(self, conn, city_id):
        """Upserting with null category should NOT overwrite existing category."""
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

        db.upsert_place(conn, city_id, "Mikla", "restaurant", pid1, category="food_and_drink")
        db.upsert_place(conn, city_id, "Mikla", "restaurant", pid2, category=None)
        conn.commit()

        place = conn.execute(
            "SELECT category FROM places WHERE city_id = ? AND name = 'Mikla'", (city_id,),
        ).fetchone()
        assert place["category"] == "food_and_drink"


# ---------------------------------------------------------------------------
# Hashtag category isolation tests
# ---------------------------------------------------------------------------

class TestHashtagCategoryIsolation:
    def test_pending_hashtags_filtered_by_category(self, conn, city_id):
        """get_pending_hashtags should filter by category when provided."""
        db.insert_hashtags(conn, city_id, ["food_tag"], category="food_and_drink")
        db.insert_hashtags(conn, city_id, ["night_tag"], category="nightlife")
        db.insert_hashtags(conn, city_id, ["generic_tag"])

        food = db.get_pending_hashtags(conn, city_id, category="food_and_drink")
        assert len(food) == 2  # 2 platforms
        assert all(r["tag"] == "food_tag" for r in food)

        night = db.get_pending_hashtags(conn, city_id, category="nightlife")
        assert len(night) == 2
        assert all(r["tag"] == "night_tag" for r in night)

        # No category returns all
        all_pending = db.get_pending_hashtags(conn, city_id)
        assert len(all_pending) == 6  # 3 tags * 2 platforms


# ---------------------------------------------------------------------------
# Merge preserves canonical category
# ---------------------------------------------------------------------------

class TestMergePreservesCategory:
    def test_merge_keeps_canonical_category(self, conn, city_id):
        """Merging places should preserve the canonical place's category."""
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

        place_a = db.upsert_place(conn, city_id, "Sky Bar", "bar", pid1, category="nightlife")
        place_b = db.upsert_place(conn, city_id, "Sky Bar Rooftop", "bar", pid2, category="food_and_drink")
        conn.commit()

        # Merge B into A (A is canonical)
        db.merge_places(conn, place_a, [place_b])
        conn.commit()

        place = conn.execute("SELECT category FROM places WHERE id = ?", (place_a,)).fetchone()
        assert place["category"] == "nightlife"
