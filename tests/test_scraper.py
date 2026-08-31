"""Tests for scraper field mapping and engagement filters."""

from datetime import datetime, timedelta, timezone

from pipeline.scraper import (
    _generate_search_queries,
    _map_instagram,
    _map_tiktok,
    _passes_instagram_filter,
    _passes_tiktok_filter,
)


class TestQueryGeneration:
    def test_tokyo_food_queries(self):
        queries = _generate_search_queries("Tokyo", "food_and_drink")
        assert "tokyo itinerary" in queries
        assert "tokyo must eat" in queries
        assert "tokyo food guide" in queries
        assert "tokyo best restaurants" in queries
        assert "tokyo eats" in queries
        assert "東京 グルメ おすすめ" in queries
        # Should NOT generate old cuisine-based queries
        assert "tokyo ramen" not in queries
        assert "tokyo sushi" not in queries
        assert "shibuya food" not in queries

    def test_generic_city_queries(self):
        queries = _generate_search_queries("Bangkok", "food_and_drink")
        assert "bangkok itinerary" in queries
        assert "bangkok must eat" in queries
        assert "bangkok food guide" in queries
        # Should not have city-specific localized queries
        assert not any("グルメ" in q for q in queries)

    def test_no_queries_for_non_food_category(self):
        queries = _generate_search_queries("Tokyo", "nightlife")
        assert queries == []


class TestTikTokMapping:
    def test_basic_mapping(self):
        item = {
            "id": "123",
            "text": "Great place!",
            "diggCount": 500,
            "commentCount": 20,
            "shareCount": 10,
            "collectCount": 5,
            "playCount": 10000,
            "webVideoUrl": "https://tiktok.com/v/123",
            "authorMeta": {"name": "testuser"},
        }
        result = _map_tiktok(item)
        assert result["post_id"] == "123"
        assert result["caption"] == "Great place!"
        assert result["likes"] == 500
        assert result["comments"] == 20
        assert result["shares"] == 10
        assert result["saves"] == 5
        assert result["views"] == 10000
        assert result["author"] == "testuser"

    def test_fallback_to_stats_dict(self):
        item = {
            "id": "456",
            "desc": "Fallback caption",
            "stats": {
                "diggCount": 100,
                "commentCount": 5,
                "shareCount": 2,
                "playCount": 3000,
            },
            "author": "user2",
        }
        result = _map_tiktok(item)
        assert result["caption"] == "Fallback caption"
        assert result["likes"] == 100
        assert result["views"] == 3000
        assert result["author"] == "user2"

    def test_missing_fields_default_zero(self):
        item = {"id": "789"}
        result = _map_tiktok(item)
        assert result["post_id"] == "789"
        assert result["likes"] == 0
        assert result["saves"] == 0

    def test_url_fallback_construction(self):
        item = {"id": "abc", "authorMeta": {"name": "bob"}}
        result = _map_tiktok(item)
        assert "bob" in result["url"]
        assert "abc" in result["url"]

    def test_slideshow_urls_include_tiktok_link_frames(self):
        """Apify slideshowImageLinks use tiktokLink — all frames must be captured."""
        item = {
            "id": "ss",
            "isSlideshow": True,
            "slideshowImageLinks": [
                {
                    "tiktokLink": "https://cdn.example/frame0.jpg",
                    "downloadLink": "https://cdn.example/frame0.jpg",
                },
                {"tiktokLink": "https://cdn.example/frame1.jpg"},
                {"tiktokLink": "https://cdn.example/frame2.jpg"},
            ],
            "videoMeta": {"coverUrl": "https://cdn.example/cover.jpg"},
        }
        result = _map_tiktok(item)
        frames = result["slideshow_urls"].split("\n")
        assert len(frames) == 3
        assert frames[0].endswith("frame0.jpg")
        assert frames[2].endswith("frame2.jpg")
        assert result["is_slideshow"] is True

    def test_mapper_persists_every_mediaurls_entry(self):
        """FAIL if mapper drops mediaUrls — ingest must land every slide URL."""
        urls = [
            "https://cdn.example/slide_0.jpg",
            "https://cdn.example/slide_1.jpg",
            "https://cdn.example/slide_2.jpg",
            "https://cdn.example/slide_3.jpg",
        ]
        item = {
            "id": "mediaurls-n",
            "isSlideshow": True,
            "mediaUrls": urls,
            "videoMeta": {
                "coverUrl": "https://cdn.example/COVER_ONLY.jpg",
                "duration": 0,
            },
            "webVideoUrl": "https://www.tiktok.com/@u/photo/mediaurls-n",
        }
        result = _map_tiktok(item)
        landed = result["slideshow_urls"].split("\n")
        assert landed == urls, (
            f"mapper dropped mediaUrls entries: expected {urls}, got {landed}"
        )
        # Cover alone is not a substitute for slideshow frames.
        assert result["cover_url"].endswith("COVER_ONLY.jpg")
        assert result["cover_url"] not in landed or len(landed) == len(urls)
        assert result["is_slideshow"] is True

    def test_insert_post_roundtrip_keeps_all_slideshow_urls(self, conn, city_id):
        """DB insert must persist every mapped slide URL (not cover-only)."""
        from pipeline import db

        urls = [f"https://cdn.example/m{i}.jpg" for i in range(5)]
        mapped = _map_tiktok(
            {
                "id": "rt1",
                "isSlideshow": True,
                "mediaUrls": urls,
                "text": "slides",
                "diggCount": 100,
                "playCount": 5000,
                "videoMeta": {"coverUrl": "https://cdn.example/cover.jpg", "duration": 0},
            }
        )
        # Satisfy engagement filter fields for realism; insert_post does not filter.
        conn.execute(
            "INSERT INTO hashtags (city_id, tag, platform, scrape_status) "
            "VALUES (?, 't', 'tiktok', 'completed')",
            (city_id,),
        )
        hid = conn.execute("SELECT id FROM hashtags").fetchone()["id"]
        raw_id = db.insert_post(conn, city_id, "tiktok", mapped, hid)
        conn.commit()
        assert raw_id is not None
        row = conn.execute(
            "SELECT slideshow_urls, cover_url, video_url, video_duration, is_slideshow "
            "FROM raw_posts WHERE id = ?",
            (raw_id,),
        ).fetchone()
        assert row["slideshow_urls"].split("\n") == urls
        assert row["is_slideshow"] == 1
        assert row["cover_url"].endswith("cover.jpg")

    def test_video_url_and_duration_persisted_from_apify_fields(self):
        item = {
            "id": "v1",
            "downloadAddr": "https://cdn.example/video.mp4",
            "videoUrl": "https://www.tiktok.com/@u/video/v1",
            "videoMeta": {"coverUrl": "https://cdn.example/cover.jpg", "duration": 12.5},
            "isSlideshow": False,
        }
        result = _map_tiktok(item)
        assert result["video_url"] == "https://cdn.example/video.mp4"
        assert result["video_duration"] == 12.5
        assert result["is_slideshow"] is False
        # Page URL must never be stored as video_url
        assert "tiktok.com/@" not in (result["video_url"] or "")

    def test_page_video_url_not_treated_as_download(self):
        item = {
            "id": "v2",
            "videoUrl": "https://www.tiktok.com/@u/video/v2",
            "webVideoUrl": "https://www.tiktok.com/@u/video/v2",
            "videoMeta": {"duration": 9},
        }
        result = _map_tiktok(item)
        assert result["video_url"] == ""
        assert result["video_duration"] == 9.0


class TestInstagramMapping:
    def test_basic_mapping(self):
        item = {
            "id": "ig_123",
            "caption": "Beautiful sunset",
            "likesCount": 200,
            "commentsCount": 15,
            "videoViewCount": 5000,
            "url": "https://instagram.com/p/123",
            "ownerUsername": "photographer",
            "timestamp": "2024-01-01",
        }
        result = _map_instagram(item)
        assert result["post_id"] == "ig_123"
        assert result["caption"] == "Beautiful sunset"
        assert result["likes"] == 200
        assert result["comments"] == 15
        assert result["shares"] == 0  # IG has no shares
        assert result["saves"] == 0  # IG has no saves
        assert result["views"] == 5000
        assert result["author"] == "photographer"


class TestEngagementFilters:
    def test_tiktok_passes(self):
        assert _passes_tiktok_filter({"views": 5000, "likes": 100})

    def test_tiktok_low_views(self):
        assert not _passes_tiktok_filter({"views": 500, "likes": 100})

    def test_tiktok_low_likes(self):
        assert not _passes_tiktok_filter({"views": 5000, "likes": 10})

    def test_instagram_passes(self):
        assert _passes_instagram_filter({"views": 1000, "likes": 50})

    def test_instagram_photo_no_views(self):
        # Photos may have 0 views — should pass if likes are enough
        assert _passes_instagram_filter({"views": 0, "likes": 50})

    def test_instagram_low_likes(self):
        assert not _passes_instagram_filter({"views": 1000, "likes": 5})

    def test_instagram_low_views_nonzero(self):
        assert not _passes_instagram_filter({"views": 100, "likes": 50})

    def test_none_values_treated_as_zero(self):
        assert not _passes_tiktok_filter({"views": None, "likes": None})
