"""Tests for the CLI orchestrator (generate_slideshow.py)."""

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from generate_slideshow import _normalize_raw_filenames, build_parser, main
from pipeline import db
from pipeline.slideshow_types import PostMeta


class _UnclosableConnection:
    """Wraps a sqlite3.Connection but makes close() a no-op.

    Delegates all attribute access and context manager protocol to the
    underlying connection so it works with ``with conn:`` transactions.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def close(self):
        pass  # no-op — keep the in-memory DB alive

    def __enter__(self):
        return self._conn.__enter__()

    def __exit__(self, *args):
        return self._conn.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mem_conn():
    """In-memory database with schema and seed data."""
    c = db.get_connection(":memory:")
    db.init_db(c)
    return c


@pytest.fixture
def seeded_conn(mem_conn):
    """Database with a city and enough places to generate a slideshow."""
    city_id = db.get_or_create_city(mem_conn, "Tokyo")
    for i in range(10):
        mem_conn.execute(
            """INSERT INTO places (city_id, name, type, category, virality_score,
               is_tourist_trap, sample_caption, neighborhood, image_prompt)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                city_id,
                f"Place {i + 1}",
                "restaurant",
                "food_and_drink",
                100.0 - i,
                False,
                f"Great place {i + 1}",
                f"District {i + 1}",
                f"A cozy restaurant in Tokyo district {i + 1}",
            ),
        )
    # Add 2 tourist traps (should be excluded)
    for i in range(2):
        mem_conn.execute(
            """INSERT INTO places (city_id, name, type, category, virality_score,
               is_tourist_trap)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (city_id, f"Tourist Trap {i + 1}", "restaurant", "food_and_drink", 200.0, True),
        )
    mem_conn.commit()
    return mem_conn


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------


class TestBuildParser:
    def test_required_city(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["--city", "Tokyo"])
        assert args.city == "Tokyo"
        assert args.slide_count == 8
        assert args.hook_format == "listicle"
        assert args.post is False
        assert args.allow_reuse is False
        assert args.category is None

    def test_all_options(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "--city",
                "Tokyo",
                "--category",
                "food_and_drink",
                "--slide-count",
                "6",
                "--format",
                "story",
                "--post",
                "--allow-reuse",
                "--cta-template",
                "/path/to/cta.png",
            ]
        )
        assert args.category == "food_and_drink"
        assert args.slide_count == 6
        assert args.hook_format == "story"
        assert args.post is True
        assert args.allow_reuse is True
        assert args.cta_template == "/path/to/cta.png"

    def test_invalid_category(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--city", "Tokyo", "--category", "invalid"])


# ---------------------------------------------------------------------------
# Normalize filename tests
# ---------------------------------------------------------------------------


class TestNormalizeRawFilenames:
    def test_renames_hook_file(self, tmp_path):
        hook = tmp_path / "slide_1_hook_raw.png"
        hook.write_bytes(b"fake png")
        _normalize_raw_filenames(tmp_path, slide_count=4)
        assert (tmp_path / "slide_1_raw.png").exists()

    def test_renames_cta_file(self, tmp_path):
        cta = tmp_path / "slide_6_cta_raw.png"
        cta.write_bytes(b"fake png")
        _normalize_raw_filenames(tmp_path, slide_count=4)
        assert (tmp_path / "slide_6_raw.png").exists()

    def test_does_not_overwrite_existing(self, tmp_path):
        hook_special = tmp_path / "slide_1_hook_raw.png"
        hook_special.write_bytes(b"special")
        hook_normal = tmp_path / "slide_1_raw.png"
        hook_normal.write_bytes(b"original")
        _normalize_raw_filenames(tmp_path, slide_count=4)
        assert hook_normal.read_bytes() == b"original"


# ---------------------------------------------------------------------------
# Integration test helper
# ---------------------------------------------------------------------------


def _run_pipeline(seeded_conn, tmp_path, args_list, mock_post=None):
    """Run main() with all external dependencies mocked."""
    mock_enrich = MagicMock(return_value=0)
    mock_hook = MagicMock(
        return_value={
            "hook_text": "4 places in Tokyo\ntourists never find",
            "hook_image_prompt": "A stunning shot of Tokyo",
            "caption": "Tokyo hidden gems #tokyo #travel",
        }
    )
    def fake_img_gen(output_dir, places, hook_image_prompt, cta_template_path=None):
        n = len(places) + 2  # hook + locations + CTA
        return {
            "generated": n,
            "skipped": 0,
            "failed": 0,
            "failed_slides": [],
        }

    mock_img_gen = MagicMock(side_effect=fake_img_gen)

    def fake_overlay(output_dir):
        texts = json.loads((Path(output_dir) / "texts.json").read_text())
        for i in range(1, len(texts) + 1):
            (Path(output_dir) / f"slide_{i}.png").write_bytes(b"fake overlay")
        return len(texts)

    mock_overlay = MagicMock(side_effect=fake_overlay)
    mock_post_fn = mock_post or MagicMock(
        return_value=PostMeta(
            postiz_post_id="post_123",
            posted_at="2026-03-24T12:00:00",
        )
    )

    wrapped_conn = _UnclosableConnection(seeded_conn)

    with (
        patch("sys.argv", ["generate_slideshow.py", *args_list]),
        patch("generate_slideshow.db.get_connection", return_value=wrapped_conn),
        patch("pipeline.enrichment.enrich_places", mock_enrich),
        patch("pipeline.hooks.generate_hook", mock_hook),
        patch("pipeline.image_gen.generate_slideshow_images", mock_img_gen),
        patch("pipeline.overlay.add_overlays", mock_overlay),
        patch("pipeline.posting.post_slideshow", mock_post_fn),
        patch("generate_slideshow.SLIDESHOW_OUTPUT_DIR", tmp_path),
    ):
        main()

    return {
        "enrich": mock_enrich,
        "hook": mock_hook,
        "img_gen": mock_img_gen,
        "overlay": mock_overlay,
        "post": mock_post_fn,
    }


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestMainIntegration:
    def test_full_pipeline(self, seeded_conn, tmp_path):
        mocks = _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4"])
        mocks["enrich"].assert_called_once()
        mocks["hook"].assert_called_once()
        mocks["img_gen"].assert_called_once()
        mocks["overlay"].assert_called_once()

    def test_creates_output_directory(self, seeded_conn, tmp_path):
        _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4"])
        dirs = [d for d in tmp_path.iterdir() if d.is_dir()]
        assert len(dirs) == 1
        assert "tokyo" in dirs[0].name

    def test_saves_meta_json(self, seeded_conn, tmp_path):
        _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4"])
        dirs = [d for d in tmp_path.iterdir() if d.is_dir()]
        meta_path = dirs[0] / "meta.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert meta["city"] == "Tokyo"
        assert meta["slide_count"] == 4
        assert meta["format"] == "listicle"

    def test_saves_texts_json(self, seeded_conn, tmp_path):
        _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4"])
        dirs = [d for d in tmp_path.iterdir() if d.is_dir()]
        texts_path = dirs[0] / "texts.json"
        assert texts_path.exists()
        texts = json.loads(texts_path.read_text())
        assert len(texts) == 6  # hook + 4 locations + CTA
        assert texts[0]["type"] == "hook"
        assert texts[-1]["type"] == "cta"

    def test_records_slideshow_in_db(self, seeded_conn, tmp_path):
        _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4"])
        row = seeded_conn.execute("SELECT * FROM slideshows").fetchone()
        assert row is not None
        assert row["slide_count"] == 4
        assert row["format"] == "listicle"
        places = seeded_conn.execute("SELECT * FROM slideshow_places").fetchall()
        assert len(places) == 4

    def test_slide_count_adjusts_when_fewer_available(self, seeded_conn, tmp_path):
        seeded_conn.execute(
            "DELETE FROM places WHERE name IN ('Place 6','Place 7','Place 8','Place 9','Place 10')"
        )
        seeded_conn.commit()
        _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "8"])
        row = seeded_conn.execute("SELECT * FROM slideshows").fetchone()
        assert row["slide_count"] == 5

    def test_aborts_when_fewer_than_4_places(self, seeded_conn, tmp_path):
        seeded_conn.execute("DELETE FROM places WHERE name NOT IN ('Place 1','Place 2','Place 3')")
        seeded_conn.commit()
        with pytest.raises(SystemExit):
            _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4"])

    def test_aborts_invalid_city(self, seeded_conn, tmp_path):
        with pytest.raises(SystemExit):
            _run_pipeline(seeded_conn, tmp_path, ["--city", "Atlantis", "--slide-count", "4"])

    def test_invalid_slide_count_too_low(self, seeded_conn, tmp_path):
        with pytest.raises(SystemExit):
            _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "2"])

    def test_invalid_slide_count_too_high(self, seeded_conn, tmp_path):
        with pytest.raises(SystemExit):
            _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "20"])

    def test_category_filter(self, seeded_conn, tmp_path):
        _run_pipeline(
            seeded_conn,
            tmp_path,
            ["--city", "Tokyo", "--slide-count", "4", "--category", "food_and_drink"],
        )
        row = seeded_conn.execute("SELECT * FROM slideshows").fetchone()
        assert row["category"] == "food_and_drink"

    def test_story_format(self, seeded_conn, tmp_path):
        _run_pipeline(
            seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4", "--format", "story"]
        )
        row = seeded_conn.execute("SELECT * FROM slideshows").fetchone()
        assert row["format"] == "story"

    def test_location_slide_numbers_in_texts(self, seeded_conn, tmp_path):
        _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4"])
        dirs = [d for d in tmp_path.iterdir() if d.is_dir()]
        texts = json.loads((dirs[0] / "texts.json").read_text())
        location_texts = [t for t in texts if t["type"] == "location"]
        assert len(location_texts) == 4
        assert location_texts[0]["number"] == "1/4"
        assert location_texts[3]["number"] == "4/4"


class TestPostingIntegration:
    def test_post_flag_triggers_posting(self, seeded_conn, tmp_path):
        mocks = _run_pipeline(
            seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4", "--post"]
        )
        mocks["post"].assert_called_once()

    def test_post_updates_db(self, seeded_conn, tmp_path):
        _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4", "--post"])
        row = seeded_conn.execute("SELECT * FROM slideshows").fetchone()
        assert row["postiz_post_id"] == "post_123"

    def test_no_post_flag_skips_posting(self, seeded_conn, tmp_path):
        mocks = _run_pipeline(seeded_conn, tmp_path, ["--city", "Tokyo", "--slide-count", "4"])
        mocks["post"].assert_not_called()
