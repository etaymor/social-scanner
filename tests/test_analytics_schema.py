"""Tests for analytics schema migrations and new tables."""

import json

from pipeline import db


def test_init_db_creates_analytics_tables(conn):
    """init_db creates all new analytics tables."""
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = {r["name"] for r in tables}

    assert "slideshow_analytics" in table_names
    assert "platform_stats" in table_names
    assert "rc_snapshots" in table_names
    assert "slideshow_performance" in table_names


def test_init_db_idempotent(conn):
    """Running init_db twice does not error."""
    db.init_db(conn)  # Already called by fixture; call again
    # Should not raise


def test_slideshows_has_new_columns(conn, city_id):
    """Slideshows table has tiktok_release_id, visual_style, cta_text, publish_status."""
    slideshow_id = db.create_slideshow(
        conn, city_id, category="food_and_drink", hook_format="listicle",
        hook_text="test", slide_count=5, output_dir="/tmp/test",
    )
    conn.commit()

    row = conn.execute("SELECT * FROM slideshows WHERE id = ?", (slideshow_id,)).fetchone()
    assert row["tiktok_release_id"] is None
    assert row["visual_style"] is None
    assert row["cta_text"] is None
    assert row["publish_status"] == "draft"


def test_slideshow_analytics_unique_constraint(conn, city_id):
    """slideshow_analytics enforces UNIQUE(slideshow_id, DATE(fetched_at))."""
    sid = db.create_slideshow(
        conn, city_id, category=None, hook_format="listicle",
        hook_text="test", slide_count=5, output_dir="/tmp/test",
    )
    conn.commit()

    conn.execute(
        "INSERT INTO slideshow_analytics (slideshow_id, views) VALUES (?, ?)",
        (sid, 100),
    )
    # Second insert same day should use INSERT OR REPLACE
    conn.execute(
        "INSERT OR REPLACE INTO slideshow_analytics (slideshow_id, views, fetched_at) "
        "VALUES (?, ?, CURRENT_TIMESTAMP)",
        (sid, 200),
    )
    conn.commit()

    rows = conn.execute(
        "SELECT views FROM slideshow_analytics WHERE slideshow_id = ?", (sid,)
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["views"] == 200


def test_slideshow_analytics_foreign_key(conn, city_id):
    """slideshow_analytics references slideshows(id)."""
    sid = db.create_slideshow(
        conn, city_id, category=None, hook_format="listicle",
        hook_text="test", slide_count=5, output_dir="/tmp/test",
    )
    conn.commit()

    conn.execute(
        "INSERT INTO slideshow_analytics (slideshow_id, views) VALUES (?, ?)",
        (sid, 100),
    )
    conn.commit()

    # Verify the row exists
    row = conn.execute(
        "SELECT * FROM slideshow_analytics WHERE slideshow_id = ?", (sid,)
    ).fetchone()
    assert row is not None


def test_slideshow_performance_append_only(conn, city_id):
    """slideshow_performance allows multiple rows per slideshow (append-only)."""
    sid = db.create_slideshow(
        conn, city_id, category=None, hook_format="listicle",
        hook_text="test", slide_count=5, output_dir="/tmp/test",
    )
    conn.commit()

    conn.execute(
        "INSERT INTO slideshow_performance (slideshow_id, views_at_48h, decision_tag) VALUES (?, ?, ?)",
        (sid, 500, "test"),
    )
    conn.execute(
        "INSERT INTO slideshow_performance (slideshow_id, views_at_48h, decision_tag) VALUES (?, ?, ?)",
        (sid, 500, "drop"),
    )
    conn.commit()

    rows = conn.execute(
        "SELECT * FROM slideshow_performance WHERE slideshow_id = ?", (sid,)
    ).fetchall()
    assert len(rows) == 2


def test_slideshow_performance_decision_tag_check(conn, city_id):
    """decision_tag only allows scale/keep/test/drop."""
    sid = db.create_slideshow(
        conn, city_id, category=None, hook_format="listicle",
        hook_text="test", slide_count=5, output_dir="/tmp/test",
    )
    conn.commit()

    import sqlite3
    import pytest

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO slideshow_performance (slideshow_id, decision_tag) VALUES (?, ?)",
            (sid, "invalid"),
        )


def test_platform_stats_table(conn):
    """platform_stats can store a snapshot."""
    conn.execute(
        "INSERT INTO platform_stats (followers, total_views, total_likes) VALUES (?, ?, ?)",
        (1000, 50000, 3000),
    )
    conn.commit()

    row = conn.execute("SELECT * FROM platform_stats ORDER BY id DESC LIMIT 1").fetchone()
    assert row["followers"] == 1000
    assert row["total_views"] == 50000


def test_rc_snapshots_table(conn):
    """rc_snapshots can store a RevenueCat snapshot."""
    conn.execute(
        "INSERT INTO rc_snapshots (mrr, active_trials, active_subscriptions, revenue) VALUES (?, ?, ?, ?)",
        (670.0, 12, 45, 1200.50),
    )
    conn.commit()

    row = conn.execute("SELECT * FROM rc_snapshots ORDER BY id DESC LIMIT 1").fetchone()
    assert row["mrr"] == 670.0
    assert row["active_trials"] == 12


def test_analytics_indexes_exist(conn):
    """All analytics indexes are created."""
    indexes = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
    ).fetchall()
    index_names = {r["name"] for r in indexes}

    expected = {
        "idx_slideshow_analytics_slideshow",
        "idx_platform_stats_fetched",
        "idx_rc_snapshots_fetched",
        "idx_slideshows_postiz",
        "idx_slideshows_publish_status",
        "idx_slideshow_performance_slideshow",
        "idx_slideshow_performance_decision",
    }
    assert expected.issubset(index_names)


def test_update_slideshow_metadata(conn, city_id):
    """update_slideshow_metadata stores visual_style and cta_text."""
    sid = db.create_slideshow(
        conn, city_id, category=None, hook_format="listicle",
        hook_text="test", slide_count=5, output_dir="/tmp/test",
    )

    style_json = json.dumps({
        "time_of_day": "golden_hour",
        "weather": "clear",
        "perspective": "street_level",
        "color_mood": "warm_analog",
    })
    db.update_slideshow_metadata(conn, sid, visual_style_json=style_json, cta_text="Test CTA")
    conn.commit()

    row = conn.execute("SELECT visual_style, cta_text FROM slideshows WHERE id = ?", (sid,)).fetchone()
    assert json.loads(row["visual_style"])["time_of_day"] == "golden_hour"
    assert row["cta_text"] == "Test CTA"


def test_views_estimated_default(conn, city_id):
    """New slideshow_analytics rows default views_estimated to FALSE."""
    sid = db.create_slideshow(
        conn, city_id, category=None, hook_format="listicle",
        hook_text="test", slide_count=5, output_dir="/tmp/test",
    )
    conn.commit()

    conn.execute(
        "INSERT INTO slideshow_analytics (slideshow_id, views) VALUES (?, ?)",
        (sid, 100),
    )
    conn.commit()

    row = conn.execute(
        "SELECT views_estimated FROM slideshow_analytics WHERE slideshow_id = ?", (sid,)
    ).fetchone()
    assert row["views_estimated"] == 0  # FALSE
