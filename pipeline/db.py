"""SQLite database setup, schema, and query helpers."""

import sqlite3
from pathlib import Path

from config import DB_PATH


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS hashtags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
            tag TEXT NOT NULL,
            platform TEXT NOT NULL CHECK(platform IN ('tiktok', 'instagram')),
            scrape_status TEXT DEFAULT 'pending'
                CHECK(scrape_status IN ('pending', 'running', 'completed', 'failed')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(city_id, tag, platform)
        );

        CREATE TABLE IF NOT EXISTS raw_posts (
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

        CREATE TABLE IF NOT EXISTS post_hashtags (
            post_id INTEGER NOT NULL REFERENCES raw_posts(id) ON DELETE CASCADE,
            hashtag_id INTEGER NOT NULL REFERENCES hashtags(id) ON DELETE CASCADE,
            PRIMARY KEY (post_id, hashtag_id)
        );

        CREATE TABLE IF NOT EXISTS places (
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

        CREATE TABLE IF NOT EXISTS place_posts (
            place_id INTEGER NOT NULL REFERENCES places(id) ON DELETE CASCADE,
            post_id INTEGER NOT NULL REFERENCES raw_posts(id) ON DELETE CASCADE,
            PRIMARY KEY (place_id, post_id)
        );
    """)
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_hashtags_city_status ON hashtags(city_id, scrape_status);
        CREATE INDEX IF NOT EXISTS idx_raw_posts_city_processed ON raw_posts(city_id, processed);
        CREATE INDEX IF NOT EXISTS idx_raw_posts_city ON raw_posts(city_id);
        CREATE INDEX IF NOT EXISTS idx_places_city_score ON places(city_id, virality_score DESC);
        CREATE INDEX IF NOT EXISTS idx_places_city_trap ON places(city_id, is_tourist_trap);
        CREATE INDEX IF NOT EXISTS idx_places_city_name ON places(city_id, name COLLATE NOCASE);
        CREATE INDEX IF NOT EXISTS idx_place_posts_place ON place_posts(place_id);
        CREATE INDEX IF NOT EXISTS idx_place_posts_post ON place_posts(post_id);
    """)
    conn.commit()


# --- City helpers ---

def get_or_create_city(conn: sqlite3.Connection, city_name: str) -> int:
    row = conn.execute("SELECT id FROM cities WHERE name = ?", (city_name,)).fetchone()
    if row:
        return row["id"]
    cur = conn.execute("INSERT INTO cities (name) VALUES (?)", (city_name,))
    conn.commit()
    return cur.lastrowid


def reset_city(conn: sqlite3.Connection, city_id: int) -> None:
    conn.execute("DELETE FROM cities WHERE id = ?", (city_id,))
    conn.commit()


# --- Hashtag helpers ---

def insert_hashtags(conn: sqlite3.Connection, city_id: int, tags: list[str]) -> None:
    for tag in tags:
        for platform in ("tiktok", "instagram"):
            conn.execute(
                "INSERT OR IGNORE INTO hashtags (city_id, tag, platform) VALUES (?, ?, ?)",
                (city_id, tag, platform),
            )
    conn.commit()


def get_pending_hashtags(conn: sqlite3.Connection, city_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM hashtags WHERE city_id = ? AND scrape_status = 'pending'",
        (city_id,),
    ).fetchall()


def update_hashtag_status(conn: sqlite3.Connection, hashtag_id: int, status: str) -> None:
    conn.execute(
        "UPDATE hashtags SET scrape_status = ? WHERE id = ?",
        (status, hashtag_id),
    )
    conn.commit()


# --- Post helpers ---

def insert_post(conn: sqlite3.Connection, city_id: int, platform: str,
                post_data: dict[str, object], hashtag_id: int) -> int | None:
    """Insert a post. Returns the raw_posts.id or None if duplicate."""
    try:
        cur = conn.execute(
            """INSERT OR IGNORE INTO raw_posts
               (city_id, platform, post_id, caption, likes, comments, shares,
                saves, views, url, author, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                city_id, platform,
                post_data["post_id"], post_data.get("caption"),
                post_data.get("likes", 0), post_data.get("comments", 0),
                post_data.get("shares", 0), post_data.get("saves", 0),
                post_data.get("views", 0), post_data.get("url"),
                post_data.get("author"), post_data.get("created_at"),
            ),
        )
        if cur.rowcount == 0:
            # Duplicate — still link the hashtag
            row = conn.execute(
                "SELECT id FROM raw_posts WHERE platform = ? AND post_id = ?",
                (platform, post_data["post_id"]),
            ).fetchone()
            raw_id = row["id"] if row else None
        else:
            raw_id = cur.lastrowid

        if raw_id:
            conn.execute(
                "INSERT OR IGNORE INTO post_hashtags (post_id, hashtag_id) VALUES (?, ?)",
                (raw_id, hashtag_id),
            )
        return raw_id
    except sqlite3.IntegrityError:
        return None


def get_unprocessed_posts(conn: sqlite3.Connection, city_id: int,
                          batch_size: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM raw_posts WHERE city_id = ? AND processed = FALSE LIMIT ?",
        (city_id, batch_size),
    ).fetchall()


def mark_posts_processed(conn: sqlite3.Connection, post_ids: list[int]) -> None:
    if not post_ids:
        return
    placeholders = ",".join("?" * len(post_ids))
    conn.execute(
        f"UPDATE raw_posts SET processed = TRUE WHERE id IN ({placeholders})",
        post_ids,
    )
    conn.commit()


# --- Place helpers ---

def upsert_place(conn: sqlite3.Connection, city_id: int, name: str,
                 place_type: str, post_id: int, sample_caption: str = None) -> int:
    """Insert or update a place, link it to the post. Returns place id."""
    import re
    name = re.sub(r"<[^>]+>", "", name)[:200].strip()
    if not name:
        return -1
    row = conn.execute(
        "SELECT id, mention_count FROM places WHERE city_id = ? AND name = ? COLLATE NOCASE",
        (city_id, name),
    ).fetchone()

    if row:
        place_id = row["id"]
        conn.execute(
            "UPDATE places SET mention_count = mention_count + 1 WHERE id = ?",
            (place_id,),
        )
    else:
        cur = conn.execute(
            "INSERT INTO places (city_id, name, type, sample_caption) VALUES (?, ?, ?, ?)",
            (city_id, name, place_type, sample_caption),
        )
        place_id = cur.lastrowid

    conn.execute(
        "INSERT OR IGNORE INTO place_posts (place_id, post_id) VALUES (?, ?)",
        (place_id, post_id),
    )
    return place_id


def get_all_places(conn: sqlite3.Connection, city_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM places WHERE city_id = ? ORDER BY virality_score DESC",
        (city_id,),
    ).fetchall()


def get_places_page(conn: sqlite3.Connection, city_id: int,
                    page: int = 1, per_page: int = 50) -> tuple[list[sqlite3.Row], int]:
    """Return a page of places and total count for pagination."""
    total = conn.execute(
        "SELECT COUNT(*) as cnt FROM places WHERE city_id = ?", (city_id,),
    ).fetchone()["cnt"]
    offset = (page - 1) * per_page
    rows = conn.execute(
        "SELECT * FROM places WHERE city_id = ? ORDER BY virality_score DESC LIMIT ? OFFSET ?",
        (city_id, per_page, offset),
    ).fetchall()
    return rows, total


def get_place_post_ids(conn: sqlite3.Connection, place_id: int) -> list[int]:
    rows = conn.execute(
        "SELECT post_id FROM place_posts WHERE place_id = ?", (place_id,),
    ).fetchall()
    return [r["post_id"] for r in rows]


def get_posts_by_ids(conn: sqlite3.Connection, post_ids: list[int]) -> list[sqlite3.Row]:
    if not post_ids:
        return []
    placeholders = ",".join("?" * len(post_ids))
    return conn.execute(
        f"SELECT * FROM raw_posts WHERE id IN ({placeholders})", post_ids,
    ).fetchall()


def update_virality_score(conn: sqlite3.Connection, place_id: int, score: float) -> None:
    conn.execute(
        "UPDATE places SET virality_score = ? WHERE id = ?", (score, place_id),
    )


def update_tourist_trap(conn: sqlite3.Connection, place_id: int, is_trap: bool) -> None:
    conn.execute(
        "UPDATE places SET is_tourist_trap = ? WHERE id = ?", (is_trap, place_id),
    )


def merge_places(conn: sqlite3.Connection, keep_id: int, merge_ids: list[int]) -> None:
    """Merge duplicate places into keep_id. Atomic transaction."""
    if not merge_ids:
        return
    placeholders = ",".join("?" * len(merge_ids))

    with conn:
        # Move all post links to the kept place
        conn.execute(
            f"""INSERT OR IGNORE INTO place_posts (place_id, post_id)
                SELECT ?, post_id FROM place_posts WHERE place_id IN ({placeholders})""",
            [keep_id] + merge_ids,
        )

        # Sum up mention counts
        row = conn.execute(
            f"SELECT COALESCE(SUM(mention_count), 0) as total FROM places WHERE id IN ({placeholders})",
            merge_ids,
        ).fetchone()
        conn.execute(
            "UPDATE places SET mention_count = mention_count + ? WHERE id = ?",
            (row["total"], keep_id),
        )

        # Delete merged places (cascade deletes their place_posts)
        conn.execute(
            f"DELETE FROM places WHERE id IN ({placeholders})", merge_ids,
        )


# --- Stats helpers ---

def get_city_stats(conn: sqlite3.Connection, city_id: int) -> dict:
    posts = conn.execute(
        "SELECT COUNT(*) as cnt FROM raw_posts WHERE city_id = ?", (city_id,),
    ).fetchone()["cnt"]
    hashtags = conn.execute(
        "SELECT COUNT(DISTINCT tag) as cnt FROM hashtags WHERE city_id = ?", (city_id,),
    ).fetchone()["cnt"]
    places = conn.execute(
        "SELECT COUNT(*) as cnt FROM places WHERE city_id = ?", (city_id,),
    ).fetchone()["cnt"]
    traps = conn.execute(
        "SELECT COUNT(*) as cnt FROM places WHERE city_id = ? AND is_tourist_trap = TRUE",
        (city_id,),
    ).fetchone()["cnt"]
    return {"posts": posts, "hashtags": hashtags, "places": places, "tourist_traps": traps}
