"""Tests for parallel per-post OCR worker pool + serialized DB upserts."""

from __future__ import annotations

import threading
import time

from pipeline import db


def test_ocr_workers_default_and_clamp():
    """OCR_WORKERS defaults to 4 and is clamped to a safe band."""
    import config as cfg
    from pipeline import ocr

    assert int(cfg.OCR_WORKERS) == 4
    assert int(cfg.OCR_WORKERS_MAX) == 16
    assert ocr._ocr_worker_count(1) == 1
    assert ocr._ocr_worker_count(100) == 4

    orig = cfg.OCR_WORKERS
    orig_max = cfg.OCR_WORKERS_MAX
    try:
        cfg.OCR_WORKERS = 99
        cfg.OCR_WORKERS_MAX = 16
        assert ocr._ocr_worker_count(50) == 16
        cfg.OCR_WORKERS = 0
        assert ocr._ocr_worker_count(50) == 1
        cfg.OCR_WORKERS = -3
        assert ocr._ocr_worker_count(50) == 1
    finally:
        cfg.OCR_WORKERS = orig
        cfg.OCR_WORKERS_MAX = orig_max


def test_ocr_n_posts_run_concurrently(monkeypatch, tmp_path):
    """N posts must OCR with overlapping workers — fail if effectively serial."""
    from pipeline import ocr

    db_path = tmp_path / "parallel_ocr.db"
    conn = db.get_connection(db_path)
    db.init_db(conn)
    city_id = db.get_or_create_city(conn, "ParallelCity")

    n_posts = 8
    workers = 4
    for i in range(n_posts):
        conn.execute(
            "INSERT INTO raw_posts "
            "(city_id, platform, post_id, cover_url, caption, ocr_status) "
            "VALUES (?, 'tiktok', ?, ?, 'cap', 'pending')",
            (city_id, f"p{i}", f"http://cdn.example/cover_{i}.jpg"),
        )
    conn.commit()

    monkeypatch.setattr(ocr.config, "OCR_USE_TESSERACT", False)
    monkeypatch.setattr(ocr.config, "OCR_MODEL", "live/vision")
    monkeypatch.setattr(ocr.config, "OCR_FALLBACK_MODELS", "")
    monkeypatch.setattr(ocr.config, "OPENROUTER_API_KEY", "test-key")
    monkeypatch.setattr(ocr.config, "OCR_WORKERS", workers)
    monkeypatch.setattr(ocr, "_download_image", lambda url, timeout=10: (b"img", False))

    active = 0
    peak_active = 0
    lock = threading.Lock()
    thread_ids: set[int] = set()
    barrier = threading.Barrier(workers, timeout=5)

    def fake_openrouter(image_bytes, model):
        nonlocal active, peak_active
        tid = threading.get_ident()
        with lock:
            active += 1
            peak_active = max(peak_active, active)
            thread_ids.add(tid)
        try:
            barrier.wait()
            time.sleep(0.05)
            return ocr._EngineResult(
                text=f"Venue-{tid}",
                engine=f"openrouter:{model}",
            )
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(ocr, "_ocr_openrouter", fake_openrouter)

    enriched = ocr.extract_cover_text(conn, city_id, "ParallelCity")
    assert enriched == n_posts
    assert peak_active >= workers, (
        f"expected >= {workers} concurrent OCR workers, peak was {peak_active}"
    )
    assert len(thread_ids) >= workers

    rows = conn.execute(
        "SELECT post_id, ocr_status, caption FROM raw_posts WHERE city_id = ? "
        "ORDER BY post_id",
        (city_id,),
    ).fetchall()
    assert len(rows) == n_posts
    assert all(r["ocr_status"] == "done" for r in rows)
    assert all("🔤 On-screen text:" in (r["caption"] or "") for r in rows)
    conn.close()


def test_ocr_parallel_db_writes_do_not_corrupt(monkeypatch, tmp_path):
    """Parallel compute + serialized upserts leave consistent per-post rows."""
    from pipeline import ocr

    db_path = tmp_path / "ocr_writes.db"
    conn = db.get_connection(db_path)
    db.init_db(conn)
    city_id = db.get_or_create_city(conn, "WriteCity")

    n_posts = 12
    for i in range(n_posts):
        conn.execute(
            "INSERT INTO raw_posts "
            "(city_id, platform, post_id, cover_url, caption, ocr_status) "
            "VALUES (?, 'tiktok', ?, ?, ?, 'pending')",
            (city_id, f"w{i}", f"http://cdn.example/c_{i}.jpg", f"base-{i}"),
        )
    conn.commit()

    monkeypatch.setattr(ocr.config, "OCR_USE_TESSERACT", False)
    monkeypatch.setattr(ocr.config, "OCR_MODEL", "live/vision")
    monkeypatch.setattr(ocr.config, "OCR_FALLBACK_MODELS", "")
    monkeypatch.setattr(ocr.config, "OPENROUTER_API_KEY", "test-key")
    monkeypatch.setattr(ocr.config, "OCR_WORKERS", 4)
    monkeypatch.setattr(ocr, "_download_image", lambda url, timeout=10: (b"img", False))

    def fake_openrouter(image_bytes, model):
        time.sleep(0.01 * (threading.get_ident() % 5))
        return ocr._EngineResult(
            text=f"Place-{threading.get_ident()}",
            engine=f"openrouter:{model}",
        )

    monkeypatch.setattr(ocr, "_ocr_openrouter", fake_openrouter)

    apply_active = 0
    apply_peak = 0
    apply_lock = threading.Lock()
    real_apply = ocr._apply_ocr_result

    def wrapped_apply(conn_, post, result):
        nonlocal apply_active, apply_peak
        with apply_lock:
            apply_active += 1
            apply_peak = max(apply_peak, apply_active)
            assert apply_active == 1, "DB apply must not interleave"
        try:
            return real_apply(conn_, post, result)
        finally:
            with apply_lock:
                apply_active -= 1

    monkeypatch.setattr(ocr, "_apply_ocr_result", wrapped_apply)

    enriched = ocr.extract_cover_text(conn, city_id, "WriteCity")
    assert enriched == n_posts
    assert apply_peak == 1

    rows = conn.execute(
        "SELECT post_id, caption, ocr_status FROM raw_posts WHERE city_id = ? "
        "ORDER BY CAST(substr(post_id, 2) AS INTEGER)",
        (city_id,),
    ).fetchall()
    assert len(rows) == n_posts
    for i, row in enumerate(rows):
        assert row["post_id"] == f"w{i}"
        assert row["ocr_status"] == "done"
        cap = row["caption"] or ""
        assert cap.startswith(f"base-{i}")
        assert "🔤 On-screen text:" in cap
        assert f"base-{i}" in cap
        assert cap.count("🔤 On-screen text:") == 1

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    assert integrity == "ok"
    conn.close()


def test_ocr_workers_pool_used_for_batch(monkeypatch, conn, city_id):
    """extract_cover_text must construct a ThreadPoolExecutor with OCR_WORKERS."""
    from concurrent.futures import ThreadPoolExecutor

    from pipeline import ocr

    for i in range(6):
        conn.execute(
            "INSERT INTO raw_posts "
            "(city_id, platform, post_id, cover_url, caption, ocr_status) "
            "VALUES (?, 'tiktok', ?, 'http://cdn.example/x.jpg', 'c', 'pending')",
            (city_id, f"pool{i}"),
        )
    conn.commit()

    monkeypatch.setattr(ocr.config, "OCR_USE_TESSERACT", False)
    monkeypatch.setattr(ocr.config, "OCR_MODEL", "live/vision")
    monkeypatch.setattr(ocr.config, "OCR_FALLBACK_MODELS", "")
    monkeypatch.setattr(ocr.config, "OPENROUTER_API_KEY", "test-key")
    monkeypatch.setattr(ocr.config, "OCR_WORKERS", 3)
    monkeypatch.setattr(ocr, "_download_image", lambda url, timeout=10: (b"img", False))
    monkeypatch.setattr(
        ocr,
        "_ocr_openrouter",
        lambda image_bytes, model: ocr._EngineResult(
            text="V", engine=f"openrouter:{model}"
        ),
    )

    seen_workers: list[int] = []

    class TrackingPool(ThreadPoolExecutor):
        def __init__(self, *args, **kwargs):
            mw = kwargs.get("max_workers")
            if mw is None and args:
                mw = args[0]
            seen_workers.append(mw)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(ocr, "ThreadPoolExecutor", TrackingPool)

    enriched = ocr.extract_cover_text(conn, city_id, "TestCity")
    assert enriched == 6
    assert seen_workers == [3]


def test_ocr_default_not_flash_image_still_holds():
    """Parallelization must not regress the OCR model chain constraints."""
    import config as cfg

    assert "flash-image" not in cfg.OCR_MODEL
    assert cfg.OCR_MODEL == "google/gemini-3.1-flash-lite"
    assert "gemini-3-flash-preview" in cfg.OCR_FALLBACK_MODELS
    assert 1.0 <= float(cfg.OCR_VIDEO_FRAME_INTERVAL) <= 2.0
