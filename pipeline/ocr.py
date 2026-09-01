"""Visual OCR — extract on-screen text from a post's media timeline.

OCR is a post-scrape caption enricher. Media must be GET'd — never treat an empty
Apify ``downloadAddr`` / ``mediaUrls`` as "nothing we can do".

Stills: every ``slideshow_urls`` frame (plus cover when present). When a photo
post has no frame URLs, resolve all slides from the TikTok photo page (yt-dlp).

Video: sample one frame every 1–2 seconds (default 1.5s) from ``video_url`` —
which may be a CDN mp4 **or** the watch page (``webVideoUrl``). Download via
yt-dlp when needed. Do not also OCR cover (avoids t=0 double-count).
Download/ffmpeg miss → ``ocr_status=failed`` for that post; city continues.

Union text per post. PR #10 engine chain. Never fail-closed. Never
``*-flash-image``.
"""

from __future__ import annotations

import base64
import io
import logging
import shutil
import sqlite3
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import requests

import config

log = logging.getLogger(__name__)

# Soft upper bound — also mirrored on config.OCR_WORKERS_MAX for docs/tests.
_OCR_WORKERS_MIN = 1
_OCR_WORKERS_MAX = 16

_OCR_PROMPT = """\
Read ALL text visible on screen in this social media post image.
Return ONLY the on-screen text, exactly as it appears, one line per text element.
Include place names, addresses, numbers/rankings, and any overlaid captions.
If there is no readable text, return "NO_TEXT"."""

_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}
_VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}


class _OCRAttempt:
    __slots__ = ("post_id", "text", "http_error", "download_error", "engine", "frame_count")

    def __init__(
        self,
        post_id: int,
        text: str | None = None,
        *,
        http_error: bool = False,
        download_error: bool = False,
        engine: str | None = None,
        frame_count: int = 0,
    ) -> None:
        self.post_id = post_id
        self.text = text
        self.http_error = http_error
        self.download_error = download_error
        self.engine = engine
        self.frame_count = frame_count


@dataclass(frozen=True)
class _EngineResult:
    """Outcome of a single OCR engine attempt on one image."""

    text: str | None = None
    engine_error: bool = False
    # True when the engine confidently reports no text (e.g. vision "NO_TEXT").
    # False for soft misses (local OCR empty) — caller should try the next engine.
    authoritative_empty: bool = False
    engine: str = ""


@dataclass(frozen=True)
class _MediaRef:
    """One media source on the post timeline."""

    kind: str  # "image_url" | "video_url" | "slideshow_page"
    url: str


def _download_image(url: str, timeout: int = 10) -> tuple[bytes | None, bool]:
    """Return (bytes, download_error). download_error True on HTTP/network failure."""
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content, False
    except requests.RequestException:
        return None, True


def _is_page_url(url: str) -> bool:
    u = (url or "").lower()
    return "tiktok.com/@" in u or "tiktok.com/video/" in u or "/photo/" in u


def _is_photo_page(url: str) -> bool:
    return "/photo/" in (url or "").lower()


def _is_video_page(url: str) -> bool:
    u = (url or "").lower()
    return "/video/" in u or ("tiktok.com/@" in u and "/photo/" not in u)


def _yt_dlp_download(url: str, timeout: int = 60) -> Path | None:
    """Download media for *url* into a temp directory; return the directory Path.

    Caller owns cleanup of the returned directory. Returns None on soft failure.
    """
    try:
        import yt_dlp
    except ImportError:
        log.warning("yt-dlp not installed — cannot resolve page URL to media bytes")
        return None

    try:
        td = tempfile.mkdtemp(prefix="ss-media-")
        outtmpl = str(Path(td) / "media.%(ext)s")
        opts = {
            "outtmpl": outtmpl,
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
            "socket_timeout": timeout,
            "format": "mp4/best/bestaudio/best",
            "writesubtitles": False,
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
        return Path(td)
    except Exception:
        log.debug("yt-dlp download failed for %s", url, exc_info=True)
        return None


def _download_video(url: str, timeout: int = 60) -> tuple[bytes | None, bool]:
    """Download video bytes from a direct CDN URL or a post page URL (yt-dlp).

    Never calls Apify. Soft-fails on any error so the city run continues.
    """
    if not url:
        return None, True
    if not _is_page_url(url):
        return _download_image(url, timeout=timeout)

    td = _yt_dlp_download(url, timeout=timeout)
    if td is None:
        return None, True
    try:
        files = sorted(td.glob("media.*"))
        video_files = [f for f in files if f.suffix.lower() in _VIDEO_EXTS]
        pick = video_files[0] if video_files else (files[0] if files else None)
        if pick is None:
            return None, True
        return pick.read_bytes(), False
    finally:
        shutil.rmtree(td, ignore_errors=True)


def resolve_slideshow_frames(page_url: str, timeout: int = 60) -> list[bytes]:
    """Fetch ALL photo-mode frames from a TikTok photo page (yt-dlp).

    Public for tests. Returns [] on soft failure — never raises.
    """
    if not page_url:
        return []
    if not _is_page_url(page_url):
        data, err = _download_image(page_url, timeout=timeout)
        return [data] if data and not err else []

    try:
        import yt_dlp
    except ImportError:
        log.warning("yt-dlp not installed — cannot resolve slideshow frames")
        return []

    frames: list[bytes] = []
    try:
        with tempfile.TemporaryDirectory(prefix="ss-slides-") as td:
            tdir = Path(td)
            opts_info = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "socket_timeout": timeout,
            }
            image_urls: list[str] = []
            with yt_dlp.YoutubeDL(opts_info) as ydl:
                info = ydl.extract_info(page_url, download=False)
            if isinstance(info, dict):
                entries = info.get("entries")
                if isinstance(entries, list) and entries:
                    for entry in entries:
                        if not isinstance(entry, dict):
                            continue
                        u = entry.get("url") or entry.get("thumbnail")
                        if isinstance(u, str) and u.startswith("http"):
                            image_urls.append(u)
                        else:
                            thumbs = entry.get("thumbnails") or []
                            if isinstance(thumbs, list) and thumbs:
                                best = thumbs[-1] if isinstance(thumbs[-1], dict) else None
                                if best and isinstance(best.get("url"), str):
                                    image_urls.append(best["url"])
                if not image_urls:
                    for fmt in info.get("formats") or []:
                        if not isinstance(fmt, dict):
                            continue
                        vcodec = (fmt.get("vcodec") or "").lower()
                        acodec = (fmt.get("acodec") or "").lower()
                        ext = (fmt.get("ext") or "").lower()
                        u = fmt.get("url")
                        if not isinstance(u, str) or not u.startswith("http"):
                            continue
                        if ext in ("jpg", "jpeg", "png", "webp") or (
                            vcodec in ("", "none") and acodec in ("", "none")
                        ):
                            if u not in image_urls:
                                image_urls.append(u)
                if not image_urls:
                    for thumb in info.get("thumbnails") or []:
                        if isinstance(thumb, dict) and isinstance(thumb.get("url"), str):
                            image_urls.append(thumb["url"])

            if image_urls:
                seen: set[str] = set()
                for u in image_urls:
                    if u in seen:
                        continue
                    seen.add(u)
                    data, err = _download_image(u, timeout=timeout)
                    if data and not err:
                        frames.append(data)
            else:
                outtmpl = str(tdir / "slide%(playlist_index)s.%(ext)s")
                opts_dl = {
                    "outtmpl": outtmpl,
                    "quiet": True,
                    "no_warnings": True,
                    "noprogress": True,
                    "socket_timeout": timeout,
                }
                with yt_dlp.YoutubeDL(opts_dl) as ydl:
                    ydl.download([page_url])
                for path in sorted(tdir.iterdir()):
                    if path.is_file() and path.suffix.lower() in _IMAGE_EXTS:
                        data = path.read_bytes()
                        if data:
                            frames.append(data)
    except Exception:
        log.debug("Slideshow frame resolve failed for %s", page_url, exc_info=True)
        return []
    return frames


def _video_frame_interval() -> float:
    """Seconds between video OCR samples — clamp to the settled 1–2s band."""
    raw = getattr(config, "OCR_VIDEO_FRAME_INTERVAL", 1.5)
    try:
        interval = float(raw)
    except (TypeError, ValueError):
        interval = 1.5
    return min(2.0, max(1.0, interval))


def _ocr_worker_count(n_posts: int) -> int:
    """How many posts may download+OCR at once (DB writes stay single-threaded)."""
    raw = getattr(config, "OCR_WORKERS", 4)
    try:
        configured = int(raw)
    except (TypeError, ValueError):
        configured = 4
    hard_max = int(getattr(config, "OCR_WORKERS_MAX", _OCR_WORKERS_MAX) or _OCR_WORKERS_MAX)
    hard_max = max(_OCR_WORKERS_MIN, min(_OCR_WORKERS_MAX, hard_max))
    workers = max(_OCR_WORKERS_MIN, min(hard_max, configured))
    return max(_OCR_WORKERS_MIN, min(workers, max(1, n_posts)))


def sample_video_frames(
    video_bytes: bytes, interval: float | None = None
) -> list[bytes]:
    """Extract JPEG frames every ``interval`` seconds via ffmpeg.

    Public for tests. Returns an empty list on ffmpeg failure (soft miss).
    """
    if not video_bytes:
        return []
    interval = (
        _video_frame_interval()
        if interval is None
        else min(2.0, max(1.0, float(interval)))
    )
    frames: list[bytes] = []
    try:
        with tempfile.TemporaryDirectory() as td:
            tdir = Path(td)
            vin = tdir / "input.mp4"
            vin.write_bytes(video_bytes)
            pattern = tdir / "frame_%04d.jpg"
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(vin),
                "-vf",
                f"fps=1/{interval}",
                "-q:v",
                "3",
                str(pattern),
            ]
            proc = subprocess.run(cmd, capture_output=True, timeout=120, check=False)
            if proc.returncode != 0:
                log.debug(
                    "ffmpeg frame sample failed: %s",
                    (proc.stderr or b"").decode("utf-8", errors="replace")[:300],
                )
                return []
            for path in sorted(tdir.glob("frame_*.jpg")):
                data = path.read_bytes()
                if data:
                    frames.append(data)
    except (OSError, subprocess.SubprocessError, TimeoutError):
        log.debug("ffmpeg frame sampling raised", exc_info=True)
        return []
    return frames


def _ocr_models() -> list[str]:
    """Ordered OpenRouter vision model ids (primary + configured fallbacks)."""
    primary = (getattr(config, "OCR_MODEL", None) or "").strip()
    raw = getattr(config, "OCR_FALLBACK_MODELS", "") or ""
    fallbacks = [m.strip() for m in str(raw).split(",") if m.strip()]
    models: list[str] = []
    for mid in ([primary] if primary else []) + fallbacks:
        if mid not in models:
            models.append(mid)
    return models


def _tesseract_available() -> bool:
    if not getattr(config, "OCR_USE_TESSERACT", True):
        return False
    try:
        import pytesseract  # noqa: F401
        from PIL import Image  # noqa: F401
    except ImportError:
        return False
    try:
        import pytesseract as _pt

        _pt.get_tesseract_version()
        return True
    except Exception:
        return False


def _ocr_tesseract(image_bytes: bytes) -> _EngineResult:
    """Local/offline OCR. Empty result is a soft miss (try vision next)."""
    name = "tesseract"
    try:
        import pytesseract
        from PIL import Image

        img = Image.open(io.BytesIO(image_bytes))
        lang = getattr(config, "OCR_TESSERACT_LANG", None) or "eng+kor"
        text = pytesseract.image_to_string(img, lang=lang).strip()
        if not text:
            return _EngineResult(engine=name)
        return _EngineResult(text=text, engine=name)
    except Exception:
        log.debug("Tesseract OCR failed", exc_info=True)
        return _EngineResult(engine_error=True, engine=name)


def _ocr_openrouter(image_bytes: bytes, model: str) -> _EngineResult:
    """OpenRouter chat-completions vision OCR for one model id."""
    name = f"openrouter:{model}"
    api_key = getattr(config, "OPENROUTER_API_KEY", "") or ""
    if not api_key:
        log.debug("Skipping %s — OPENROUTER_API_KEY not set", name)
        return _EngineResult(engine_error=True, engine=name)

    b64 = base64.b64encode(image_bytes).decode("utf-8")
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
                    },
                    {"type": "text", "text": _OCR_PROMPT},
                ],
            }
        ],
        "temperature": 0.1,
        "max_tokens": 500,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(
            config.OPENROUTER_BASE_URL,
            json=payload,
            headers=headers,
            timeout=30,
        )
        if resp.status_code in (401, 403, 404, 408, 429) or resp.status_code >= 500:
            log.warning(
                "OCR engine %s HTTP %s — trying next engine",
                name,
                resp.status_code,
            )
            return _EngineResult(engine_error=True, engine=name)
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"].strip()
        if text == "NO_TEXT" or not text:
            return _EngineResult(authoritative_empty=True, engine=name)
        return _EngineResult(text=text, engine=name)
    except (requests.RequestException, KeyError, IndexError, TypeError, ValueError):
        log.warning("OCR engine %s request failed — trying next", name, exc_info=True)
        return _EngineResult(engine_error=True, engine=name)


def _iter_engines():
    """Yield callables (image_bytes) -> _EngineResult in preference order."""
    if _tesseract_available():
        yield _ocr_tesseract
    for model in _ocr_models():

        def _make(m: str):
            return lambda image_bytes, _m=m: _ocr_openrouter(image_bytes, _m)

        yield _make(model)


def _ocr_image(image_bytes: bytes) -> tuple[str | None, bool, str | None]:
    """Run the OCR fallback chain on one image.

    Returns (text, engine_error, engine_name).
    engine_error True only when every configured engine was down (404/timeout/
    auth/etc). ``NO_TEXT`` is not an engine_error.
    """
    engines = list(_iter_engines())
    if not engines:
        log.error("No OCR engines configured (no Tesseract, no OpenRouter models)")
        return None, True, None

    for run in engines:
        result = run(image_bytes)
        if result.engine_error:
            continue
        if result.text:
            log.info("OCR succeeded via %s", result.engine)
            return result.text, False, result.engine
        if result.authoritative_empty:
            log.info("OCR %s reported NO_TEXT", result.engine)
            return None, False, result.engine

    return None, True, None


def _split_urls(raw: object) -> list[str]:
    if not raw:
        return []
    urls: list[str] = []
    for part in str(raw).split("\n"):
        part = part.strip()
        if part and part not in urls:
            urls.append(part)
    return urls


def _get_field(post: sqlite3.Row | dict, name: str) -> object:
    if isinstance(post, dict):
        return post.get(name)
    try:
        return post[name]
    except (KeyError, IndexError):
        return None


def media_timeline_for_post(post: sqlite3.Row | dict) -> list[_MediaRef]:
    """Build the OCR media timeline for one post.

    Priority:
    1. Explicit slideshow frame URLs (cover + every slide, deduped).
    2. Photo page URL when ``is_slideshow`` / ``/photo/`` and frames missing —
       resolve ALL slides at OCR time (not cover-only).
    3. Video source: ``video_url`` (CDN **or** watch page) — never also OCR cover.
    4. Watch page ``url`` when ``video_url`` empty (already-ingested Seoul-style rows).
    5. Cover alone only when no slideshow/video/page source exists.
    """
    slideshow = _split_urls(_get_field(post, "slideshow_urls"))
    cover = str(_get_field(post, "cover_url") or "").strip()
    video = str(_get_field(post, "video_url") or "").strip()
    page = str(_get_field(post, "url") or "").strip()
    is_ss_flag = _get_field(post, "is_slideshow")
    is_slideshow = is_ss_flag in (True, 1, "1", "true", "True")
    is_slideshow = is_slideshow or bool(slideshow) or _is_photo_page(page)

    if slideshow:
        urls: list[str] = []
        if cover:
            urls.append(cover)
        for u in slideshow:
            if u not in urls:
                urls.append(u)
        return [_MediaRef("image_url", u) for u in urls]

    if is_slideshow and page:
        return [_MediaRef("slideshow_page", page)]

    video_src = video
    if not video_src and page and _is_video_page(page):
        video_src = page
    if video_src:
        return [_MediaRef("video_url", video_src)]

    if cover:
        return [_MediaRef("image_url", cover)]
    return []


def _frames_from_media(ref: _MediaRef) -> tuple[list[bytes], bool]:
    """Return (frame_bytes_list, download_error_occurred)."""
    if ref.kind == "image_url":
        data, err = _download_image(ref.url)
        if err or not data:
            return [], True
        return [data], False

    if ref.kind == "slideshow_page":
        frames = resolve_slideshow_frames(ref.url)
        if not frames:
            return [], True
        return frames, False

    video_bytes, err = _download_video(ref.url)
    if err or not video_bytes:
        return [], True
    frames = sample_video_frames(video_bytes)
    if not frames:
        return [], True
    return frames, False


def _process_one(post_id: int, media: list[_MediaRef]) -> _OCRAttempt:
    """Download + OCR every frame on the post's media timeline; union text.

    Download/ffmpeg miss on a video (or total still miss) surfaces as
    download_error → ``ocr_status=failed`` for that post; the city run continues.
    No cover soft-success when the timeline was video-only / slideshow-page.
    """
    texts: list[str] = []
    engines_used: list[str] = []
    any_http = False
    any_download = False
    frame_count = 0
    attempted = False
    needs_multi_frame = any(r.kind in ("video_url", "slideshow_page") for r in media)

    for ref in media:
        if not ref.url:
            continue
        attempted = True
        frames, dl_err = _frames_from_media(ref)
        if dl_err:
            any_download = True
            continue
        for image_bytes in frames:
            frame_count += 1
            text, engine_err, engine = _ocr_image(image_bytes)
            if engine_err:
                any_http = True
                continue
            if engine:
                engines_used.append(engine)
            if text:
                texts.append(text)

    if not attempted:
        return _OCRAttempt(post_id, download_error=True)

    if needs_multi_frame and frame_count == 0:
        return _OCRAttempt(post_id, download_error=True, frame_count=0)

    seen: set[str] = set()
    unique_texts: list[str] = []
    for t in texts:
        if t not in seen:
            seen.add(t)
            unique_texts.append(t)

    combined = "\n".join(unique_texts) if unique_texts else None
    engine_label = ",".join(dict.fromkeys(engines_used)) if engines_used else None
    return _OCRAttempt(
        post_id,
        combined,
        http_error=any_http and not combined,
        download_error=any_download and not combined and not any_http,
        engine=engine_label,
        frame_count=frame_count,
    )


def _apply_ocr_result(
    conn: sqlite3.Connection,
    post: sqlite3.Row | dict,
    result: _OCRAttempt,
) -> tuple[bool, bool]:
    """Persist one OCR attempt. Returns (enriched, http_or_download_error).

    Caller must hold the DB write lock. Never raises for soft OCR failures.
    """
    post_id = result.post_id
    caption = _get_field(post, "caption") or ""

    if result.http_error:
        conn.execute(
            "UPDATE raw_posts SET ocr_status = 'failed' WHERE id = ?",
            (post_id,),
        )
        log.warning(
            "OCR all engines failed for post %d — marked failed, continuing",
            post_id,
        )
        return False, True

    if result.text:
        if "🔤 On-screen text:" not in str(caption):
            updated = f"{caption}\n🔤 On-screen text: {result.text}"
            conn.execute(
                "UPDATE raw_posts SET caption = ?, ocr_status = 'done' WHERE id = ?",
                (updated, post_id),
            )
        else:
            conn.execute(
                "UPDATE raw_posts SET ocr_status = 'done' WHERE id = ?",
                (post_id,),
            )
        log.info(
            "OCR enriched post %d via %s (%d frames)",
            post_id,
            result.engine or "unknown",
            result.frame_count,
        )
        return True, False

    status = "empty" if not result.download_error else "failed"
    conn.execute(
        "UPDATE raw_posts SET ocr_status = ? WHERE id = ?",
        (status, post_id),
    )
    if status == "empty":
        log.info(
            "OCR post %d: no on-screen text (%s, %d frames)",
            post_id,
            result.engine or "unknown",
            result.frame_count,
        )
    return False, bool(result.download_error)


def extract_cover_text(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    batch_size: int = 50,
) -> int:
    """OCR each pending post's media timeline (slideshow frames / video samples).

    Download + ffmpeg + engine chain run concurrently across posts
    (``OCR_WORKERS``). SQLite upserts are applied on the orchestrator thread
    under a write lock — workers never share ``conn``. Never aborts the city
    run when OCR engines or individual frames are down — posts are marked
    ``failed`` and discover continues.
    """
    total_enriched = 0
    total_attempted = 0
    total_http_errors = 0
    db_lock = threading.Lock()

    while True:
        posts = conn.execute(
            """SELECT id, cover_url, slideshow_urls, video_url, url, is_slideshow, caption
               FROM raw_posts
               WHERE city_id = ?
                 AND processed = FALSE
                 AND COALESCE(ocr_status, 'pending') = 'pending'
                 AND (
                   (slideshow_urls IS NOT NULL AND slideshow_urls != '')
                   OR (video_url IS NOT NULL AND video_url != '')
                   OR (cover_url IS NOT NULL AND cover_url != '')
                   OR (
                     url IS NOT NULL AND url != ''
                     AND (
                       instr(lower(url), '/video/') > 0
                       OR instr(lower(url), '/photo/') > 0
                     )
                   )
                 )
               LIMIT ?""",
            (city_id, batch_size),
        ).fetchall()

        if not posts:
            break

        max_workers = _ocr_worker_count(len(posts))
        log.info(
            "Running visual OCR (media timeline) on %d posts for %s "
            "(%d workers)...",
            len(posts),
            city_name,
            max_workers,
        )

        # Build timelines on this thread so sqlite3.Row stays off the pool.
        jobs = [(post["id"], media_timeline_for_post(post), post) for post in posts]

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_process_one, post_id, media): post
                for post_id, media, post in jobs
            }
            for future in as_completed(futures):
                post = futures[future]
                total_attempted += 1
                try:
                    result = future.result()
                except Exception:
                    total_http_errors += 1
                    with db_lock:
                        conn.execute(
                            "UPDATE raw_posts SET ocr_status = 'failed' WHERE id = ?",
                            (post["id"],),
                        )
                        conn.commit()
                    log.debug("OCR failed for post %d", post["id"], exc_info=True)
                    continue

                with db_lock:
                    enriched, err = _apply_ocr_result(conn, post, result)
                    conn.commit()
                if enriched:
                    total_enriched += 1
                if err:
                    total_http_errors += 1

    if total_attempted == 0:
        log.info("No posts with media timeline assets to OCR for %s", city_name)
    else:
        if total_http_errors:
            log.warning(
                "Visual OCR for %s finished with gaps: %d enriched, %d attempted, "
                "%d engine/download failures (city run continues)",
                city_name,
                total_enriched,
                total_attempted,
                total_http_errors,
            )
        else:
            log.info(
                "Visual OCR complete for %s: %d enriched, %d attempted, %d errors",
                city_name,
                total_enriched,
                total_attempted,
                total_http_errors,
            )
    return total_enriched
