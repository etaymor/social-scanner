"""Visual OCR — extract on-screen text from a post's media timeline.

OCR unit is the **per-post media timeline**, not the cover thumbnail:

- Slideshow / photo-mode: OCR **every** frame URL.
- Video: sample one frame every 1–2 seconds (default 1.5s) via ffmpeg and OCR
  each sample.
- Union the text per post for extraction.

Tries a fallback chain of engines. One engine/frame 404/timeout/auth miss must
not abort the city run — the next engine or frame is tried. ``NO_TEXT`` from a
vision model is a successful empty read, not an HTTP failure. Never fail-closed.
"""

from __future__ import annotations

import base64
import io
import logging
import sqlite3
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import requests

import config

log = logging.getLogger(__name__)

_OCR_PROMPT = """\
Read ALL text visible on screen in this social media post image.
Return ONLY the on-screen text, exactly as it appears, one line per text element.
Include place names, addresses, numbers/rankings, and any overlaid captions.
If there is no readable text, return "NO_TEXT"."""


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

    kind: str  # "image_url" | "video_url"
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


def _download_video(url: str, timeout: int = 60) -> tuple[bytes | None, bool]:
    """Download video bytes from a direct CDN URL or a post page URL (yt-dlp).

    Never calls Apify. Soft-fails on any error so the city run continues.
    """
    if not url:
        return None, True
    if not _is_page_url(url):
        return _download_image(url, timeout=timeout)

    try:
        import yt_dlp
    except ImportError:
        log.warning("yt-dlp not installed — cannot resolve page URL to video bytes")
        return None, True

    try:
        with tempfile.TemporaryDirectory() as td:
            outtmpl = str(Path(td) / "media.%(ext)s")
            opts = {
                "outtmpl": outtmpl,
                "quiet": True,
                "no_warnings": True,
                "noprogress": True,
                "format": "mp4/best",
                "socket_timeout": timeout,
            }
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
            files = sorted(Path(td).glob("media.*"))
            if not files:
                return None, True
            return files[0].read_bytes(), False
    except Exception:
        log.debug("Video download failed for %s", url, exc_info=True)
        return None, True


def _video_frame_interval() -> float:
    """Seconds between video OCR samples — clamp to the settled 1–2s band."""
    raw = getattr(config, "OCR_VIDEO_FRAME_INTERVAL", 1.5)
    try:
        interval = float(raw)
    except (TypeError, ValueError):
        interval = 1.5
    return min(2.0, max(1.0, interval))


def sample_video_frames(
    video_bytes: bytes, interval: float | None = None
) -> list[bytes]:
    """Extract JPEG frames every ``interval`` seconds via ffmpeg.

    Public for tests. Returns an empty list on ffmpeg failure (soft miss).
    """
    if not video_bytes:
        return []
    interval = _video_frame_interval() if interval is None else min(2.0, max(1.0, float(interval)))
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
            # Soft miss — stylized TikTok overlays often defeat Tesseract.
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
            # Authoritative empty — not an HTTP failure.
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
        # Soft empty (local) — fall through to the next engine.

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


def media_timeline_for_post(post: sqlite3.Row | dict) -> list[_MediaRef]:
    """Build the OCR media timeline for one post (slideshow → video → cover).

    Cover alone is never preferred when slideshow frames or a video URL exist.
    """

    def _get(name: str) -> object:
        if isinstance(post, dict):
            return post.get(name)
        try:
            return post[name]
        except (KeyError, IndexError):
            return None

    slideshow = _split_urls(_get("slideshow_urls"))
    if slideshow:
        return [_MediaRef("image_url", u) for u in slideshow]

    video = str(_get("video_url") or "").strip()
    if video:
        return [_MediaRef("video_url", video)]

    # Allow resolving the post page URL to video bytes (yt-dlp) — no Apify.
    page = str(_get("url") or "").strip()
    if page and _is_page_url(page):
        return [_MediaRef("video_url", page)]

    cover = str(_get("cover_url") or "").strip()
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

    video_bytes, err = _download_video(ref.url)
    if err or not video_bytes:
        return [], True
    frames = sample_video_frames(video_bytes)
    if not frames:
        return [], True
    return frames, False


def _process_one(
    post_id: int,
    media: list[_MediaRef],
    *,
    cover_fallback: str | None = None,
) -> _OCRAttempt:
    """Download + OCR every frame on the post's media timeline; union text."""
    texts: list[str] = []
    engines_used: list[str] = []
    any_http = False
    any_download = False
    frame_count = 0
    attempted = False

    for ref in media:
        if not ref.url:
            continue
        attempted = True
        frames, dl_err = _frames_from_media(ref)
        if dl_err:
            any_download = True
            # One frame/source miss must not abort the post or city run.
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

    if frame_count == 0 and cover_fallback:
        attempted = True
        frames, dl_err = _frames_from_media(_MediaRef("image_url", cover_fallback))
        if dl_err:
            any_download = True
        else:
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

    # Deduplicate identical frame reads while preserving order
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


def extract_cover_text(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    batch_size: int = 50,
) -> int:
    """OCR each pending post's media timeline (slideshow frames / video samples).

    Loops until no pending posts remain. Appends unioned on-screen text to
    captions. Never aborts the city run when OCR engines or individual frames
    are down — posts are marked ``failed`` and discover continues.
    """
    total_enriched = 0
    total_attempted = 0
    total_http_errors = 0

    while True:
        posts = conn.execute(
            """SELECT id, cover_url, slideshow_urls, video_url, url, caption
               FROM raw_posts
               WHERE city_id = ?
                 AND processed = FALSE
                 AND COALESCE(ocr_status, 'pending') = 'pending'
                 AND (
                   (slideshow_urls IS NOT NULL AND slideshow_urls != '')
                   OR (video_url IS NOT NULL AND video_url != '')
                   OR (cover_url IS NOT NULL AND cover_url != '')
                   OR (url IS NOT NULL AND url != '')
                 )
               LIMIT ?""",
            (city_id, batch_size),
        ).fetchall()

        if not posts:
            break

        log.info(
            "Running visual OCR (media timeline) on %d posts for %s...",
            len(posts),
            city_name,
        )
        max_workers = min(5, len(posts))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {}
            for post in posts:
                cover = (post["cover_url"] or "").strip() if post["cover_url"] else None
                futures[
                    pool.submit(
                        _process_one,
                        post["id"],
                        media_timeline_for_post(post),
                        cover_fallback=cover,
                    )
                ] = post
            for future in as_completed(futures):
                post = futures[future]
                total_attempted += 1
                try:
                    result = future.result()
                except Exception:
                    total_http_errors += 1
                    conn.execute(
                        "UPDATE raw_posts SET ocr_status = 'failed' WHERE id = ?",
                        (post["id"],),
                    )
                    log.debug("OCR failed for post %d", post["id"], exc_info=True)
                    continue

                if result.http_error:
                    total_http_errors += 1
                    conn.execute(
                        "UPDATE raw_posts SET ocr_status = 'failed' WHERE id = ?",
                        (result.post_id,),
                    )
                    log.warning(
                        "OCR all engines failed for post %d — marked failed, continuing",
                        result.post_id,
                    )
                    continue

                if result.text:
                    existing = post["caption"] or ""
                    if "🔤 On-screen text:" not in existing:
                        updated = existing + f"\n🔤 On-screen text: {result.text}"
                        conn.execute(
                            "UPDATE raw_posts SET caption = ?, ocr_status = 'done' WHERE id = ?",
                            (updated, result.post_id),
                        )
                    else:
                        conn.execute(
                            "UPDATE raw_posts SET ocr_status = 'done' WHERE id = ?",
                            (result.post_id,),
                        )
                    total_enriched += 1
                    log.info(
                        "OCR enriched post %d via %s (%d frames)",
                        result.post_id,
                        result.engine or "unknown",
                        result.frame_count,
                    )
                else:
                    # No text or download-only miss — mark done so we don't spin forever
                    status = "empty" if not result.download_error else "failed"
                    if result.download_error:
                        total_http_errors += 1
                    conn.execute(
                        "UPDATE raw_posts SET ocr_status = ? WHERE id = ?",
                        (status, result.post_id),
                    )
                    if status == "empty":
                        log.info(
                            "OCR post %d: no on-screen text (%s, %d frames)",
                            result.post_id,
                            result.engine or "unknown",
                            result.frame_count,
                        )

        conn.commit()

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
