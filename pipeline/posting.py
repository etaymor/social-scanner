"""Postiz API integration for posting slideshows to TikTok."""

import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

import config
from pipeline.retry import retry_with_backoff
from pipeline.slideshow_types import PostMeta, save_post_meta, load_post_meta

log = logging.getLogger(__name__)

# Retry settings
MAX_RETRIES = 2
RETRY_BASE_DELAY = 2  # seconds


class PostingError(Exception):
    pass


class PostingAuthError(PostingError):
    """Non-retryable auth failure (401/403)."""
    pass


def upload_image(api_key: str, image_path: str | Path) -> dict:
    """Upload an image to Postiz and return the response JSON.

    Retries on 5xx/timeout with exponential backoff (max 2 retries).
    Raises PostingAuthError on 401/403.
    Raises PostingError on other 4xx.
    """
    url = f"{config.POSTIZ_BASE_URL}/upload"
    headers = {"Authorization": f"Bearer {api_key}"}
    image_path = Path(image_path)

    def _do_upload():
        with open(image_path, "rb") as f:
            resp = requests.post(
                url,
                headers=headers,
                files={"file": (image_path.name, f, "image/png")},
                timeout=60,
            )
        if resp.status_code in (401, 403):
            raise PostingAuthError(
                f"Upload auth failed (HTTP {resp.status_code})"
            )
        if 400 <= resp.status_code < 500:
            raise PostingError(
                f"Upload failed (HTTP {resp.status_code}): {resp.text}"
            )
        resp.raise_for_status()
        return resp.json()

    try:
        return retry_with_backoff(
            _do_upload,
            max_retries=MAX_RETRIES + 1,
            base_delay=RETRY_BASE_DELAY,
            non_retryable=(PostingAuthError, PostingError),
        )
    except (PostingAuthError, PostingError):
        raise
    except Exception as e:
        raise PostingError(
            f"Upload of {image_path} failed after {MAX_RETRIES + 1} attempts: {e}"
        ) from e


def create_tiktok_post(
    api_key: str,
    integration_id: str,
    image_paths: list[str],
    caption: str,
) -> str:
    """Create a TikTok carousel post on Postiz and return the post ID.

    Retries on 5xx/timeout with exponential backoff (max 2 retries).
    Raises PostingAuthError on 401/403.
    Raises PostingError on other 4xx.
    """
    url = f"{config.POSTIZ_BASE_URL}/posts"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "type": "carousel",
        "integration_id": integration_id,
        "content": caption,
        "media": image_paths,
        "settings": {
            "privacy_level": "SELF_ONLY",
            "autoAddMusic": "no",
            "video_made_with_ai": True,
        },
    }

    def _do_create():
        resp = requests.post(
            url, headers=headers, json=payload, timeout=60,
        )
        if resp.status_code in (401, 403):
            raise PostingAuthError(
                f"Post creation auth failed (HTTP {resp.status_code})"
            )
        if 400 <= resp.status_code < 500:
            raise PostingError(
                f"Post creation failed (HTTP {resp.status_code}): {resp.text}"
            )
        resp.raise_for_status()
        return resp.json()["id"]

    try:
        return retry_with_backoff(
            _do_create,
            max_retries=MAX_RETRIES + 1,
            base_delay=RETRY_BASE_DELAY,
            non_retryable=(PostingAuthError, PostingError),
        )
    except (PostingAuthError, PostingError):
        raise
    except Exception as e:
        raise PostingError(
            f"Post creation failed after {MAX_RETRIES + 1} attempts: {e}"
        ) from e


def post_slideshow(output_dir: str | Path, caption: str) -> PostMeta:
    """Upload slide images and create a TikTok draft post via Postiz.

    If post_meta.json already exists in output_dir, logs and returns the
    existing metadata without re-posting.
    """
    output_dir = Path(output_dir)
    post_meta_path = output_dir / "post_meta.json"

    # Don't re-post if already done
    if post_meta_path.exists():
        log.info("Post already exists at %s — skipping", post_meta_path)
        return load_post_meta(post_meta_path)

    # Discover slide images sorted numerically (only final overlays, not raw/hook/cta variants)
    slide_files = sorted(
        (p for p in output_dir.glob("slide_*.png")
         if re.fullmatch(r"slide_\d+\.png", p.name)),
        key=lambda p: int(re.search(r"slide_(\d+)", p.stem).group(1)),
    )
    if not slide_files:
        raise PostingError(f"No slide_*.png files found in {output_dir}")

    log.info("Found %d slides to upload in %s", len(slide_files), output_dir)

    api_key = config.POSTIZ_API_KEY
    if not api_key:
        raise PostingError("POSTIZ_API_KEY not set in environment")

    # Upload each image
    uploaded_paths: list[str] = []
    for i, slide_path in enumerate(slide_files, start=1):
        log.info("Uploading slide %d/%d: %s", i, len(slide_files), slide_path.name)
        result = upload_image(api_key, slide_path)
        uploaded_paths.append(result["path"])
        # Rate-limit buffer between uploads (skip after last)
        if i < len(slide_files):
            time.sleep(config.POSTIZ_UPLOAD_DELAY)

    # Create the post
    log.info("Creating TikTok post with %d images", len(uploaded_paths))
    post_id = create_tiktok_post(
        api_key,
        config.POSTIZ_TIKTOK_INTEGRATION_ID,
        uploaded_paths,
        caption,
    )

    # Save metadata
    meta = PostMeta(
        postiz_post_id=post_id,
        posted_at=datetime.now(timezone.utc).isoformat(),
        platform="tiktok",
        privacy_level="SELF_ONLY",
    )
    save_post_meta(meta, post_meta_path)
    log.info("Post created: %s (meta saved to %s)", post_id, post_meta_path)

    return meta
