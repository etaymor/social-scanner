"""Real photo sourcing via Google Places API (New).

Attempts to fetch an actual photo of a named venue.  Returns False when
no photo is found so the caller can fall back to AI generation.
"""

from __future__ import annotations

import logging
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image as PILImage

from config import GOOGLE_PLACES_API_KEY

log = logging.getLogger(__name__)

# Target dimensions for 9:16 portrait slideshow slides
TARGET_WIDTH = 1080
TARGET_HEIGHT = 1920

_PLACES_API_BASE = "https://places.googleapis.com/v1"
_TIMEOUT = 15  # seconds per request


def search_place_photo(
    place_name: str,
    city: str,
    output_path: Path,
    *,
    max_width_px: int = 1200,
) -> bool:
    """Search Google Places for a real photo and save it cropped to 9:16.

    Returns True if a photo was saved, False otherwise.
    """
    if not GOOGLE_PLACES_API_KEY:
        return False

    place_id = _find_place_id(f"{place_name}, {city}")
    if not place_id:
        log.debug("No Google Places result for '%s, %s'", place_name, city)
        return False

    photo_name = _get_photo_name(place_id)
    if not photo_name:
        log.debug("No photos available for place_id=%s", place_id)
        return False

    return _download_and_crop_photo(photo_name, output_path, max_width_px=max_width_px)


def _find_place_id(query: str) -> str | None:
    """Use Places API Text Search to find the place ID for a venue."""
    resp = requests.post(
        f"{_PLACES_API_BASE}/places:searchText",
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
            "X-Goog-FieldMask": "places.id",
        },
        json={"textQuery": query},
        timeout=_TIMEOUT,
    )
    if resp.status_code != 200:
        log.warning("Places Text Search failed (%d): %s", resp.status_code, resp.text[:200])
        return None

    places = resp.json().get("places", [])
    if not places:
        return None

    return places[0].get("id")


def _get_photo_name(place_id: str) -> str | None:
    """Get the best photo resource name from Place Details."""
    resp = requests.get(
        f"{_PLACES_API_BASE}/places/{place_id}",
        headers={
            "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
            "X-Goog-FieldMask": "photos",
        },
        timeout=_TIMEOUT,
    )
    if resp.status_code != 200:
        log.warning("Place Details failed (%d): %s", resp.status_code, resp.text[:200])
        return None

    photos = resp.json().get("photos", [])
    if not photos:
        return None

    return photos[0].get("name")


def _download_and_crop_photo(
    photo_name: str,
    output_path: Path,
    *,
    max_width_px: int = 1200,
) -> bool:
    """Download a photo from Google Places and crop to 9:16 portrait."""
    resp = requests.get(
        f"{_PLACES_API_BASE}/{photo_name}/media",
        headers={"X-Goog-Api-Key": GOOGLE_PLACES_API_KEY},
        params={"maxWidthPx": max_width_px, "skipHttpRedirect": "true"},
        timeout=_TIMEOUT,
    )
    if resp.status_code != 200:
        log.warning("Photo media request failed (%d): %s", resp.status_code, resp.text[:200])
        return False

    photo_url = resp.json().get("photoUri")
    if not photo_url:
        log.warning("No photoUri in media response")
        return False

    img_resp = requests.get(photo_url, timeout=30)
    if img_resp.status_code != 200:
        log.warning("Photo download failed (%d)", img_resp.status_code)
        return False

    try:
        img = PILImage.open(BytesIO(img_resp.content))
        img = _crop_to_portrait(img)

        dest = Path(output_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        img.save(dest, format="PNG")
        log.info("Real photo saved to %s (%dx%d)", dest, img.width, img.height)
        return True
    except Exception as e:
        log.warning("Failed to process downloaded photo: %s", e)
        return False


def _crop_to_portrait(img: PILImage.Image) -> PILImage.Image:
    """Center-crop and resize an image to 1080x1920 (9:16 portrait)."""
    target_ratio = TARGET_WIDTH / TARGET_HEIGHT  # 0.5625
    w, h = img.size
    current_ratio = w / h

    if current_ratio > target_ratio:
        # Image is wider than 9:16 — crop width
        new_w = int(h * target_ratio)
        left = (w - new_w) // 2
        img = img.crop((left, 0, left + new_w, h))
    else:
        # Image is taller than 9:16 — crop height
        new_h = int(w / target_ratio)
        top = (h - new_h) // 2
        img = img.crop((0, top, w, top + new_h))

    return img.resize((TARGET_WIDTH, TARGET_HEIGHT), PILImage.LANCZOS)
