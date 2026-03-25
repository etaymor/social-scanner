"""Shared data contracts for the slideshow generation pipeline."""

from dataclasses import dataclass, asdict
from pathlib import Path
import json


# ---------------------------------------------------------------------------
# Slide text dataclasses
# ---------------------------------------------------------------------------

@dataclass
class HookSlideText:
    """Opening hook slide — grabs attention."""

    type: str = "hook"
    text: str = ""  # hook text with \n line breaks

    def __post_init__(self) -> None:
        if self.type != "hook":
            raise ValueError(f"HookSlideText.type must be 'hook', got {self.type!r}")


@dataclass
class LocationSlideText:
    """One location slide in a listicle or story."""

    type: str = "location"
    name: str = ""  # place name
    neighborhood: str = ""  # neighborhood / district
    number: str = ""  # e.g. "1/8"

    def __post_init__(self) -> None:
        if self.type != "location":
            raise ValueError(f"LocationSlideText.type must be 'location', got {self.type!r}")


@dataclass
class CTASlideText:
    """Final call-to-action slide."""

    type: str = "cta"
    text: str = ""  # CTA text

    def __post_init__(self) -> None:
        if self.type != "cta":
            raise ValueError(f"CTASlideText.type must be 'cta', got {self.type!r}")


# Union of all slide-text types
SlideText = HookSlideText | LocationSlideText | CTASlideText


# ---------------------------------------------------------------------------
# Pipeline metadata dataclasses
# ---------------------------------------------------------------------------

@dataclass
class SlideshowMeta:
    """Top-level metadata written to meta.json alongside the slides."""

    city: str
    category: str | None
    format: str  # "listicle" or "story" — name kept for JSON/DB compatibility
    hook_text: str
    slide_count: int
    created_at: str
    places: list[dict]  # [{"id": 1, "name": "...", "neighborhood": "..."}]


@dataclass
class PostMeta:
    """Metadata recorded after a slideshow is posted."""

    postiz_post_id: str
    posted_at: str
    platform: str = "tiktok"
    privacy_level: str = "SELF_ONLY"


# ---------------------------------------------------------------------------
# Serialization helpers — texts.json
# ---------------------------------------------------------------------------

_TYPE_MAP: dict[str, type[SlideText]] = {
    "hook": HookSlideText,
    "location": LocationSlideText,
    "cta": CTASlideText,
}


def to_texts_json(slides: list[SlideText]) -> str:
    """Serialize a list of SlideText objects to a JSON string."""
    return json.dumps([asdict(s) for s in slides], indent=2, ensure_ascii=False)


def from_texts_json(path: str | Path) -> list[SlideText]:
    """Deserialize a texts.json file into the correct SlideText subtypes."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    result: list[SlideText] = []
    for item in raw:
        cls = _TYPE_MAP.get(item.get("type", ""))
        if cls is None:
            raise ValueError(f"Unknown slide type: {item.get('type')!r}")
        # Only pass keys that the dataclass expects
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
        result.append(cls(**{k: v for k, v in item.items() if k in valid_keys}))
    return result


# ---------------------------------------------------------------------------
# Serialization helpers — meta.json
# ---------------------------------------------------------------------------

def to_meta_json(meta: SlideshowMeta) -> str:
    """Serialize a SlideshowMeta to a JSON string."""
    return json.dumps(asdict(meta), indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Serialization helpers — post_meta.json
# ---------------------------------------------------------------------------

def save_post_meta(meta: PostMeta, path: str | Path) -> None:
    """Save a PostMeta to a JSON file."""
    Path(path).write_text(
        json.dumps(asdict(meta), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def load_post_meta(path: str | Path) -> PostMeta:
    """Load a PostMeta from a JSON file."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return PostMeta(**data)


