"""Deterministic place-name hints from listicle captions and on-screen/subtitle text.

Used as a supplement to LLM extraction so numbered "Top N restaurants" lists
and OCR/subtitle blocks are not lost when captions are long or the model skips
items. Does not invent places — only parses explicit name-like lines.

On-screen OCR (🔤) is treated as noisy overlay chrome: floor numbers, city-only
labels, English filler, and dish generics without a venue are dropped. Real
venue names in the same block are kept. Never fail-closed — empty results are OK.
"""

from __future__ import annotations

import re

# Numbered list: "1. Isaac Toast — Breakfast" / "3) Hanmiok"
_NUMBERED = re.compile(r"(?m)^\s*\d{1,2}[\.)]\s+(.+)$")

# On-screen / subtitle blocks appended by OCR or subtitle enrichment
_BLOCK = re.compile(
    r"(?:🔤 On-screen text:|🎙 Subtitles:)\s*(.*?)(?=\n(?:📍|🔤|🎙)|$)",
    re.DOTALL,
)

_LOCATION = re.compile(r"📍 Location tag:\s*([^\n]+)")

_SKIP = frozenset(
    {
        "seoul",
        "tokyo",
        "korea",
        "south korea",
        "japan",
        "hongdae",
        "itaewon",
        "myeongdong",
        "gangnam",
        "shibuya",
        "shinjuku",
    }
)

# Building-level chrome from map pins / storefront overlays: 1F, 2F, B1, 2F-3F, 2F 3F
_FLOOR_ONLY = re.compile(
    r"^(?:"
    r"[Bb]\d{1,2}"
    r"|\d{1,2}\s*[Ff]"
    r")(?:\s*[-–—~/to]+\s*(?:[Bb]?\d{1,2}\s*[Ff]?))?(?:\s+(?:[Bb]?\d{1,2}\s*[Ff]?))*$",
    re.I,
)

# City-only overlay labels: "SEOUL", "IN SEOUL", "IN TOKYO"
_CITY_ONLY = re.compile(
    r"^(?:in\s+)?(?:"
    r"seoul|tokyo|osaka|busan|kyoto|yokohama|nagoya|fukuoka|"
    r"korea|japan|south\s+korea"
    r")$",
    re.I,
)

# English filler / reaction chrome commonly burned into food TikToks
_FILLER = frozenset(
    {
        "here",
        "delicious",
        "yummy",
        "tasty",
        "amazing",
        "so good",
        "so delicious",
        "must try",
        "must eat",
        "best",
        "recommend",
        "recommended",
        "save this",
        "save for later",
        "follow",
        "like",
        "comment",
        "subscribe",
        "next",
        "look",
        "wow",
        "omg",
        "food",
        "eat",
        "try this",
        "this place",
        "this spot",
        "wait for it",
        "let's go",
        "lets go",
    }
)

# Dish / cuisine phrases that are not venues when they appear alone (no proper name)
_DISH_GENERIC = frozenset(
    {
        "naengmyeon",
        "bibimbap",
        "bulgogi",
        "dakgalbi",
        "seolleongtang",
        "tteokbokki",
        "kimbap",
        "gimbap",
        "kimchi",
        "galbi",
        "samgyeopsal",
        "jjajangmyeon",
        "tangsuyuk",
        "korean beef barbecue",
        "korean beef bbq",
        "korean barbecue",
        "korean bbq",
        "beef barbecue",
        "beef bbq",
        "fried chicken",
        "hot pot",
        "ramen",
        "sushi",
        "udon",
        "soba",
        "tonkatsu",
        "okonomiyaki",
        "takoyaki",
        "salt bread",
        "pot rice",
        "brisket",
        "hanwoo",
        "cold noodles",
        "korean food",
        "korean comfort food",
        "traditional korean food",
        "spicy stir-fried chicken",
        "soy marinated raw crab",
        "ox bone soup",
        "breakfast sandwich",
    }
)

# Longest dish phrases first so "korean beef barbecue" wins over "barbecue"
_DISH_GENERIC_SORTED = tuple(sorted(_DISH_GENERIC, key=len, reverse=True))

# Modifiers / chrome around a dish head that still leave no venue proper name
_DISH_HEAD_NOISE = frozenset(
    {
        "mul",
        "bibim",
        "hoe",
        "spicy",
        "hot",
        "cold",
        "fried",
        "grilled",
        "and",
        "even",
        "some",
        "the",
        "a",
        "an",
        "with",
        "of",
        "my",
        "our",
        "best",
        "famous",
        "classic",
        "traditional",
        "fresh",
        "good",
        "great",
        "try",
        "tried",
        "eating",
        "eat",
        "had",
        "was",
        "is",
        "so",
        "very",
        "really",
        "also",
        "just",
        "only",
        "order",
        "ordered",
        "get",
        "got",
        "this",
        "that",
        "their",
        "its",
    }
)

# Score / rating chrome: "10/10", "5/5", "100%", standalone digits
_RATING_TOKEN = re.compile(r"^\d+(?:[./]\d+)?%?$")


def _dish_generic_as_head_without_venue(lower: str) -> bool:
    """True when *lower* is a dish phrase (optionally with modifiers) and no venue.

    Catches ``and even naengmyeon``, ``10/10 mul naengmyeon``, ``mul naengmyeon``.
    Keeps names that still have a distinct proper-name token after the dish is
    stripped (e.g. ``Myeongdong Kyoja`` alone, or ``Kyoja naengmyeon``).
    """
    for dish in _DISH_GENERIC_SORTED:
        if not re.search(rf"(?<!\w){re.escape(dish)}(?!\w)", lower):
            continue
        remainder = re.sub(rf"(?<!\w){re.escape(dish)}(?!\w)", " ", lower)
        tokens = [t for t in re.split(r"[^a-z0-9가-힣]+", remainder) if t]
        meaningful: list[str] = []
        for tok in tokens:
            if tok in _DISH_HEAD_NOISE or tok in _FILLER or tok in _SKIP:
                continue
            if _RATING_TOKEN.match(tok):
                continue
            if len(tok) < 2:
                continue
            meaningful.append(tok)
        if not meaningful:
            return True
    return False


def is_overlay_junk(name: str) -> bool:
    """True if *name* is OCR/overlay chrome, not a venue.

    Drops floor numbers, city-only labels, English filler, and dish generics
    without a venue (including dish-as-head lines like ``mul naengmyeon``).
    Proper names (e.g. ``Myeongdong Kyoja``) return False.
    """
    cleaned = re.sub(r"\s+", " ", name).strip()
    if not cleaned:
        return True
    lower = cleaned.lower()
    if lower in _SKIP or lower in _FILLER or lower in _DISH_GENERIC:
        return True
    if _FLOOR_ONLY.match(cleaned):
        return True
    if _CITY_ONLY.match(cleaned):
        return True
    if _dish_generic_as_head_without_venue(lower):
        return True
    return False


def _clean_name(raw: str) -> str | None:
    # Strip trailing "— description" / "- description" / "(branch)"
    name = re.split(r"\s*[—–\|]\s*", raw, maxsplit=1)[0]
    name = re.sub(r"\([^)]*branch[^)]*\)", "", name, flags=re.I)
    name = re.sub(r"\s+", " ", name).strip(" \t.•,;:|/\\")
    # Drop recipe/instruction lines
    if len(name) < 2 or len(name) > 80:
        return None
    if name.lower() in _SKIP:
        return None
    if re.match(r"^(mix|add|boil|bake|let|refrigerate|here are|number)\b", name, re.I):
        return None
    if is_overlay_junk(name):
        return None
    return name


def extract_named_places_heuristic(text: str) -> list[dict[str, str]]:
    """Return ``{"name", "type", "category"}`` dicts found in *text*.

    Prefers restaurant typing for food-list context; otherwise ``other``.
    """
    if not text or not text.strip():
        return []

    foodish = bool(
        re.search(
            r"restaurant|must eat|food guide|맛집|eats|ramen|bbq|cafe|bakery|"
            r"viral spots|foodreview|ganjang|brisket",
            text,
            re.I,
        )
    )
    default_type = "restaurant" if foodish else "other"
    default_cat = "food_and_drink" if foodish else "sights_and_attractions"

    found: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(name: str, place_type: str = default_type, category: str = default_cat) -> None:
        cleaned = _clean_name(name)
        if not cleaned:
            return
        # Drop planner / UI chrome, recipe steps, and our own enrichment labels
        if re.search(
            r"\b(choose your|select the|pick your|cover and|roll tightly|"
            r"place in a|cut into|let rise|grammys|oscars|amas|"
            r"location tag|subtitles|on-screen text)\b",
            cleaned,
            re.I,
        ):
            return
        if cleaned.lower() in {"location tag", "subtitles", "on-screen text"}:
            return
        key = cleaned.lower()
        if key in seen:
            return
        seen.add(key)
        found.append({"name": cleaned, "type": place_type, "category": category})

    for m in _NUMBERED.finditer(text):
        add(m.group(1))

    # Decorative list markers: "˖ ࣪⭑ Solsot: description" / "★ Hanmiok: …"
    # Skip lines that are our own 📍 Location tag / enrichment prefixes.
    for m in re.finditer(
        r"(?m)^(?!📍)[^\w\n]{0,8}([A-Z][A-Za-z0-9][A-Za-z0-9\'’&\-\s]{1,40}?):\s+\S",
        text,
    ):
        add(m.group(1))

    # Inline bullet lists on a single line:
    # "Hanmiok Brisket BBQ • HangJungSun • Solsot Pot Rice"
    for line in text.splitlines():
        if "•" not in line and "·" not in line:
            continue
        parts = re.split(r"[•·]", line)
        if len(parts) < 2:
            continue
        for part in parts:
            part = part.strip()
            if 2 <= len(part) <= 60 and re.match(r"^[A-Za-z가-힣]", part):
                if part.count(" ") <= 6 and not part.endswith("."):
                    add(part)

    for m in _LOCATION.finditer(text):
        loc = m.group(1).strip()
        if loc.lower() in _SKIP:
            continue
        add(loc.split(",")[0].strip(), place_type="other", category="sights_and_attractions")

    # Map-pin lines like "📍Odarijip (Hongdae branch)" — skip our own "📍 Location tag:" lines
    for m in re.finditer(r"📍(?!\s*Location tag:)\s*([^\n,]+)", text):
        add(m.group(1), place_type=default_type, category=default_cat)

    for m in _BLOCK.finditer(text):
        block = m.group(1)
        for line in block.splitlines():
            line = line.strip()
            if not line or line == "NO_TEXT":
                continue
            nm = re.match(r"^\d{1,2}[\.)]\s+(.+)$", line)
            if nm:
                add(nm.group(1))
            elif "•" in line:
                for part in line.split("•"):
                    add(part)
            elif len(line) <= 60 and not line.endswith("."):
                add(line)

    return found
