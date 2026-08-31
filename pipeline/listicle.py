"""Deterministic place-name hints from listicle captions and on-screen/subtitle text.

Used as a supplement to LLM extraction so numbered "Top N restaurants" lists
and OCR/subtitle blocks are not lost when captions are long or the model skips
items. Does not invent places — only parses explicit name-like lines.
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
