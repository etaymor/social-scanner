"""Visual style palettes and prompt composition for scroll-stopping image generation.

Centralises all creative direction for slideshow images — variety palettes,
composition rules, negative guidance, deterministic style selection, and
weight-biased style selection for the analytics intelligence loop.
"""

import hashlib
import logging
import random
from typing import TypedDict

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------


class StyleOption(TypedDict):
    name: str
    desc: str


class SlideshowStyle(TypedDict):
    time_of_day: StyleOption
    weather: StyleOption
    perspective: StyleOption
    color_mood: StyleOption


class ImagePreset(TypedDict):
    name: str            # e.g. "bright_hazy_vista"
    style: str           # analytics key, e.g. "high_key_atmospheric_travel"
    template: str        # prompt with [INSERT SUBJECT HERE] placeholder
    negative_prompt: str


# ---------------------------------------------------------------------------
# Variety palettes
# ---------------------------------------------------------------------------

TIME_OF_DAY: list[StyleOption] = [
    {
        "name": "golden_hour",
        "desc": (
            "warm golden-hour side-lighting casting long amber shadows, "
            "sun low on the horizon painting everything in honey tones"
        ),
    },
    {
        "name": "blue_hour",
        "desc": (
            "cool blue-hour twilight with deep indigo sky, "
            "city lights just beginning to glow, a luminous gradient "
            "from cobalt overhead to warm amber at the horizon"
        ),
    },
    {
        "name": "overcast",
        "desc": (
            "soft overcast diffused light, rich saturated colours with "
            "no harsh shadows, even illumination that makes textures pop"
        ),
    },
    {
        "name": "harsh_midday",
        "desc": (
            "high-contrast midday sun with deep blacks and crisp highlights, "
            "sharp geometric shadows slicing across walls and pavement"
        ),
    },
    {
        "name": "night",
        "desc": (
            "warm tungsten street lighting mixed with neon reflections, "
            "deep shadows punctuated by pools of amber and electric light"
        ),
    },
    {
        "name": "morning_mist",
        "desc": (
            "early morning with atmospheric haze and soft directional rays "
            "filtering through mist, silhouettes emerging from a luminous fog"
        ),
    },
]

WEATHER_MOOD: list[StyleOption] = [
    {
        "name": "after_rain",
        "desc": (
            "wet cobblestones reflecting warm lights, glistening surfaces, "
            "puddle reflections doubling the scene"
        ),
    },
    {
        "name": "clear",
        "desc": (
            "crystal-clear air with vivid colours and sharp distant details, "
            "deep blue sky fading to pale near the horizon"
        ),
    },
    {
        "name": "humid",
        "desc": (
            "visible humidity haze softening the background, lush tropical greens "
            "amplified, a slight sheen on every surface"
        ),
    },
    {
        "name": "foggy",
        "desc": (
            "atmospheric fog creating natural depth layers, "
            "silhouettes and shapes emerging from soft white haze"
        ),
    },
    {
        "name": "dusty_warm",
        "desc": (
            "warm desert air with golden dust particles catching the light, "
            "a soft diffusion that turns the sky apricot"
        ),
    },
]

PERSPECTIVE: list[StyleOption] = [
    {
        "name": "street_level",
        "desc": (
            "low street-level perspective looking slightly up, converging "
            "vertical lines creating dramatic scale"
        ),
    },
    {
        "name": "cafe_window",
        "desc": (
            "shot through a rain-streaked or condensation-fogged window, "
            "bokeh foreground droplets framing the scene beyond"
        ),
    },
    {
        "name": "elevated_balcony",
        "desc": (
            "elevated perspective from a second-floor balcony or rooftop, "
            "looking down across rooftops and the street below"
        ),
    },
    {
        "name": "narrow_alley",
        "desc": (
            "compressed telephoto perspective down a narrow alley or corridor, "
            "stacked depth layers with walls closing in on both sides"
        ),
    },
    {
        "name": "over_shoulder",
        "desc": (
            "over-the-shoulder of an anonymous figure in the foreground, "
            "their silhouette naturally framing the destination beyond"
        ),
    },
    {
        "name": "reflection",
        "desc": (
            "reflected in a shop window, mirror, or still puddle, "
            "adding a dreamlike doubled composition"
        ),
    },
]

COLOR_MOOD: list[StyleOption] = [
    {
        "name": "warm_analog",
        "desc": (
            "warm analog film tones with lifted blacks, amber highlights, "
            "and slightly desaturated greens — like Kodak Portra 400"
        ),
    },
    {
        "name": "teal_orange",
        "desc": (
            "cinematic teal-and-orange colour grading, cool shadows "
            "contrasting warm skin and light tones"
        ),
    },
    {
        "name": "vivid_saturated",
        "desc": (
            "hyper-vivid saturated colours with punchy contrast, "
            "colours that pop on a phone screen, bold and unapologetic"
        ),
    },
    {
        "name": "muted_earth",
        "desc": (
            "muted earth tones with terracotta, sage, and cream, "
            "editorial matte finish like a Kinfolk magazine spread"
        ),
    },
    {
        "name": "neon_night",
        "desc": (
            "neon-soaked colour palette with magenta, cyan, and electric blue "
            "reflected on wet surfaces — cyberpunk without the fiction"
        ),
    },
]

# ---------------------------------------------------------------------------
# Incompatible combinations
# ---------------------------------------------------------------------------

_INCOMPATIBLE: set[tuple[str, str]] = {
    ("night", "morning_mist"),
    ("night", "clear"),
    ("morning_mist", "dusty_warm"),
    ("harsh_midday", "foggy"),
    ("harsh_midday", "after_rain"),
    ("golden_hour", "neon_night"),
    ("morning_mist", "neon_night"),
    ("overcast", "neon_night"),
}


_PRESET_USE_PROBABILITY = 0.7  # 70% chance to use preset, 30% composited


# ---------------------------------------------------------------------------
# Image presets — self-contained prompt templates per visual mood
# ---------------------------------------------------------------------------

IMAGE_PRESETS: list[ImagePreset] = [
    {
        "name": "bright_hazy_vista",
        "style": "high_key_atmospheric_travel",
        "template": (
            "[INSERT SUBJECT HERE] captured with a bright, high-key visual aesthetic. "
            "The image is dominated by a strong, diffused, high-angle light source "
            "(such as the sun through a hazy layer) that creates a soft, misty "
            "atmospheric haze, reducing background contrast. In stark, high-contrast "
            "opposition, the extreme foreground elements are sharply defined with very "
            "deep, almost pitch-black, crushed shadows. The foreground features "
            "intricate, complex textures (like dense, intricate patterns or materials) "
            "that appear sharp and detailed against the soft background. The entire "
            "composition has a cool, blue-ish environmental tone. High-resolution, "
            "professional travel-journalism style, 9:16 aspect ratio."
        ),
        "negative_prompt": (
            "oversaturated, golden hour, flat lighting, warm tones, blurry foreground"
        ),
    },
    {
        "name": "moody_symmetry",
        "style": "low_key_architectural",
        "template": (
            "A perfectly symmetrical, low-angle vertical photograph of "
            "[INSERT SUBJECT HERE]. The composition is defined by a rhythmic "
            "repetition of dark structural elements and high-contrast vaulted shapes "
            "that create a deep one-point perspective. The color palette is dominated "
            "by rich, dark earth tones and polished textures. Dramatic low-key "
            "lighting creates deep shadows and warm, focused highlights on "
            "architectural details. High-resolution textures, sharp focus throughout "
            "the frame, professional architectural photography style, 9:16 aspect ratio."
        ),
        "negative_prompt": (
            "bright, airy, flat lighting, outdoor daylight, cluttered, blurry, people"
        ),
    },
    {
        "name": "travel_aesthetic",
        "style": "photorealistic_travel",
        "template": (
            "[INSERT SUBJECT HERE] captured in a high-resolution cinematic travel "
            "photography style. Sharp foreground details with clear textures, warm "
            "directional sunlight from a low angle creating soft shadows. Vast, "
            "open-air composition with a soft atmospheric haze on the horizon "
            "transitioning into a deep, clear blue sky. Professional 35mm lens "
            "aesthetic, natural earth-tone color palette, 9:16 aspect ratio."
        ),
        "negative_prompt": (
            "oversaturated, blurry, distorted, crowded, urban clutter"
        ),
    },
]

PRESET_BY_NAME: dict[str, ImagePreset] = {p["name"]: p for p in IMAGE_PRESETS}

# Category → preferred preset name
CATEGORY_PRESET_MAP: dict[str, str] = {
    "outdoors_and_nature": "bright_hazy_vista",
    "sights_and_attractions": "moody_symmetry",
    "arts_and_culture": "moody_symmetry",
    "food_and_drink": "travel_aesthetic",
    "nightlife": "travel_aesthetic",
    "shopping": "travel_aesthetic",
    "places_to_stay": "travel_aesthetic",
    "activities_and_experiences": "travel_aesthetic",
}


def _is_compatible(style: SlideshowStyle) -> bool:
    """Check that no two selections clash."""
    names = [
        style["time_of_day"]["name"],
        style["weather"]["name"],
        style["color_mood"]["name"],
    ]
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            pair = tuple(sorted((a, b)))
            if pair in _INCOMPATIBLE:
                return False
    return True


# ---------------------------------------------------------------------------
# Composition & negative guidance
# ---------------------------------------------------------------------------

COMPOSITION_RULES = (
    "Strong foreground-midground-background depth separation. "
    "A clear single focal point with the eye drawn to it through leading lines or contrast. "
    "Foreground interest element (plant, railing, table edge, archway) creating natural framing. "
    "Vertical composition optimised for 9:16 portrait format — stack visual layers "
    "top-to-bottom, not left-to-right. Leave the upper 15% relatively clean for text overlay."
)

NEGATIVE_GUIDANCE = (
    "CRITICAL: Do NOT render any text, words, letters, numbers, signs, labels, "
    "captions, titles, or typography of any kind anywhere in the image. "
    "No watermarks, no logos, no UI elements, no borders, no signage. "
    "No people looking directly at camera, no posed selfies, no group photos. "
    "No oversaturated HDR look, no AI glow effect, no plastic skin texture. "
    "No symmetrical dead-centre composition, no flat frontal perspective. "
    "No stock photography poses or setups. No clipart or illustrated elements. "
    "No collage or split-screen layouts."
)

IMAGE_SYSTEM_PROMPT = (
    "You are generating photorealistic travel photography for a vertical 9:16 "
    "social media format. Every image must look like it was taken by a talented "
    "photographer with a high-end smartphone — never like AI art, stock photography, "
    "or digital illustration. Prioritise: natural imperfections, realistic light "
    "behaviour, authentic textures, and environmental storytelling. The viewer should "
    "feel like they could step into the scene."
)

# Universal negatives shared by both composited prompts and presets
NEGATIVE_GUIDANCE_CORE = (
    "CRITICAL: Do NOT render any text, words, letters, numbers, signs, labels, "
    "captions, titles, or typography of any kind anywhere in the image. "
    "No watermarks, no logos, no UI elements, no borders, no signage. "
    "No people looking directly at camera, no posed selfies, no group photos. "
    "No oversaturated HDR look, no AI glow effect, no plastic skin texture. "
    "No clipart or illustrated elements. No collage or split-screen layouts."
)

# ---------------------------------------------------------------------------
# Preset selection & prompt building
# ---------------------------------------------------------------------------


def select_preset_for_place(category: str) -> ImagePreset | None:
    """Pick an image preset for a place based on its category.

    Returns the matched :class:`ImagePreset` ~70% of the time and *None*
    (meaning fall back to composited style suffix) ~30% of the time.
    """
    preset_name = CATEGORY_PRESET_MAP.get(category)
    if not preset_name:
        return None
    if random.random() > _PRESET_USE_PROBABILITY:
        return None
    return PRESET_BY_NAME[preset_name]


def build_preset_prompt(preset: ImagePreset, subject: str) -> str:
    """Build a complete image generation prompt from a preset and subject.

    Replaces ``[INSERT SUBJECT HERE]`` in the preset template with *subject*
    (the enrichment image_prompt), then appends the preset's negative prompt
    and universal negative guidance.
    """
    body = preset["template"].replace("[INSERT SUBJECT HERE]", subject)
    return (
        f"{body} "
        f"AVOID: {preset['negative_prompt']}. "
        f"{NEGATIVE_GUIDANCE_CORE}"
    )


# ---------------------------------------------------------------------------
# Selection & assembly
# ---------------------------------------------------------------------------

_MAX_REROLLS = 20


def select_slideshow_style(city: str, date_str: str) -> SlideshowStyle:
    """Deterministically select a visual style combination for a slideshow.

    Seeded from *city + date_str* so re-runs on the same day produce the same
    style, but different cities or different dates get variety.

    Note: This is the deterministic (unweighted) version.  For weight-biased
    selection from the analytics intelligence loop, use
    :func:`select_weighted_style` instead.
    """
    seed = int(hashlib.sha256(f"{city.lower().strip()}:{date_str}".encode()).hexdigest(), 16)
    rng = random.Random(seed)

    for _ in range(_MAX_REROLLS):
        style: SlideshowStyle = {
            "time_of_day": rng.choice(TIME_OF_DAY),
            "weather": rng.choice(WEATHER_MOOD),
            "perspective": rng.choice(PERSPECTIVE),
            "color_mood": rng.choice(COLOR_MOOD),
        }
        if _is_compatible(style):
            return style

    # Fallback: safe combination
    return {
        "time_of_day": TIME_OF_DAY[0],  # golden_hour
        "weather": WEATHER_MOOD[1],  # clear
        "perspective": PERSPECTIVE[0],  # street_level
        "color_mood": COLOR_MOOD[0],  # warm_analog
    }


def select_weighted_style(weights: dict[str, dict[str, float]] | None = None) -> SlideshowStyle:
    """Select a visual style combination biased by performance weights.

    Unlike :func:`select_slideshow_style`, this version does NOT use
    city/date seeding.  Each call is intentionally random, biased by the
    per-axis weights from ``performance_weights.json``.

    Args:
        weights: Nested dict ``{dimension: {value: weight, ...}, ...}``
            as returned by ``intelligence.read_weights()``.  If *None* or
            empty, all options are equally likely.

    Returns:
        A :class:`SlideshowStyle` dict with compatible axis selections.
    """
    if weights is None:
        weights = {}

    def _pick_axis(axis_options: list[StyleOption], dim_key: str) -> StyleOption:
        dim_weights = weights.get(dim_key, {})
        names = [opt["name"] for opt in axis_options]
        w = [dim_weights.get(n, 1.0) for n in names]
        return random.choices(axis_options, weights=w, k=1)[0]

    for _ in range(_MAX_REROLLS):
        style: SlideshowStyle = {
            "time_of_day": _pick_axis(TIME_OF_DAY, "time_of_day"),
            "weather": _pick_axis(WEATHER_MOOD, "weather"),
            "perspective": _pick_axis(PERSPECTIVE, "perspective"),
            "color_mood": _pick_axis(COLOR_MOOD, "color_mood"),
        }
        if _is_compatible(style):
            log.debug(
                "Weighted style selected: %s + %s + %s + %s",
                style["time_of_day"]["name"],
                style["weather"]["name"],
                style["perspective"]["name"],
                style["color_mood"]["name"],
            )
            return style

    # Fallback: safe combination
    log.warning("Max rerolls reached in select_weighted_style, using fallback")
    return {
        "time_of_day": TIME_OF_DAY[0],  # golden_hour
        "weather": WEATHER_MOOD[1],  # clear
        "perspective": PERSPECTIVE[0],  # street_level
        "color_mood": COLOR_MOOD[0],  # warm_analog
    }


def get_perspectives_for_slides(city: str, date_str: str, count: int) -> list[StyleOption]:
    """Return a shuffled list of perspectives for per-slide rotation.

    Each location slide gets a different camera perspective while the rest of
    the visual style stays consistent.  Deterministic for the same inputs.
    """
    seed = int(hashlib.sha256(f"{city.lower().strip()}:{date_str}:perspectives".encode()).hexdigest(), 16)
    rng = random.Random(seed)
    pool = list(PERSPECTIVE)
    rng.shuffle(pool)
    # Cycle through if more slides than perspectives
    return [pool[i % len(pool)] for i in range(count)]


def build_location_style_suffix(style: SlideshowStyle, perspective_override: StyleOption | None = None) -> str:
    """Compose the full style suffix appended to each location image prompt."""
    perspective = perspective_override or style["perspective"]
    return (
        f"Photorealistic travel photograph. "
        f"{style['time_of_day']['desc']}. "
        f"{style['weather']['desc']}. "
        f"{perspective['desc']}. "
        f"{style['color_mood']['desc']}. "
        f"{COMPOSITION_RULES} "
        f"{NEGATIVE_GUIDANCE}"
    )


def build_hook_style_block(style: SlideshowStyle) -> str:
    """Compose the style block appended to hook image prompts."""
    return (
        f"Photorealistic travel photograph. "
        f"{style['time_of_day']['desc']}. "
        f"{style['weather']['desc']}. "
        f"{style['color_mood']['desc']}. "
        f"{COMPOSITION_RULES} "
        f"{NEGATIVE_GUIDANCE}"
    )
