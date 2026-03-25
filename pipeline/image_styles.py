"""Visual style palettes and prompt composition for scroll-stopping image generation.

Centralises all creative direction for slideshow images — variety palettes,
composition rules, negative guidance, and deterministic style selection.
"""

import hashlib
import random
from typing import TypedDict

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
    "No text, no watermarks, no logos, no UI elements, no borders. "
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

# ---------------------------------------------------------------------------
# Selection & assembly
# ---------------------------------------------------------------------------

_MAX_REROLLS = 20


def select_slideshow_style(city: str, date_str: str) -> SlideshowStyle:
    """Deterministically select a visual style combination for a slideshow.

    Seeded from *city + date_str* so re-runs on the same day produce the same
    style, but different cities or different dates get variety.
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
