"""Configuration loading and constants."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# API Keys
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")

# OpenRouter
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "anthropic/claude-sonnet-4")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MAX_RETRIES = 3
OPENROUTER_RETRY_BASE_DELAY = 2  # seconds

# Apify actor IDs
TIKTOK_ACTOR = "clockworks/free-tiktok-scraper"
INSTAGRAM_ACTOR = "apify/instagram-hashtag-scraper"

# Pipeline defaults
DEFAULT_MAX_POSTS = 100
EXTRACTION_BATCH_SIZE = 20
FILTER_BATCH_SIZE = 50
DEDUP_SCORE_CUTOFF = 85  # rapidfuzz token_sort_ratio threshold
DEDUP_RELATIVE_THRESHOLD = 0.3  # max normalized distance for merge candidates

# Engagement minimums — skip low-quality posts to save Apify credits
MIN_VIEWS_TIKTOK = 1000
MIN_LIKES_TIKTOK = 50
MIN_VIEWS_INSTAGRAM = 500
MIN_LIKES_INSTAGRAM = 20

# Virality score weights
WEIGHT_SAVES = 5.0
WEIGHT_SHARES = 4.0
WEIGHT_COMMENTS = 2.0
WEIGHT_LIKES = 1.0

# Google Places API (real photo sourcing — falls back to AI if unset)
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

# Gemini (image generation via OpenRouter)
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "google/gemini-3.1-flash-image-preview")
GEMINI_MAX_RETRIES = 2
GEMINI_TIMEOUT = 120  # seconds

# Postiz (TikTok posting)
POSTIZ_API_KEY = os.getenv("POSTIZ_API_KEY", "")
POSTIZ_BASE_URL = "https://api.postiz.com/public/v1"
POSTIZ_TIKTOK_INTEGRATION_ID = os.getenv("POSTIZ_TIKTOK_INTEGRATION_ID", "")
POSTIZ_UPLOAD_DELAY = 1.5  # seconds between uploads (rate-limit buffer)

# Slideshow generation
SLIDESHOW_OUTPUT_DIR = Path(os.getenv("SLIDESHOW_OUTPUT_DIR", "slideshows"))
PLACE_REUSE_COOLDOWN_DAYS = 30

# Database
DB_PATH = Path(os.getenv("DB_PATH", "places.db"))

# Place types (derived after CATEGORIES below)

# Categories
CATEGORIES = {
    "food_and_drink": {
        "label": "Food & Drink",
        "description": "Restaurants, cafes, and bakeries",
        "types": ["restaurant", "cafe", "bakery"],
    },
    "places_to_stay": {
        "label": "Places to Stay",
        "description": "Hotels, hostels, and unique accommodations",
        "types": ["hotel", "hostel"],
    },
    "sights_and_attractions": {
        "label": "Sights & Attractions",
        "description": "Viewpoints, neighborhoods, streets, monuments, and temples",
        "types": ["viewpoint", "neighborhood", "street", "monument", "temple"],
    },
    "nightlife": {
        "label": "Nightlife",
        "description": "Bars, clubs, lounges, and breweries",
        "types": ["bar", "club", "lounge", "brewery"],
    },
    "shopping": {
        "label": "Shopping",
        "description": "Shops, markets, and boutiques",
        "types": ["shop", "market", "boutique"],
    },
    "outdoors_and_nature": {
        "label": "Outdoors & Nature",
        "description": "Parks, beaches, gardens, and trails",
        "types": ["park", "beach", "garden", "trail"],
    },
    "arts_and_culture": {
        "label": "Arts & Culture",
        "description": "Museums, galleries, and theaters",
        "types": ["museum", "gallery", "theater"],
    },
    "activities_and_experiences": {
        "label": "Activities & Experiences",
        "description": "Tours, classes, spas, and workshops",
        "types": ["activity", "tour", "class", "spa", "workshop"],
    },
}

VALID_CATEGORIES = frozenset(CATEGORIES.keys())
DEFAULT_CATEGORY = "sights_and_attractions"

# Derived from CATEGORIES — single source of truth for type→category mapping
TYPE_TO_CATEGORY = {
    place_type: cat_key
    for cat_key, cat_val in CATEGORIES.items()
    for place_type in cat_val["types"]
}

VALID_PLACE_TYPES = frozenset(TYPE_TO_CATEGORY.keys()) | {"other"}

CATEGORY_HASHTAG_SEEDS = {
    "food_and_drink": {
        "suffixes": ["food", "foodie", "eats", "restaurants", "cafes"],
        "tags": ["hiddenfoodie", "localeats", "foodfinds"],
    },
    "places_to_stay": {
        "suffixes": ["hotels", "stays", "accommodation"],
        "tags": ["boutiquehotels", "uniquestays", "hiddenhotels"],
    },
    "sights_and_attractions": {
        "suffixes": ["sights", "views", "landmarks"],
        "tags": ["hiddensights", "secretspots", "offthebeatenpath"],
    },
    "nightlife": {
        "suffixes": ["nightlife", "bars", "clubs", "nightout"],
        "tags": ["hiddenbars", "secretbars", "localnightlife"],
    },
    "shopping": {
        "suffixes": ["shopping", "markets", "shops"],
        "tags": ["localshopping", "hiddenmarkets", "boutiqueshopping"],
    },
    "outdoors_and_nature": {
        "suffixes": ["nature", "outdoors", "hiking", "parks"],
        "tags": ["hiddennature", "secretgardens", "localtrails"],
    },
    "arts_and_culture": {
        "suffixes": ["art", "culture", "museums", "galleries"],
        "tags": ["hiddenart", "localculture", "secretgalleries"],
    },
    "activities_and_experiences": {
        "suffixes": ["activities", "experiences", "tours", "workshops"],
        "tags": ["localexperiences", "hiddentours", "uniqueactivities"],
    },
}
