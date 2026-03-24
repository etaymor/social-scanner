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

# Database
DB_PATH = Path(os.getenv("DB_PATH", "places.db"))

# Place types
VALID_PLACE_TYPES = frozenset({
    "restaurant", "cafe", "bar", "club", "market", "neighborhood",
    "viewpoint", "park", "museum", "gallery", "shop", "activity",
    "street", "other",
})
