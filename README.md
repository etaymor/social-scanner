# Atlasi Place Discovery Pipeline

Discover trending, non-obvious places in any city by scraping TikTok and Instagram via Apify, extracting place names with an LLM, and scoring them by virality.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Dashboard](#dashboard)
- [Running Tests](#running-tests)
- [Project Structure](#project-structure)
- [Pipeline Architecture](#pipeline-architecture)
- [Categories](#categories)
- [API Endpoints](#api-endpoints)
- [Database](#database)
- [Costs](#costs)
- [Troubleshooting](#troubleshooting)

## Prerequisites

- **Python 3.11+** (developed on 3.14)
- **Apify account** — for TikTok and Instagram scraping ([sign up](https://apify.com/))
- **OpenRouter account** — for LLM calls ([sign up](https://openrouter.ai/))

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/etaymor/social-scanner.git
   cd social-scanner
   ```

2. **Create and activate a virtual environment:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate        # macOS / Linux
   # .venv\Scripts\activate          # Windows
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

   This installs:
   | Package | Purpose |
   |---|---|
   | `apify-client` | Apify API client for TikTok/Instagram scraping |
   | `requests` | HTTP requests to OpenRouter API |
   | `python-dotenv` | Load environment variables from `.env` |
   | `rapidfuzz` | Fuzzy string matching for place deduplication |
   | `flask` | Local web dashboard |
   | `pytest` | Test framework |

## Configuration

1. **Copy the example environment file:**

   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your API keys:**

   ```env
   APIFY_API_TOKEN=your_apify_token
   OPENROUTER_API_KEY=your_openrouter_key
   OPENROUTER_MODEL=anthropic/claude-sonnet-4
   ```

   | Variable | Required | Default | Description |
   |---|---|---|---|
   | `APIFY_API_TOKEN` | Yes | — | Your Apify API token |
   | `OPENROUTER_API_KEY` | Yes | — | Your OpenRouter API key |
   | `OPENROUTER_MODEL` | No | `anthropic/claude-sonnet-4` | LLM model to use via OpenRouter |
   | `DB_PATH` | No | `places.db` | Path to the SQLite database file |
   | `FLASK_DEBUG` | No | `false` | Set to `true` for Flask debug mode |

## Running the Pipeline

The CLI entry point is `discover.py`. It runs a 5-step discovery pipeline for a given city.

### Basic usage

```bash
python discover.py --city "Istanbul"
```

### All CLI options

```bash
python discover.py --city "Istanbul" [OPTIONS]
```

| Flag | Description |
|---|---|
| `--city CITY` | **(required)** City name to research |
| `--category CATEGORY` | Focus on a specific category (see [Categories](#categories)) |
| `--max-posts N` | Max posts per hashtag per platform (default: 100) |
| `--skip-scrape` | Skip Apify scraping, re-run extraction on existing data |
| `--reset` | Clear all data for this city before running |
| `--export-csv` | Export results to a CSV file |
| `--verbose` | Debug-level logging |
| `--quiet` | Minimal output (warnings and errors only) |

### Examples

```bash
# Discover food spots in Tokyo, limit to 50 posts per hashtag
python discover.py --city "Tokyo" --category food_and_drink --max-posts 50

# Re-run extraction without re-scraping (saves Apify credits)
python discover.py --city "Istanbul" --skip-scrape

# Start fresh for a city
python discover.py --city "Istanbul" --reset

# Export results to CSV
python discover.py --city "Istanbul" --export-csv

# Verbose output for debugging
python discover.py --city "Istanbul" --verbose
```

**Re-runs are safe.** The pipeline is fully resumable — if interrupted, it picks up where it left off without duplicating work.

## Dashboard

Browse results in a local web dashboard:

```bash
python dashboard.py
```

Open **http://localhost:5555** in your browser.

Dashboard features:
- Switch between cities
- Filter by category or place type
- Search places by name
- Pagination (50 items per page)
- View place type distribution

## Running Tests

```bash
# Run the full test suite
python -m pytest tests/ -v

# Run a specific test file
python -m pytest tests/test_category.py -v

# Run with coverage (requires pytest-cov)
python -m pytest tests/ -v --cov=pipeline
```

Test files:
| File | What it tests |
|---|---|
| `test_pipeline.py` | Full pipeline integration (mocked APIs) |
| `test_category.py` | Category validation, CLI parsing, dashboard filtering |
| `test_llm.py` | OpenRouter LLM wrapper and retry logic |
| `test_scorer.py` | Virality scoring and deduplication |
| `test_scraper.py` | Apify scraper response mapping |

## Project Structure

```
social-scanner/
├── discover.py              # CLI entry point — runs the 5-step pipeline
├── dashboard.py             # Flask web dashboard (http://localhost:5555)
├── config.py                # Configuration, constants, and category definitions
├── requirements.txt         # Python dependencies
├── .env.example             # Environment variable template
├── places.db                # SQLite database (auto-created on first run)
├── pipeline/                # Core pipeline modules
│   ├── __init__.py
│   ├── db.py                # Database schema, queries, and migrations
│   ├── hashtags.py          # Step 1: LLM hashtag generation
│   ├── scraper.py           # Step 2: Apify TikTok/Instagram scraping
│   ├── extractor.py         # Step 3: LLM place extraction from captions
│   ├── scorer.py            # Step 4: Fuzzy dedup + virality scoring
│   ├── filter.py            # Step 5: LLM tourist trap classification
│   └── llm.py               # OpenRouter LLM wrapper with retries
├── templates/
│   └── dashboard.html       # Dashboard UI template
├── tests/                   # Test suite
│   ├── conftest.py          # Shared fixtures (in-memory DB, test city)
│   ├── test_pipeline.py
│   ├── test_category.py
│   ├── test_llm.py
│   ├── test_scorer.py
│   └── test_scraper.py
└── docs/                    # Specifications and planning documents
    ├── initial-spec
    └── plans/
```

## Pipeline Architecture

The pipeline runs 5 sequential steps, each building on the previous:

```
┌─────────────────────────────────────────────────────────┐
│  Step 1: Hashtag Generation                             │
│  LLM generates ~20 city-specific hashtags               │
│  (category-aware when --category is specified)           │
├─────────────────────────────────────────────────────────┤
│  Step 2: Apify Scraping                                 │
│  Fetches TikTok + Instagram posts per hashtag            │
│  Filters by engagement minimums to save credits          │
├─────────────────────────────────────────────────────────┤
│  Step 3: Place Extraction                               │
│  LLM extracts named places from captions (batches of 20)│
├─────────────────────────────────────────────────────────┤
│  Step 4: Dedup + Scoring                                │
│  Fuzzy dedup (rapidfuzz) with LLM confirmation           │
│  Virality scoring: saves(5x) + shares(4x) +             │
│                    comments(2x) + likes(1x)              │
├─────────────────────────────────────────────────────────┤
│  Step 5: Tourist Trap Filter                            │
│  LLM classifies places as tourist traps (batches of 50) │
└─────────────────────────────────────────────────────────┘
```

## Categories

The pipeline supports 8 place categories. Use `--category` to focus discovery on one:

| Category key | Label | Place types |
|---|---|---|
| `food_and_drink` | Food & Drink | restaurant, cafe, bakery |
| `places_to_stay` | Places to Stay | hotel, hostel |
| `sights_and_attractions` | Sights & Attractions | viewpoint, neighborhood, street, monument, temple |
| `nightlife` | Nightlife | bar, club, lounge, brewery |
| `shopping` | Shopping | shop, market, boutique |
| `outdoors_and_nature` | Outdoors & Nature | park, beach, garden, trail |
| `arts_and_culture` | Arts & Culture | museum, gallery, theater |
| `activities_and_experiences` | Activities & Experiences | activity, tour, class, spa, workshop |

## API Endpoints

The dashboard also exposes a JSON API:

### `GET /api/places`

Returns paginated place data.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `city_id` | int | — | City ID (required) |
| `page` | int | 1 | Page number |
| `per_page` | int | 50 | Results per page (max 500) |
| `category` | string | — | Filter by category key |

**Response:**

```json
{
  "places": [
    {
      "name": "Karaköy Güllüoğlu",
      "type": "bakery",
      "category": "food_and_drink",
      "virality_score": 0.8523,
      "mention_count": 12,
      "is_tourist_trap": false
    }
  ],
  "total": 142,
  "page": 1,
  "per_page": 50
}
```

## Database

The application uses SQLite (`places.db` by default). The database is automatically created and initialized on first run.

**Tables:**
- `cities` — City records
- `hashtags` — Generated hashtags per platform
- `raw_posts` — Scraped posts with engagement metrics
- `post_hashtags` — Links posts to hashtags
- `places` — Extracted/deduplicated places with virality scores
- `place_posts` — Links places to their source posts

The database location can be changed with the `DB_PATH` environment variable.

## Costs

| Service | Estimated cost |
|---|---|
| Apify | ~$2 per 1,000 results. A typical city run (15 hashtags x 100 posts x 2 platforms) costs ~$6 |
| OpenRouter | Depends on model. ~50 LLM calls per city for extraction, filtering, and dedup |

Use `--max-posts` and `--skip-scrape` to manage Apify costs during development.

## Troubleshooting

**"OpenRouter credits exhausted"**
Add credits at https://openrouter.ai and re-run. Progress is saved — the pipeline resumes where it left off.

**Pipeline interrupted mid-run**
Just re-run the same command. Hashtags stuck in "running" state are automatically reset to "pending" on startup.

**Want to start fresh for a city?**
Use `--reset` to clear all data for a city before running.

**Tests failing?**
Make sure your virtual environment is activated and dependencies are installed:
```bash
source .venv/bin/activate
pip install -r requirements.txt
python -m pytest tests/ -v
```
