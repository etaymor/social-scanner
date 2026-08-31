# Social Scanner — Atlasi City Guide Discovery

Discover trending places in cities by scraping TikTok hashtags, extracting place names, and ranking them by engagement. Built for Atlasi city guides, starting with Tokyo food spots.

## What this is

A place discovery pipeline that:

1. Takes a city name as input
2. Generates city-related hashtags (e.g., "tokyofood", "tokyoeats")
3. Scrapes TikTok posts via Apify
4. Extracts place names from captions and location tags using an LLM
5. Deduplicates and scores places by engagement (likes, shares, comments, saves)
6. Filters out tourist traps
7. Exports results as JSON/CSV for Atlasi

**Primary use case:** Tokyo food discovery

## What this is not

**Important limitations to understand:**

- **City is a hashtag seed, not a geo filter** — The pipeline generates hashtags like "#tokyofood" and "#tokyoeats" to find posts. It does not geo-filter posts by city boundaries. Places mentioned in posts scraped under these hashtags may be outside the city.

- **Saves = TikTok video collectCount, not user bookmark lists** — `raw_posts.saves` stores how many times a *video* was saved (`collectCount`), not how many people bookmarked a *place*. This is a useful discovery signal but not a "most saved places" ranking.

- **Do not publish virality_score as "most saved"** — The current `places.virality_score` is an engagement-rate formula (saves × 5 + shares × 4 + comments × 2 + likes × 1, normalized by views). This weights video saves but is not a "places that keep repeating in saved videos" metric.

- **Instagram is leftover code** — Instagram hashtag generation and scraping exist in the codebase but are not used by default. TikTok-only is the current path.

- **The repeating-saves ranker is NOT shipped** — `pipeline/city_saved_places.py` is a design stub with `NotImplementedError` on all ranking functions. The planned "places that keep appearing in saved-heavy videos" ranker (by distinct authors + distinct posts) does not exist yet. Current CSV/dashboard results are sorted by `virality_score` as a placeholder.

- **No time-window filtering** — `discover.py` has no `--days`, `--last-30`, or `--last-60` flag. The Apify scraper input is hashtags + `resultsPerPage` only. Filtering to "posts from the last 30 days" is a product direction, not a shipped feature. Do not claim results are "most saved in the last 30 days."

## Prerequisites

- **Python 3.11+** (developed on 3.14)
- **Apify account** — for TikTok scraping ([sign up](https://apify.com/))
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
   | `apify-client` | Apify API client for TikTok scraping |
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
   OPENROUTER_MODEL=google/gemini-3.1-flash-lite
   OCR_MODEL=google/gemini-3.1-flash-lite
   GEMINI_MODEL=google/gemini-3.1-flash-image
   ```

   | Variable             | Required | Default                          | Description                                      |
   | -------------------- | -------- | -------------------------------- | ------------------------------------------------ |
   | `APIFY_API_TOKEN`    | Yes      | —                                | Your Apify API token                             |
   | `OPENROUTER_API_KEY` | Yes      | —                                | Your OpenRouter API key                          |
   | `OPENROUTER_MODEL`   | No       | `google/gemini-3.1-flash-lite`   | Caption extract / JSON via OpenRouter            |
   | `OCR_MODEL`          | No       | `google/gemini-3.1-flash-lite`   | Vision OCR primary (not `*-flash-image`)         |
   | `GEMINI_MODEL`       | No       | `google/gemini-3.1-flash-image`  | Slideshow image gen only (Nano Banana 2)         |
   | `DB_PATH`            | No       | `places.db`                      | Path to the SQLite database file                 |
   | `FLASK_DEBUG`        | No       | `false`                          | Set to `true` for Flask debug mode               |

## Running the Pipeline

The CLI entry point is `discover.py`. It runs a 5-step discovery pipeline for a given city.

### Basic usage

```bash
python discover.py --city "Tokyo"
```

### All CLI options

```bash
python discover.py --city "Tokyo" [OPTIONS]
```

| Flag                  | Description                                                  |
| --------------------- | ------------------------------------------------------------ |
| `--city CITY`         | **(required)** City name to research                         |
| `--category CATEGORY` | Focus on a specific category (see [Categories](#categories)) |
| `--max-posts N`       | Max posts per hashtag per platform (default: 100)            |
| `--skip-scrape`       | Skip Apify scraping, re-run extraction on existing data      |
| `--retry-failed`      | Reset failed hashtags to pending so they get re-scraped      |
| `--reset`             | Clear all data for this city before running                  |
| `--export-csv`        | Export results to a CSV file                                 |
| `--verbose`           | Debug-level logging                                          |
| `--quiet`             | Minimal output (warnings and errors only)                    |

### Examples

```bash
# Discover food spots in Tokyo, limit to 50 posts per hashtag
python discover.py --city "Tokyo" --category food_and_drink --max-posts 50

# Re-run extraction without re-scraping (saves Apify credits)
python discover.py --city "Tokyo" --skip-scrape

# Retry failed hashtags from a previous run
python discover.py --city "Tokyo" --retry-failed

# Start fresh for a city
python discover.py --city "Tokyo" --reset

# Export results to CSV
python discover.py --city "Tokyo" --export-csv

# Verbose output for debugging
python discover.py --city "Tokyo" --verbose
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

**Note:** The dashboard currently sorts by `virality_score` (engagement-rate formula). This is a placeholder until the repeating-saves ranker is implemented.

## Running Tests

```bash
# Run the full test suite
python -m pytest tests/ -v

# Run a specific test file
python -m pytest tests/test_category.py -v

# Run with coverage (requires pytest-cov)
python -m pytest tests/ -v --cov=pipeline
```

Test files for discovery pipeline:
| File | What it tests |
|---|---|
| `test_pipeline.py` | Full pipeline integration (mocked APIs) |
| `test_category.py` | Category validation, CLI parsing, dashboard filtering |
| `test_llm.py` | OpenRouter LLM wrapper and retry logic |
| `test_scorer.py` | Virality scoring and deduplication |
| `test_scraper.py` | Apify scraper response mapping |
| `test_city_saved_places.py` | Contract tests for the unimplemented repeating-saves ranker |

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
│   ├── scraper.py           # Step 2: Apify TikTok scraping
│   ├── ocr.py               # Step 2.5: Visual OCR on cover images
│   ├── extractor.py         # Step 3: LLM place extraction from captions
│   ├── scorer.py            # Step 4: Fuzzy dedup + virality scoring
│   ├── filter.py            # Step 5: LLM tourist trap classification
│   ├── city_saved_places.py # STUB: Repeating-saves ranker (NotImplementedError)
│   └── llm.py               # OpenRouter LLM wrapper with retries
├── templates/
│   └── dashboard.html       # Dashboard UI template
├── tests/                   # Test suite
│   ├── conftest.py          # Shared fixtures (in-memory DB, test city)
│   ├── test_pipeline.py
│   ├── test_category.py
│   ├── test_llm.py
│   ├── test_scorer.py
│   ├── test_scraper.py
│   └── test_city_saved_places.py
└── docs/                    # Specifications and planning documents
    ├── city-saved-places.md # Repeating-saves ranker design doc
    ├── initial-spec
    └── plans/
```

**Note:** The codebase also contains slideshow generation (`generate_slideshow.py`), analytics (`daily_report.py`), and RevenueCat integration modules. These are separate products that share the same SQLite database. This README focuses on the discovery pipeline only.

## Pipeline Architecture

The pipeline runs 5 sequential steps, each building on the previous:

```
┌─────────────────────────────────────────────────────────┐
│  Step 1: Hashtag Generation                             │
│  LLM generates ~20 city-specific hashtags               │
│  (category-aware when --category is specified)          │
├─────────────────────────────────────────────────────────┤
│  Step 2: Apify Scraping                                 │
│  Fetches TikTok posts per hashtag via unofficial actor  │
│  (Note: Instagram scraping exists but is not used)      │
├─────────────────────────────────────────────────────────┤
│  Step 2.5: Visual OCR                                   │
│  Extracts on-screen text from TikTok cover images       │
├─────────────────────────────────────────────────────────┤
│  Step 3: Place Extraction                               │
│  LLM extracts named places from captions (batches of 20)│
├─────────────────────────────────────────────────────────┤
│  Step 4: Dedup + Scoring                                │
│  Fuzzy dedup (rapidfuzz) with LLM confirmation          │
│  Virality scoring: saves(5x) + shares(4x) +             │
│                    comments(2x) + likes(1x)              │
│  (Normalized by views)                                   │
├─────────────────────────────────────────────────────────┤
│  Step 5: Tourist Trap Filter                            │
│  LLM classifies places as tourist traps (batches of 50) │
└─────────────────────────────────────────────────────────┘
```

## Categories

The pipeline supports 8 place categories. Use `--category` to focus discovery on one:

| Category key                 | Label                    | Place types                                       |
| ---------------------------- | ------------------------ | ------------------------------------------------- |
| `food_and_drink`             | Food & Drink             | restaurant, cafe, bakery                          |
| `places_to_stay`             | Places to Stay           | hotel, hostel                                     |
| `sights_and_attractions`     | Sights & Attractions     | viewpoint, neighborhood, street, monument, temple |
| `nightlife`                  | Nightlife                | bar, club, lounge, brewery                        |
| `shopping`                   | Shopping                 | shop, market, boutique                            |
| `outdoors_and_nature`        | Outdoors & Nature        | park, beach, garden, trail                        |
| `arts_and_culture`           | Arts & Culture           | museum, gallery, theater                          |
| `activities_and_experiences` | Activities & Experiences | activity, tour, class, spa, workshop              |

## API Endpoints

The dashboard also exposes a JSON API:

### `GET /api/places`

Returns paginated place data.

| Parameter  | Type   | Default | Description                |
| ---------- | ------ | ------- | -------------------------- |
| `city_id`  | int    | —       | City ID (required)         |
| `page`     | int    | 1       | Page number                |
| `per_page` | int    | 50      | Results per page (max 500) |
| `category` | string | —       | Filter by category key     |

**Response:**

```json
{
  "places": [
    {
      "name": "Sushi Saito",
      "type": "restaurant",
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

**Note:** Results are sorted by `virality_score` (placeholder until repeating-saves ranker is implemented).

## Database

The application uses SQLite (`places.db` by default). The database is automatically created and initialized on first run.

**Tables:**

- `cities` — City records
- `hashtags` — Generated hashtags per platform
- `raw_posts` — Scraped posts with engagement metrics (likes, shares, comments, saves, views)
- `post_hashtags` — Links posts to hashtags
- `places` — Extracted/deduplicated places with virality scores
- `place_posts` — Links places to their source posts

The database location can be changed with the `DB_PATH` environment variable.

## Costs

| Service    | Estimated cost                                                                              |
| ---------- | ------------------------------------------------------------------------------------------- |
| Apify      | ~$2 per 1,000 results. A typical city run (15 hashtags × 100 posts) costs ~$3              |
| OpenRouter | Depends on model. ~50 LLM calls per city for extraction, filtering, and dedup               |

Use `--max-posts` and `--skip-scrape` to manage Apify costs during development.

## Troubleshooting

**"OpenRouter credits exhausted"**  
Add credits at https://openrouter.ai and re-run. Progress is saved — the pipeline resumes where it left off.

**Pipeline interrupted mid-run**  
Just re-run the same command. Hashtags stuck in "running" state are automatically reset to "pending" on startup.

**Want to start fresh for a city?**  
Use `--reset` to clear all data for a city before running.

**Scraping failed for some hashtags?**  
Use `--retry-failed` to reset failed hashtags to pending and re-run them.

**Tests failing?**  
Make sure your virtual environment is activated and dependencies are installed:

```bash
source .venv/bin/activate
pip install -r requirements.txt
python -m pytest tests/ -v
```

## Future Work

The planned **repeating-saves ranker** (`pipeline/city_saved_places.py`) will rank places by:

1. Distinct authors who posted videos mentioning the place
2. Distinct posts (not just one viral video)
3. Total video `collectCount` (sum of saves across all posts) as a secondary signal

This ranker will be labeled as **"places that keep appearing in saved-heavy Tokyo TikToks"** (not "most saved places in Tokyo") to reflect that it's based on video saves, not place bookmarks.

See `docs/city-saved-places.md` for design details.
