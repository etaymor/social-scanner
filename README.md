# Atlasi Place Discovery Pipeline

Discover trending, non-obvious places in any city by scraping TikTok and Instagram via Apify, extracting place names with an LLM, and scoring them by virality.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and add your API keys:

```bash
cp .env.example .env
```

Required keys:
- `APIFY_API_TOKEN` — [Get one here](https://apify.com/)
- `OPENROUTER_API_KEY` — [Get one here](https://openrouter.ai/)

## Usage

```bash
# Discover places in a city
python discover.py --city "Istanbul"

# Limit posts per hashtag (saves Apify credits)
python discover.py --city "Tokyo" --max-posts 50

# Skip scraping, re-run extraction on existing data
python discover.py --city "Istanbul" --skip-scrape

# Reset all data for a city and start fresh
python discover.py --city "Istanbul" --reset

# Export results to CSV
python discover.py --city "Istanbul" --export-csv

# Verbose logging
python discover.py --city "Istanbul" --verbose
```

## Pipeline Steps

1. **Hashtag Generation** — LLM generates ~20 city-specific hashtags
2. **Apify Scraping** — Fetches TikTok + Instagram posts per hashtag, filtering for high engagement
3. **Place Extraction** — LLM extracts named places from captions in batches of 20
4. **Dedup + Scoring** — Fuzzy deduplication with LLM confirmation, then virality scoring
5. **Tourist Trap Filter** — LLM classifies places, batched at 50

Re-runs are safe — the pipeline resumes from where it left off without duplicating work.

## Dashboard

Browse results in a local web dashboard:

```bash
python dashboard.py
```

Open http://localhost:5555 — switch between cities, filter by type, search places, sort by score.

## Tests

```bash
python -m pytest tests/ -v
```

## Costs

- Apify: ~$2/1000 results. A city with 15 hashtags x 100 posts x 2 platforms ≈ $6
- OpenRouter: depends on model. ~50 LLM calls per city for extraction + filter + dedup
