---
title: "feat: Atlasi Place Discovery Pipeline"
type: feat
status: completed
date: 2026-03-23
---

# feat: Atlasi Place Discovery Pipeline

## Overview

A Python CLI tool that discovers trending, non-obvious places in any city by scraping TikTok and Instagram via Apify, extracting place names using an LLM (OpenRouter), deduplicating/scoring them, and storing everything in a local SQLite database. Built to feed TikTok slideshow content for the Atlasi travel app.

The pipeline runs 5 sequential steps: hashtag generation → social media scraping → LLM place extraction → deduplication + scoring → tourist trap filtering.

## Problem Statement / Motivation

Atlasi needs a repeatable, data-driven way to discover genuinely interesting places in any city — the kind of spots locals love that don't appear in guidebooks. Social media (TikTok + Instagram) is where these places surface organically. Manual research doesn't scale; this tool automates the discovery pipeline.

## Proposed Solution

A single-file-per-step Python CLI tool with a linear pipeline architecture. Each step is a separate module, all coordinated by `discover.py`. SQLite stores all intermediate and final data, enabling checkpoint-based resume and idempotent reruns.

### Architecture

```
CLI Input (city name)
    │
    ▼
Hashtag Generator (LLM via OpenRouter)
    │
    ▼
Apify Scrapers (TikTok + Instagram)
    │
    ▼
Raw Post Storage (SQLite)
    │
    ▼
LLM Place Extraction (batches of 20)
    │
    ▼
Deduplication + Scoring
    │
    ▼
Tourist Trap Filter (LLM)
    │
    ▼
Final Places Table (SQLite) + stdout summary + optional CSV
```

### File Structure

```
atlasi-place-discovery/
├── discover.py          # CLI entrypoint + orchestrator
├── config.py            # .env loading, defaults, constants
├── db.py                # SQLite setup, schema, queries
├── hashtags.py          # Step 1: hashtag generation
├── scraper.py           # Step 2: Apify scraping (TikTok + IG)
├── extractor.py         # Step 3: LLM place extraction
├── scorer.py            # Step 4: dedup + scoring
├── filter.py            # Step 5: tourist trap filter
├── llm.py               # OpenRouter API wrapper
├── requirements.txt
├── .env.example
├── .gitignore
├── places.db            # SQLite database (gitignored)
└── README.md
```

## Technical Considerations

### Dependency Changes from Spec

- **Use `rapidfuzz` instead of `python-Levenshtein`**: MIT license (vs GPL), 40% faster, more algorithms (token_sort_ratio, partial_ratio), and actively maintained by the same team. `python-Levenshtein` is now just an alias for the `Levenshtein` package anyway.

### Database Schema Refinements

The spec's schema is solid but needs three adjustments:

1. **Add `scrape_status` to `hashtags` table**: The spec claims checkpoint-based resume but Step 2 (scraping) has no checkpoint mechanism. Without tracking which hashtag+platform combinations have been scraped, reruns re-invoke all Apify actor runs, burning duplicate credits. Add `scrape_status TEXT DEFAULT 'pending' CHECK(scrape_status IN ('pending', 'running', 'completed', 'failed'))` to the `hashtags` table.

2. **Replace `source_post_ids` JSON TEXT with a junction table**: The `places.source_post_ids` column stores a JSON array of raw_post IDs as TEXT. This breaks referential integrity, makes dedup merging error-prone (JSON parse → merge → serialize), and prevents efficient querying. Use a `place_posts(place_id, post_id)` junction table instead.

3. **Add `post_hashtags` junction table**: The `raw_posts.hashtag` column stores a single hashtag, but a post can appear under multiple hashtag searches. The `UNIQUE(platform, post_id)` constraint means only the first-seen hashtag is recorded, losing multi-hashtag attribution data.

4. **Map `created_at` in raw_posts**: The spec's field mappings don't map any platform field to `raw_posts.created_at`, so it defaults to NULL. Map TikTok's `createTime` and Instagram's `timestamp` fields.

### Revised Schema

```sql
-- Additions/changes to spec schema:

-- hashtags table: add scrape tracking
ALTER TABLE hashtags ADD COLUMN scrape_status TEXT DEFAULT 'pending'
  CHECK(scrape_status IN ('pending', 'running', 'completed', 'failed'));

-- Replace places.source_post_ids with junction table
CREATE TABLE place_posts (
    place_id INTEGER NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    post_id INTEGER NOT NULL REFERENCES raw_posts(id) ON DELETE CASCADE,
    PRIMARY KEY (place_id, post_id)
);

-- Optional: multi-hashtag attribution
CREATE TABLE post_hashtags (
    post_id INTEGER NOT NULL REFERENCES raw_posts(id) ON DELETE CASCADE,
    hashtag_id INTEGER NOT NULL REFERENCES hashtags(id) ON DELETE CASCADE,
    PRIMARY KEY (post_id, hashtag_id)
);
```

### SQLite Configuration

Enable on every connection open in `db.py`:

```python
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA foreign_keys=ON')
conn.execute('PRAGMA busy_timeout=5000')
conn.execute('PRAGMA synchronous=NORMAL')
conn.row_factory = sqlite3.Row
```

### CLI Flag Refinements

| Issue | Resolution |
|-------|-----------|
| `--skip-scrape` and `--only-extract` are functionally identical | Collapse into `--skip-scrape`. Consider adding `--only-score` and `--only-filter` for step-specific reruns |
| `--export-csv` mentioned in output section but missing from CLI args table | Add to CLI args: `--export-csv` flag that dumps non-tourist-trap places sorted by score |
| No verbosity control | Add `--verbose` / `--quiet` flags |
| No city validation | Add a quick LLM validation check before burning Apify credits on nonsense input |

### Apify Integration Notes

- **`actor.call()` returns `None` on failure** — always check return value before accessing `run['defaultDatasetId']`
- **Use `build='latest'`** for TikTok scraper — it's updated nearly daily to counter TikTok's anti-bot measures
- **Dataset pagination**: `list_items()` does not auto-paginate; implement manual pagination with offset/limit
- **Cost control**: Pass `maxItems` parameter in actor input (maps to `--max-posts` CLI arg); budget ~$2/1000 results for both actors
- **TikTok runtime variability**: TikTok scraper runtime can vary 3-5x due to anti-bot measures; set generous `timeout_secs`

### OpenRouter Integration Notes

- **Use `response_format: {"type": "json_object"}` AND include "Return ONLY a JSON array" in prompt** — both are needed for reliable JSON output
- **Handle 402 (credits exhausted) as fatal** — no point retrying
- **Rate limits**: Free tier = 20 req/min, 50/day. For a city with 1000 posts, Step 3 alone makes ~50 LLM calls. Ensure the user has credits or the pipeline will hit limits
- **OpenRouter supports model fallback**: `"models": ["anthropic/claude-sonnet-4", "openai/gpt-4o"]` — consider offering this as a resilience option

### Deduplication Improvements

The spec's Levenshtein distance ≤ 3 threshold is too aggressive for short place names:
- "Kat" vs "Bar" = distance 3 → false merge
- "The Loft" vs "The Lost" = distance 1 → false merge

**Use a relative threshold**: `distance / max(len(a), len(b)) < 0.3` AND require LLM confirmation for all proposed merges, not just "ambiguous cases."

Use `rapidfuzz.fuzz.token_sort_ratio` with a score cutoff of ~85 for the initial candidate matching, then LLM disambiguation for all candidates.

### Tourist Trap Filter Batching

Step 5 sends "all places for the city" in one LLM call. For cities with 200+ places, this exceeds reasonable context window limits. Batch at 50 places per call, consistent with Step 3's batching approach.

### Virality Score Normalization

The raw virality score is not comparable across cities. Instagram posts will also systematically score lower since saves (5x weight) and shares (4x weight) are unavailable from the Instagram scraper. Consider:
- Percentile rank normalization within each city (0.0 to 1.0)
- Or acknowledge the bias in output and document it

## System-Wide Impact

- **Interaction graph**: CLI → orchestrator → each step module → `llm.py` or `scraper.py` → external APIs. `db.py` is touched by every step.
- **Error propagation**: API failures (Apify/OpenRouter) → retry with backoff → after 3 retries, log and continue (scraping) or raise (extraction/filtering). Partial scraping failures are acceptable; partial extraction failures should not silently skip posts.
- **State lifecycle risks**: Partial failure in Step 4 (dedup) could leave merged places in an inconsistent state if the merge operation isn't atomic. Wrap each merge group in a transaction.
- **API surface parity**: N/A — CLI tool only, no exposed APIs.
- **Integration test scenarios**: (1) Full pipeline run on a small city; (2) Resume after crash mid-scrape; (3) Re-run same city without `--reset`; (4) `--reset` then re-run; (5) City with zero results from scraping.

## Acceptance Criteria

### Functional Requirements

- [ ] `python discover.py --city "Istanbul"` runs the full 5-step pipeline end-to-end
- [ ] Hashtag generation produces ~20 unique hashtags (15 LLM + 5 hardcoded, deduped)
- [ ] Apify scraping fetches posts from both TikTok and Instagram for each hashtag
- [ ] LLM extraction identifies named places from post captions in batches of 20
- [ ] Fuzzy deduplication merges similar place names using relative threshold + LLM confirmation
- [ ] Virality scores are calculated using the weighted engagement formula
- [ ] Tourist trap filter classifies places and updates the database
- [ ] Summary output prints to stdout with top 20 places by score
- [ ] `--export-csv` dumps results to CSV file
- [ ] `--skip-scrape` skips Apify scraping and runs extraction on existing data
- [ ] `--reset` clears all data for the city before running
- [ ] `--max-posts N` caps results per hashtag per platform

### Resilience Requirements

- [ ] Pipeline resumes from last checkpoint on re-run (scrape_status tracking for Step 2, processed flag for Step 3)
- [ ] All API calls retry 3x with exponential backoff
- [ ] Failed hashtag scrapes log error and continue with remaining hashtags
- [ ] Duplicate posts are handled via `INSERT OR IGNORE`
- [ ] Running the same city twice is idempotent — adds new data without duplicating

### Data Integrity Requirements

- [ ] `PRAGMA foreign_keys=ON` enforced on every connection
- [ ] `--reset` deletes child tables first, then city row, in a single transaction
- [ ] `place_posts` junction table maintains referential integrity
- [ ] Place type values are validated/normalized before INSERT, falling back to "other"

### Testing Requirements

- [ ] Unit tests for virality score calculation
- [ ] Unit tests for fuzzy dedup logic with edge cases (short names, containment)
- [ ] Integration test for full pipeline with mocked API responses

## Success Metrics

- Pipeline completes end-to-end for any real city without crashing
- Discovers 50+ unique places for a major city (e.g., Istanbul, Tokyo)
- Tourist trap filter correctly identifies obvious tourist traps (>80% precision on manual review)
- Re-runs don't waste Apify credits on already-scraped hashtags
- Total runtime for a city: < 30 minutes for 15 hashtags × 100 posts × 2 platforms

## Dependencies & Risks

### Dependencies

- **Apify API** — requires active account with credits. Free tier: $5/month
- **OpenRouter API** — requires API key with credits. Free tier: 50 requests/day (insufficient for full pipeline)
- **`clockworks/free-tiktok-scraper`** — community actor, no SLA. Updated daily to counter TikTok anti-bot. Could break without notice
- **`apify/instagram-hashtag-scraper`** — first-party Apify actor, more stable. Don't confuse with deprecated `jansquared/instagram-scraper`

### Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| TikTok scraper breaks due to platform changes | Pipeline produces no TikTok data | Use `build='latest'`, graceful degradation (continue with Instagram only) |
| OpenRouter rate limits on free/low tier | Pipeline stalls at extraction | Document minimum tier requirements, implement proper backoff with retry-after header |
| LLM returns malformed JSON | Extraction/filter steps crash | Wrap JSON parsing in try/except, retry with explicit "return valid JSON" prompt |
| Apify credits exhausted mid-run | Partial scraping results | Check credit balance before starting, implement `max_total_charge_usd` cap |
| Python 3.14 compatibility | Dependencies may not have wheels | Verify all 4 deps have 3.14 wheels; fall back to 3.12/3.13 via pyenv if needed |

## Sources & References

### Internal References

- Specification: `docs/initial-spec` — complete pipeline spec with schema, prompts, field mappings, and file structure

### External References

- [Apify Python Client docs](https://docs.apify.com/api/client/python/docs/overview/getting-started)
- [TikTok Data Extractor actor](https://apify.com/clockworks/free-tiktok-scraper)
- [Instagram Hashtag Scraper actor](https://apify.com/apify/instagram-hashtag-scraper)
- [OpenRouter API docs — Structured Outputs](https://openrouter.ai/docs/guides/features/structured-outputs)
- [OpenRouter API docs — Rate Limits](https://openrouter.ai/docs/api/reference/limits)
- [OpenRouter API docs — Error Handling](https://openrouter.ai/docs/api/reference/errors-and-debugging)
- [RapidFuzz documentation](https://rapidfuzz.com/)
- [SQLite WAL mode](https://www.sqlite.org/wal.html)
