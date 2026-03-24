---
title: "feat: Add Category-Based Search Filter to Discovery Pipeline"
type: feat
status: completed
date: 2026-03-24
---

# feat: Add Category-Based Search Filter to Discovery Pipeline

## Overview

Add an optional `--category` CLI argument that focuses the discovery pipeline on a specific place category (e.g., Food & Drink, Nightlife, Outdoors & Nature). When a category is selected, hashtag generation produces category-specific hashtags mixed with a smaller set of generic hidden-gem tags, resulting in more targeted scraping. The LLM assigns a category to each extracted place, and the dashboard gains a category filter for browsing results.

The current generic search (no `--category`) remains the default and is fully backward compatible.

## Problem Statement / Motivation

The current pipeline generates one-size-fits-all hashtags focused broadly on "hidden gems." This means:

- **Food dominates results.** Generic hidden-gem hashtags on TikTok/Instagram surface food content ~70% of the time, starving categories like nightlife, sights, and shopping.
- **No way to focus discovery.** A user interested specifically in nightlife must sift through hundreds of restaurant results.
- **Wasted scraping budget.** Apify credits are spent scraping posts that don't match the user's interest.

Category-based filtering lets users say "I want to discover nightlife in Istanbul" and get hashtags, scrapes, and results tuned for that intent.

## Proposed Solution

### Category Taxonomy

Define 8 high-level categories in `config.py`, each with a label, description, and mapping to existing place types:

| Key | Label | Example Place Types |
|-----|-------|-------------------|
| `food_and_drink` | Food & Drink | restaurant, cafe, bar, bakery, market |
| `places_to_stay` | Places to Stay | hotel, hostel |
| `sights_and_attractions` | Sights & Attractions | viewpoint, neighborhood, street, monument, temple |
| `nightlife` | Nightlife | bar, club, lounge, brewery |
| `shopping` | Shopping | shop, market, boutique |
| `outdoors_and_nature` | Outdoors & Nature | park, beach, garden, trail |
| `arts_and_culture` | Arts & Culture | museum, gallery, theater |
| `activities_and_experiences` | Activities & Experiences | activity, tour, class, spa, workshop |

### CLI Interface

```
python discover.py --city "Istanbul" --category food_and_drink
python discover.py --city "Istanbul"  # default: no category, generic search
```

- Single category per run (run multiple times for different categories)
- CLI-friendly snake_case keys with validation and helpful error messages
- `--category` is optional; omitting it preserves current behavior exactly

### Pipeline Changes

**Step 1 — Hashtag Generation (modified)**
- When `--category` is provided: generate ~12 category-specific LLM hashtags + 3 generic LLM hashtags + 5 universal hardcoded tags = ~20 total (same budget as today)
- Category-specific prompt asks for hashtags targeting that category's content
- Category-specific seed hashtags added (e.g., `{city}nightlife`, `{city}bars` for nightlife)
- Store category association on each hashtag in the DB for run isolation

**Step 2 — Scraping (unchanged)**
- Scraper processes pending hashtags as before, unaware of categories

**Step 3 — Extraction (modified)**
- LLM prompt updated to also assign a `category` from the valid list for each extracted place
- `_validate_category()` validates LLM output, falls back to `TYPE_TO_CATEGORY` mapping
- The LLM assigns the category based on the caption content — NOT based on the `--category` flag. This means a bar found via food hashtags is still correctly categorized as nightlife.

**Step 4 — Dedup & Scoring (minor update)**
- Dedup remains name-based (city_id + name). Category is a property of the place, not identity.
- When merging duplicates, keep the category from the canonical (highest mention_count) place.

**Step 5 — Tourist Trap Filter (unchanged)**
- Orthogonal to categories, no changes needed.

**Output (updated)**
- `print_summary()` shows the category searched and each place's assigned category
- `export_csv` includes the category column
- `/api/places` JSON includes category (automatic via `dict(p)`)

### Dashboard Changes

- Add category dropdown filter (server-side, alongside existing type filter)
- Category filter as a query parameter to `get_places_page()` with `WHERE category = ?` clause
- Server-side filtering is required because client-side filtering only works within the current 50-place page

## Technical Considerations

### Schema Changes

**`places` table** — add `category TEXT DEFAULT NULL`:
```sql
ALTER TABLE places ADD COLUMN category TEXT DEFAULT NULL;
```

**`hashtags` table** — add `category TEXT DEFAULT NULL`:
```sql
ALTER TABLE hashtags ADD COLUMN category TEXT DEFAULT NULL;
```

Both migrations handled inside `init_db()` using safe `ALTER TABLE` wrapped in try/except for the "duplicate column name" error. This is the simplest approach for SQLite without a migration framework.

### Expanded Place Types

Add to `VALID_PLACE_TYPES` in `config.py`:
```python
VALID_PLACE_TYPES = frozenset({
    # Existing
    "restaurant", "cafe", "bar", "club", "market", "neighborhood",
    "viewpoint", "park", "museum", "gallery", "shop", "activity",
    "street", "other",
    # New
    "hotel", "hostel", "tour", "class", "beach", "temple", "spa",
    "brewery", "lounge", "bakery", "garden", "theater", "monument",
    "boutique", "trail", "workshop",
})
```

### Type-to-Category Fallback Mapping

```python
TYPE_TO_CATEGORY = {
    "restaurant": "food_and_drink",
    "cafe": "food_and_drink",
    "bakery": "food_and_drink",
    "bar": "nightlife",
    "club": "nightlife",
    "lounge": "nightlife",
    "brewery": "nightlife",
    "hotel": "places_to_stay",
    "hostel": "places_to_stay",
    "market": "shopping",
    "shop": "shopping",
    "boutique": "shopping",
    "neighborhood": "sights_and_attractions",
    "viewpoint": "sights_and_attractions",
    "street": "sights_and_attractions",
    "monument": "sights_and_attractions",
    "temple": "sights_and_attractions",
    "park": "outdoors_and_nature",
    "beach": "outdoors_and_nature",
    "garden": "outdoors_and_nature",
    "trail": "outdoors_and_nature",
    "museum": "arts_and_culture",
    "gallery": "arts_and_culture",
    "theater": "arts_and_culture",
    "activity": "activities_and_experiences",
    "tour": "activities_and_experiences",
    "class": "activities_and_experiences",
    "spa": "activities_and_experiences",
    "workshop": "activities_and_experiences",
}
```

### Hashtag Run Isolation

The `hashtags` table gets a `category` column. `get_pending_hashtags()` filters by category when one is active, preventing a "Nightlife" run from scraping leftover "Food & Drink" hashtags from a crashed prior run.

### Performance & Cost

- Per-category run costs the same as a generic run (~20 hashtags × 2 platforms)
- Running all 8 categories costs 8× a single run in Apify credits
- No additional LLM cost — extraction batches remain the same size

### Architecture Impact

- Categories are layered on top of the existing type system, not replacing it
- No changes to the dedup algorithm, scoring formula, or tourist trap filter
- Dashboard server-side filtering is a pattern that should eventually apply to type/search filters too, but that's out of scope

## System-Wide Impact

### Interaction Graph

- CLI `--category` → `generate_hashtags()` (category-aware prompt) → `scrape_posts()` (unchanged, scrapes pending hashtags) → `extract_places()` (category-aware prompt + validation) → `deduplicate_and_score()` (category merge strategy) → `filter_tourist_traps()` (unchanged) → `print_summary()` + `export_csv` (category-aware output)
- Dashboard: `get_places_page()` gains `category` parameter → template renders category dropdown → JavaScript sends category as query param

### Error Propagation

- Invalid `--category` input: rejected at CLI parse time with a helpful error listing valid categories
- LLM returns invalid category: `_validate_category()` falls back to `TYPE_TO_CATEGORY` mapping, then to `"sights_and_attractions"` as final default
- Schema migration failure: `ALTER TABLE` wrapped in try/except; if column already exists, silently continues

### State Lifecycle Risks

- **Partial category runs**: If a category run crashes mid-scrape, the hashtag `scrape_status` checkpoint system handles resume correctly because `get_pending_hashtags` will filter by the same category on retry
- **Place rediscovery**: A place found in a generic run (category=NULL) and later in a category run will have its category updated by the LLM. The reverse (category run first, generic second) could set category to NULL — the upsert logic should only update category when the new value is not NULL.

### API Surface Parity

- `/api/places` JSON endpoint: category field included automatically
- Dashboard HTML: needs explicit category dropdown addition
- CSV export: needs explicit category column addition

## Acceptance Criteria

### Functional Requirements

- [ ] `--category food_and_drink` generates food-specific hashtags mixed with generic ones
- [ ] `--category` omitted produces identical behavior to current pipeline (backward compatible)
- [ ] Invalid `--category` value shows error with list of valid categories
- [ ] LLM assigns a category to each extracted place during Step 3
- [ ] Places table stores category; hashtags table stores category for run isolation
- [ ] Dashboard has a category dropdown filter that works server-side with pagination
- [ ] `print_summary()` displays category information
- [ ] `export_csv` includes category column
- [ ] Existing databases migrate safely (new columns added without data loss)
- [ ] Dedup merges preserve the canonical place's category

### Non-Functional Requirements

- [ ] Per-category run stays within ~20 hashtags (same Apify budget as generic)
- [ ] Category validation fallback chain: LLM output → TYPE_TO_CATEGORY → default
- [ ] No breaking changes to existing CLI usage or database

### Quality Gates

- [ ] Unit tests for `_validate_category()` and `TYPE_TO_CATEGORY` mapping
- [ ] Integration test for category-aware hashtag generation
- [ ] Integration test for category-aware extraction
- [ ] Test for `--category` CLI argument parsing and validation
- [ ] Test for dashboard category filter with pagination
- [ ] Test schema migration on existing database (with and without category column)

## Implementation Phases

### Phase 1: Config & Schema Foundation

**Files:** `config.py`, `pipeline/db.py`

- Define `CATEGORIES` dict, `VALID_CATEGORIES` frozenset, `TYPE_TO_CATEGORY` mapping in `config.py`
- Expand `VALID_PLACE_TYPES` with new types (hotel, hostel, tour, etc.)
- Add `category TEXT DEFAULT NULL` to `places` and `hashtags` tables via safe `ALTER TABLE` in `init_db()`
- Update `upsert_place()` to accept and store category (only update if new value is not NULL)
- Update `insert_hashtags()` to accept category
- Update `get_pending_hashtags()` to filter by category when provided
- Add `get_places_page()` category filter parameter with `WHERE` clause

### Phase 2: Hashtag Generation

**Files:** `pipeline/hashtags.py`, `config.py`

- Add `CATEGORY_HASHTAG_SEEDS` with per-category universal suffixes and tags
- Modify `generate_hashtags()` to accept optional `category` parameter
- When category is provided: use category-specific LLM prompt (~12 tags) + generic prompt (~3 tags) + universal hardcoded (5 tags)
- When no category: existing behavior unchanged
- Store category on each hashtag row

### Phase 3: Extraction

**Files:** `pipeline/extractor.py`, `config.py`

- Update `SYSTEM_PROMPT` to include category enumeration and ask LLM to assign a category per place
- Add `_validate_category()` function with fallback chain
- Update `_process_batch()` to extract, validate, and pass category to `upsert_place()`

### Phase 4: CLI & Output

**Files:** `discover.py`

- Add `--category` argument to argparse with `choices` from `VALID_CATEGORIES`
- Thread category through to `generate_hashtags()` and display functions
- Update `print_summary()` to show searched category and per-place categories
- Update `export_csv()` to include category column

### Phase 5: Dashboard

**Files:** `dashboard.py`, `templates/dashboard.html`

- Add category to server-side query: `get_places_page()` accepts category parameter
- Pass distinct categories and active filter to template
- Add category dropdown in dashboard HTML
- Wire category selection to page reload with query parameter

### Phase 6: Scorer Update & Tests

**Files:** `pipeline/scorer.py`, `tests/test_pipeline.py`, `tests/test_extractor.py` (new or updated)

- Update `merge_places()` to preserve canonical place's category during dedup
- Add unit tests for `_validate_category()`, `TYPE_TO_CATEGORY`
- Add integration tests for category-aware hashtag generation and extraction
- Add CLI argument parsing tests
- Add dashboard category filter test
- Add schema migration test

## Dependencies & Risks

| Risk | Mitigation |
|------|-----------|
| LLM inconsistently assigns categories | `_validate_category()` fallback chain ensures every place gets a valid category |
| Existing databases break on new columns | Safe `ALTER TABLE` migration in `init_db()`, idempotent |
| Category run scrapes wrong hashtags on crash resume | Category stored on hashtags table, `get_pending_hashtags` filters by it |
| Cost multiplies if users run all 8 categories | Each run is same cost as before; document in help text |
| `--category` + `--skip-scrape` is confusing | Document: skip-scrape skips Steps 1+2, so category only affects extraction labeling |

## Sources & References

### Internal References

- `config.py:46-50` — existing `VALID_PLACE_TYPES` definition
- `pipeline/hashtags.py:16-32` — current hashtag generation prompt
- `pipeline/extractor.py:12-24` — current extraction system prompt
- `pipeline/db.py:24-96` — current schema definitions
- `pipeline/db.py:204-233` — `upsert_place()` logic
- `discover.py:78-98` — CLI argument parser
- `templates/dashboard.html` — existing type filter dropdown

### External References

- [Foursquare Categories](https://docs.foursquare.com/data-products/docs/categories) — industry-standard venue category taxonomy
- [Google Maps Place Types](https://developers.google.com/maps/documentation/places/web-service/place-types) — reference for type expansion
- [TripAdvisor Categories](https://developer-tripadvisor.com/content-api/business-content/categories-subcategories-and-types/) — traveler-oriented groupings
- [LLM Classification Best Practices](https://mattrickard.com/categorization-and-classification-with-llms) — constrained classification patterns
