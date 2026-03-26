---
title: "feat: Add analytics intelligence loop for slideshow performance optimization"
type: feat
status: completed
date: 2026-03-25
origin: docs/brainstorms/2026-03-25-analytics-intelligence-loop-requirements.md
deepened: 2026-03-25
---

# feat: Add analytics intelligence loop for slideshow performance optimization

## Overview

Close the feedback loop on slideshow generation: pull TikTok analytics from Postiz API, pull conversion data from RevenueCat V2 API, cross-reference them, learn what works across six dimensions (hook style, category, city, visual style, place virality, CTA), and auto-adjust future slideshow generation. A daily report surfaces intelligence; performance weights bias the pipeline toward winning content while preserving exploration.

## Problem Frame

The Atlasi slideshow pipeline is fire-and-forget. Slideshows are generated and posted but their TikTok performance is never captured. The system can't learn which hooks, categories, cities, or visual styles drive views and conversions. Every day it generates content with the same blind assumptions. (see origin: `docs/brainstorms/2026-03-25-analytics-intelligence-loop-requirements.md`)

## Requirements Trace

- R1. Pull per-post TikTok analytics from Postiz API, store in SQLite
- R2. Handle Postiz-to-TikTok release ID connection (draft → published → indexed)
- R3. Pull platform-level TikTok stats for delta tracking
- R4. Pull conversion data from RevenueCat V2 API
- R5. Cross-reference conversions with post publish times (72h attribution, last-touch model)
- R6. Diagnostic framework: detect CTA issues vs hook issues vs app issues
- R7. Track performance across 6 dimensions
- R8. Maintain `performance_weights.json` with learned per-dimension weights
- R9. Auto-adjust `generate_slideshow.py` with 70/30 exploit/explore ratio
- R10. Apply decision rules (50K+ DOUBLE DOWN, <1K twice DROP)
- R11. Daily report script pulling last 3 days
- R12. Report content: tables, deltas, diagnoses, recommendations, weight changes
- R13. Track hook performance history in SQLite

## Scope Boundaries

- No web dashboard — markdown reports only (see origin)
- No music automation — posts remain drafts (see origin)
- No changes to `discover.py` or scoring (see origin)
- No A/B testing infrastructure (see origin)
- No RevenueCat webhook server — REST API polling only (see origin)
- `--city` remains a required CLI argument; auto-adjustment applies to category, hook format, visual style, CTA, and place ranking within the chosen city

## Context & Research

### Relevant Code and Patterns

- `pipeline/db.py` — Schema, migration pattern (`ALTER TABLE ADD COLUMN` in try/except), DB helper conventions (conn as first param, explicit commit control)
- `generate_slideshow.py:148` — Place selection point (`available[:slide_count]`) where weighted selection injects
- `generate_slideshow.py:208` — Visual style selection (computed but never persisted)
- `generate_slideshow.py:265` — CTA text hardcoded as `"Find more hidden gems\non Atlasi"`
- `pipeline/posting.py` — Postiz integration, saves `postiz_post_id` to DB
- `pipeline/image_styles.py` — 4 style axes with named options (time_of_day, weather, perspective, color_mood)
- `pipeline/hooks.py` — Hook format dimension (listicle/story), `HOOK_TEMPLATES` dict
- `pipeline/retry.py` — `retry_with_backoff()` for external API calls
- `config.py` — API keys via `os.getenv()`, all constants defined once, `POSTIZ_BASE_URL` already set
- `tests/conftest.py` — In-memory SQLite fixture (`conn`, `city_id`)

### Institutional Learnings

- **Do not commit inside helper functions** — callers control transaction scope (from batch-resolve code review)
- **Index alongside every new column/table** — analytics queries will filter by date ranges, slideshow_id (from category feature review)
- **Validate all external API responses** before acting — check status, structure, size (from batch-resolve review)
- **Define thresholds as constants in `config.py`** — single source of truth (from category feature review)
- **Use `retry_with_backoff()` from `pipeline/retry.py`** for Postiz and RevenueCat calls (from batch-resolve review)
- **Sanitize analytics-derived text** before LLM prompt interpolation when feeding back into hook generation (from batch-resolve review)
- **Postiz per-post TikTok analytics may return empty arrays** — implement delta method as fallback (from Larry `analytics-loop.md`)

### External References

- **RevenueCat V2 API**: `/v2/projects/{project_id}/metrics/overview` for aggregate metrics (MRR, active_trials, active_subscriptions, revenue, new_customers). Rate limit: 5 req/min. Returns snapshot values, not time series — must poll daily and compute deltas.
- **RevenueCat V2 subscriptions**: `/v2/projects/{project_id}/subscriptions?status=trialing` for trial listing. No date filter — client-side filtering by `starts_at`. Rate limit: 480 req/min.
- **No official Python SDK** for RevenueCat — use raw `requests` (already a dependency).
- **V2 auth**: Strictly `Authorization: Bearer {v2_secret_key}`. V2 keys created separately in dashboard with granular permissions. Need `charts_metrics:overview:read` and `customer_information:subscriptions:read`.
- **Postiz API**: Per-post analytics via `GET /analytics/post/{postId}`, platform stats via `GET /analytics/{integrationId}`, posts list via `GET /posts?startDate&endDate`, release ID connection via `GET /posts/{id}/missing` + `PUT /posts/{id}/release-id`.

## Key Technical Decisions

- **Last-touch attribution model**: Each conversion is assigned to the single most recent published post that precedes the conversion timestamp. Avoids double-counting with overlapping 72h windows. (Resolves flow analysis gap #3)
- **Draft detection via 72h stale threshold with recovery**: Posts with `releaseId = "missing"` after 72 hours are marked `publish_status = "stale"` and excluded from analytics. Extended from 48h to accommodate weekend batch publishing. Stale status is reversible: each daily run re-checks stale posts for newly-connected release IDs on Postiz, transitioning them back to `published` if found. (Resolves flow analysis gap #2, deepened from data integrity review)
- **RevenueCat V2 with daily-snapshot-delta approach**: Call `/metrics/overview` daily, store snapshots in `rc_snapshots` table, compute deltas for the report. Use `/subscriptions?status=trialing` filtered client-side by `starts_at` for new trial attribution. No webhook server needed. (Resolves outstanding question on V1 vs V2)
- **Independent per-dimension weights with clamped combination**: `performance_weights.json` stores one weight per dimension value. Individual weights clamped to `[0.5, 2.0]`. At selection time, weights combine multiplicatively but the ratio between best and worst candidate is capped at `MAX_COMBINED_WEIGHT_RATIO = 10` (re-normalize after capping). This prevents 6-dimensional multiplication from producing extreme skew (unclamped could reach 40:1 ratios, making explore mode functionally extinct). (Deepened from architecture review)
- **Maturation-normalized view counts for weight computation**: Use views measured at 48 hours post-publish (`views_at_48h`) as the standard metric, since TikTok posts peak at 24-48h. Posts younger than 48h are excluded from weight calculations. This prevents a 14-day-old post with 50K cumulative views from unfairly dominating a 1-day-old post. Store both `views_at_48h` and `views_latest` in slideshow_performance. (Deepened from data integrity review)
- **30-day rolling window with exponential decay**: Weight computation only considers posts from the last 30 days (`WEIGHT_DECAY_DAYS`). Within that window, each post's contribution is weighted by `0.95^(days_since_post)` (~14-day half-life). This prevents stale data from anchoring weights, avoids oscillation from over-correction, and naturally handles the transition when RevenueCat is first enabled (old views-only weights decay out over ~30 days). (Deepened from architecture and data integrity reviews)
- **Composite scoring function with configurable blend**: `composite = normalized_views * alpha + conversions_per_1k * (1 - alpha)` where `SCORE_VIEWS_WEIGHT = 1.0` when RC is not configured, `0.6` when RC is active. Normalized views = views / global median. This is the single number that drives per-dimension weight computation. (Deepened from architecture review)
- **Visual style tracked as 4 sub-dimensions**: The `visual_style` JSON column stays as one column in `slideshows`, but weight computation decomposes it into 4 separate axes (time_of_day, weather, perspective, color_mood). Each axis has 5-6 values, reachable within weeks. Tracking as one composite (900 combinations) would be in permanent cold-start. Total weight dimensions: 9 (category, city, format, time_of_day, weather, perspective, color_mood, CTA, virality_band). (Deepened from architecture review)
- **Minimum sample size of 3 posts**: Dimension values with fewer than 3 matured posts (48h+) stay at weight 1.0. Above that, apply Bayesian smoothing: `adjusted = (n * observed + 5 * 1.0) / (n + 5)` where prior_n=5 dampens early noise. (From flow analysis recommendation)
- **Per-update weight change rate limiter**: No dimension weight moves more than 20% per daily update (`MAX_WEIGHT_DELTA_PER_DAY = 0.2`). Prevents a single viral post or flop from causing drastic reallocation. (Deepened from architecture review)
- **Stale draft recovery**: Each daily run re-checks all stale-status slideshows for newly-connected release IDs on Postiz. If found, transition from `stale` to `published` and begin collecting analytics. The stale threshold is 72h (not 48h) to accommodate batch publishing on weekends. (Deepened from data integrity review)
- **Phased commits around external mutations**: `daily_report.py` commits in 3 phases: (1) after Postiz data + release ID connections (external mutations that can't be rolled back), (2) after RevenueCat data, (3) after intelligence + weight computation. Each phase is idempotent. `slideshow_analytics` has `UNIQUE(slideshow_id, DATE(fetched_at))` with upsert semantics. (Deepened from data integrity review)
- **Circuit breaker**: If average views across all posts in the last 7 days drops below 50% of the 30-day average, reset all weights to 1.0 and log a critical warning. This is the last-resort guardrail against the learning system degrading account growth. (Deepened from architecture review)
- **Atomic JSON writes**: Write `performance_weights.json` to `.tmp` then `os.replace()` to prevent corruption from concurrent reads. (Resolves race condition concern)
- **SQLite for all analytics data**: Hook performance (R13), analytics snapshots, RC snapshots, platform stats — all in SQLite with proper indexes. Only `performance_weights.json` stays as JSON for fast read at generation time. (see origin)
- **Delta method fallback with confidence weighting**: When per-post analytics return empty arrays, estimate per-post views from platform-level total deltas. Flag as `views_estimated = TRUE` and assign `views_confidence = 0.5` so estimated views contribute half weight to dimension calculations. (Deepened from data integrity review)

## Open Questions

### Resolved During Planning

- **Q: Which RevenueCat API version?** → V2. The `/metrics/overview` endpoint provides aggregate metrics. V2 keys require separate creation in the dashboard with permissions `charts_metrics:overview:read` and `customer_information:subscriptions:read`.
- **Q: How to handle overlapping attribution windows?** → Last-touch attribution. Each conversion attributed to the single most recent published post before the conversion.
- **Q: How to detect unpublished drafts?** → 72h stale threshold (extended from 48h to accommodate weekend batch publishing). Posts still showing `releaseId = "missing"` after 72h are marked stale. Stale status is reversible — each run re-checks stale posts for newly-connected release IDs.
- **Q: Explore/exploit integration point?** → `--city` stays required. The 70/30 ratio applies when `--category` is omitted (auto-select category) and always applies to hook format, visual style, and CTA selection.
- **Q: Weight structure for 6 dimensions?** → Independent per-dimension weights with multiplicative combination. Bayesian smoothing with prior_n=5.
- **Q: Hook performance in SQLite or JSON?** → SQLite. New `slideshow_performance` table that combines hook text, dimensions, and analytics.

### Deferred to Implementation

- **Exact RevenueCat V2 subscription response structure**: Verify `starts_at` field format (milliseconds vs ISO 8601) with a live API call during implementation of Unit 3.
- **Postiz per-post analytics response for existing posts**: Make a test call to confirm which metrics are actually returned for connected TikTok posts. If views are missing, the delta method becomes primary rather than fallback.
- **CTA variant pool**: The initial set of CTA text variants to rotate through. Start with the current hardcoded text plus 2-3 alternatives; expand based on performance data.

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```
Daily Report Flow (morning cron):
┌─────────────────────────────────────────────────────────┐
│ daily_report.py                                          │
│                                                          │
│  1. analytics.fetch_post_analytics(conn, days=3)         │
│     ├── List posts from Postiz API                       │
│     ├── Connect release IDs for unconnected posts        │
│     ├── Pull per-post stats (views, likes, etc.)         │
│     ├── Fallback: delta method if per-post views empty   │
│     ├── Mark stale drafts (>48h, no release ID)          │
│     └── Store in slideshow_analytics table               │
│                                                          │
│  2. analytics.fetch_platform_stats(conn)                 │
│     ├── Pull platform-level stats from Postiz            │
│     └── Store in platform_stats table (delta tracking)   │
│                                                          │
│  3. conversions.fetch_revenuecat_data(conn)              │
│     ├── Pull /metrics/overview snapshot                  │
│     ├── Pull /subscriptions?status=trialing              │
│     ├── Store snapshot in rc_snapshots table             │
│     └── Compute deltas from previous snapshot            │
│                                                          │
│  4. conversions.attribute_conversions(conn, days=3)      │
│     ├── Cross-reference trial starts with post times     │
│     ├── Last-touch: assign to most recent prior post     │
│     └── Store attribution in slideshow_performance       │
│                                                          │
│  5. intelligence.update_weights(conn)                    │
│     ├── Aggregate performance by each dimension          │
│     ├── Apply min-sample (3) and Bayesian smoothing      │
│     ├── Apply decision rules (SCALE/FIX/DROP)            │
│     ├── Write performance_weights.json (atomic)          │
│     └── Return weight change summary                     │
│                                                          │
│  6. Generate markdown report                             │
│     ├── Per-slideshow table with diagnosis               │
│     ├── Platform growth deltas                           │
│     ├── RevenueCat summary + funnel health               │
│     ├── Hook recommendations from winners                │
│     ├── Weight changes summary                           │
│     └── Save to reports/YYYY-MM-DD.md                    │
└─────────────────────────────────────────────────────────┘

Generation-Time Flow (when --category omitted or --auto used):
┌─────────────────────────────────────────────────────────┐
│ generate_slideshow.py                                    │
│                                                          │
│  Read performance_weights.json                           │
│  Roll exploit (70%) vs explore (30%)                     │
│                                                          │
│  If exploit:                                             │
│    Select category weighted by category weights          │
│    Select hook format weighted by format weights         │
│    Select visual style weighted by style weights         │
│    Select CTA weighted by CTA weights                    │
│    Rank places using virality * place_band_weight        │
│                                                          │
│  If explore:                                             │
│    Select category/format/style/CTA uniformly random     │
│    Keep standard virality ranking for places             │
│                                                          │
│  Continue existing pipeline                              │
└─────────────────────────────────────────────────────────┘
```

## Implementation Units

- [ ] **Unit 1: Schema migrations and new config constants**

  **Goal:** Add all new database tables, columns, and indexes needed by the analytics loop. Add RevenueCat and analytics config constants.

  **Requirements:** R1, R3, R4, R7, R8, R13

  **Dependencies:** None

  **Files:**
  - Modify: `pipeline/db.py`
  - Modify: `config.py`
  - Modify: `.env.example`
  - Test: `tests/test_analytics_schema.py`

  **Approach:**
  - Add new columns to `slideshows` table via the existing migration pattern (ALTER TABLE ADD COLUMN in try/except):
    - `tiktok_release_id TEXT` — TikTok's native video ID (distinct from `postiz_post_id`)
    - `visual_style TEXT` — JSON string of the 4 style axis names (e.g., `'{"time_of_day":"golden_hour","weather":"clear","perspective":"street_level","color_mood":"warm_analog"}'`)
    - `cta_text TEXT` — the CTA text used on the final slide
    - `publish_status TEXT DEFAULT 'draft' CHECK(publish_status IN ('draft', 'published', 'stale'))` — tracks whether the TikTok draft was actually published. Stale is reversible — re-checked each run
  - Create new tables in `init_db()`:
    - `slideshow_analytics` — time-series per-post stats: `(id, slideshow_id FK, fetched_at TIMESTAMP, views INT, likes INT, comments INT, shares INT, saves INT, views_estimated BOOLEAN DEFAULT FALSE, UNIQUE(slideshow_id, DATE(fetched_at)))` — uniqueness constraint enables idempotent re-runs via `INSERT OR REPLACE`
    - `platform_stats` — daily platform-level snapshot: `(id, fetched_at TIMESTAMP, followers INT, total_views INT, total_likes INT, recent_comments INT, recent_shares INT, videos INT)`
    - `rc_snapshots` — daily RevenueCat snapshot: `(id, fetched_at TIMESTAMP, mrr REAL, active_trials INT, active_subscriptions INT, active_users INT, new_customers INT, revenue REAL)`
    - `slideshow_performance` — maturation-normalized performance record: `(id, slideshow_id FK, evaluated_at TIMESTAMP, views_at_48h INT, views_latest INT, likes INT, comments INT, shares INT, saves INT, conversions INT, conversion_rate REAL, composite_score REAL, views_estimated BOOLEAN, views_confidence REAL DEFAULT 1.0, decision_tag TEXT CHECK(decision_tag IN ('scale', 'keep', 'test', 'drop')))` — append-only table (not single-row-per-slideshow) so that decision rules like "<1K twice" can query historical evaluations
  - Add indexes: `(slideshow_analytics: slideshow_id, fetched_at)`, `(platform_stats: fetched_at)`, `(rc_snapshots: fetched_at)`, `(slideshows: postiz_post_id)`, `(slideshows: publish_status, posted_at)`, `(slideshow_performance: slideshow_id, evaluated_at)`, `(slideshow_performance: decision_tag)`
  - Add config constants to `config.py`:
    - `REVENUECAT_V2_SECRET_KEY = os.getenv("REVENUECAT_V2_SECRET_KEY", "")`
    - `REVENUECAT_PROJECT_ID = os.getenv("REVENUECAT_PROJECT_ID", "")`
    - `REVENUECAT_BASE_URL = "https://api.revenuecat.com/v2"`
    - `PERFORMANCE_WEIGHTS_PATH = Path("performance_weights.json")`
    - `ANALYTICS_LOOKBACK_DAYS = 3`
    - `ATTRIBUTION_WINDOW_HOURS = 72`
    - `STALE_DRAFT_HOURS = 72` (extended from 48 to accommodate weekend batch publishing)
    - `POST_MATURATION_HOURS = 48` (views measured at this age used for weight computation)
    - `MIN_POSTS_FOR_WEIGHT = 3`
    - `WEIGHT_PRIOR_N = 5`
    - `EXPLOIT_RATIO = 0.7`
    - `WEIGHT_DECAY_DAYS = 30` (rolling window for weight computation)
    - `WEIGHT_DECAY_FACTOR = 0.95` (per-day decay, ~14-day half-life)
    - `MAX_WEIGHT_DELTA_PER_DAY = 0.2` (rate limiter per update)
    - `MIN_WEIGHT = 0.5` / `MAX_WEIGHT = 2.0` (per-dimension clamps)
    - `MAX_COMBINED_WEIGHT_RATIO = 10` (cap on best-to-worst candidate ratio)
    - `SCORE_VIEWS_WEIGHT = 1.0` (alpha for composite score; set to 0.6 when RC is configured)
    - `CIRCUIT_BREAKER_THRESHOLD = 0.5` (reset weights if 7-day avg < 50% of 30-day avg)
    - Decision rule thresholds: `VIEWS_SCALE = 50_000`, `VIEWS_GOOD = 10_000`, `VIEWS_TEST = 1_000`
  - Add new env vars to `.env.example`

  **Patterns to follow:**
  - Migration pattern: `pipeline/db.py:99-116` (ALTER TABLE in try/except for "duplicate column name")
  - Table creation: `pipeline/db.py:119-140` (CREATE TABLE IF NOT EXISTS in executescript)
  - Index creation: `pipeline/db.py:142-152` (CREATE INDEX IF NOT EXISTS)
  - Config pattern: `config.py:49-51` (os.getenv with defaults)

  **Test scenarios:**
  - `init_db()` creates all new tables and columns on a fresh database
  - `init_db()` is idempotent — running twice does not error
  - New columns have correct defaults (publish_status='draft', views_estimated=FALSE)
  - Foreign keys on slideshow_analytics and slideshow_performance reference slideshows(id)
  - Indexes exist after init_db (query sqlite_master)

  **Verification:**
  - All existing tests still pass (schema is backward-compatible)
  - New test file passes with full coverage of schema creation

---

- [ ] **Unit 2: Postiz analytics module (`pipeline/analytics.py`)**

  **Goal:** Fetch TikTok post analytics from Postiz API, connect release IDs, detect stale drafts, store results in SQLite.

  **Requirements:** R1, R2, R3

  **Dependencies:** Unit 1

  **Files:**
  - Create: `pipeline/analytics.py`
  - Test: `tests/test_analytics.py`

  **Approach:**
  - Create a `PostizAnalyticsClient` or set of module-level functions that wrap the Postiz API:
    - `fetch_posts(days)` — GET `/posts?startDate&endDate`, filter to TikTok posts with `postiz_post_id` matching slideshows in DB
    - `connect_release_ids(conn, posts)` — For posts with missing release IDs: GET `/posts/{id}/missing` to get TikTok video list, match chronologically (higher ID = newer), PUT `/posts/{id}/release-id` to connect. Update `slideshows.tiktok_release_id` and set `publish_status = 'published'`
    - `detect_stale_drafts(conn)` — Mark slideshows where `posted_at` is >48h ago and `tiktok_release_id` is still NULL as `publish_status = 'stale'`
    - `fetch_post_analytics(conn, posts)` — GET `/analytics/post/{postId}` for each connected post. Parse response for views, likes, comments, shares, saves. Insert into `slideshow_analytics`
    - `fetch_platform_stats(conn)` — GET `/analytics/{integrationId}` using `POSTIZ_TIKTOK_INTEGRATION_ID`. Insert into `platform_stats`
    - Delta method fallback: if per-post analytics return empty arrays for views, compute estimated per-post views from platform-level total view deltas between consecutive `platform_stats` entries, divided among posts published in that window. Flag as `views_estimated = TRUE`
  - Use `retry_with_backoff()` from `pipeline/retry.py` for all API calls
  - Validate API responses before processing (check status, structure)
  - Rate-limit calls with configurable delay between requests (existing pattern from `POSTIZ_UPLOAD_DELAY`)
  - All DB functions accept `conn` as first param, do NOT commit internally — the caller (daily_report.py) controls the transaction

  **Patterns to follow:**
  - API call pattern: `pipeline/posting.py` (headers, error handling, Postiz base URL)
  - Retry pattern: `pipeline/retry.py:retry_with_backoff()` with non-retryable exceptions
  - Error hierarchy: Create `AnalyticsError` (retryable) + `AnalyticsAuthError(AnalyticsError)` (non-retryable), matching `PostingError`/`PostingAuthError` pattern
  - Sleep between API calls: `pipeline/posting.py` uses `time.sleep(POSTIZ_UPLOAD_DELAY)`

  **Test scenarios:**
  - Fetches posts and filters to TikTok-only with matching slideshow records
  - Connects release IDs correctly via chronological matching (oldest post → lowest video ID)
  - Skips posts published <2 hours ago (TikTok indexing delay)
  - Detects stale drafts (>48h, no release ID) and marks them
  - Stores per-post analytics in slideshow_analytics table
  - Falls back to delta method when per-post views return empty
  - Flags delta-estimated views as `views_estimated = TRUE`
  - Handles Postiz API errors gracefully (auth error, rate limit, timeout)
  - Handles empty post list (no posts in date range)
  - Platform stats stored correctly with delta computation

  **Verification:**
  - Analytics data for posted slideshows appears in slideshow_analytics table
  - Stale drafts are excluded from analytics (publish_status = 'stale')
  - Platform stats show meaningful deltas when run on consecutive days

---

- [ ] **Unit 3: RevenueCat conversions module (`pipeline/conversions.py`)**

  **Goal:** Fetch conversion data from RevenueCat V2 API, store daily snapshots, attribute conversions to specific slideshows.

  **Requirements:** R4, R5, R6

  **Dependencies:** Unit 1

  **Files:**
  - Create: `pipeline/conversions.py`
  - Test: `tests/test_conversions.py`

  **Approach:**
  - Create a `RevenueCatClient` class using raw `requests`:
    - Constructor: takes `v2_secret_key` and `project_id`, creates a `requests.Session` with `Authorization: Bearer {key}` header
    - `get_overview_metrics(currency="USD")` — GET `/v2/projects/{project_id}/metrics/overview`. Returns dict of metric ID → value
    - `list_subscriptions(status=None, limit=50)` — Paginate through `/v2/projects/{project_id}/subscriptions`. Returns full list
    - `get_recent_trials(days)` — Filter `list_subscriptions(status="trialing")` by `starts_at` within the last N days (client-side)
  - Module-level functions for the pipeline:
    - `fetch_rc_snapshot(conn)` — Call `get_overview_metrics()`, insert into `rc_snapshots` table
    - `compute_rc_deltas(conn)` — Compare latest two `rc_snapshots` entries, return dict of deltas
    - `attribute_conversions(conn, days)` — For each new trial (from `get_recent_trials`), find the most recent published slideshow whose `posted_at` is before the trial's `starts_at`. This is last-touch attribution. Update the slideshow's `slideshow_performance.conversions` count
    - `diagnose_funnel(views_good, conversions_good, has_rc_data)` — Return diagnostic string using the framework from R6: SCALE / FIX CTA / FIX HOOKS / NEEDS WORK
  - Graceful degradation: if RevenueCat is not configured (`REVENUECAT_V2_SECRET_KEY` empty), skip all RC operations and log a warning. The report still generates with Postiz-only data
  - Use `retry_with_backoff()` for RC API calls
  - RC error hierarchy: `RevenueCatError` (retryable) + `RevenueCatAuthError` (non-retryable)

  **Patterns to follow:**
  - Client class pattern: similar to how `pipeline/posting.py` wraps Postiz (session-based HTTP)
  - Error hierarchy: `pipeline/llm.py` (`LLMError`/`CreditsExhaustedError`) pattern
  - Graceful degradation: similar to how `generate_slideshow.py` handles optional `--post` flag

  **Test scenarios:**
  - Overview metrics parsed correctly from API response format (`{"metrics": [{"id": "mrr", "value": 670}]}`)
  - Subscription pagination follows `next_page` cursor through multiple pages
  - Recent trials filtered correctly by `starts_at` within last N days
  - Snapshot stored in rc_snapshots table
  - Deltas computed correctly between consecutive snapshots
  - Last-touch attribution: conversion assigned to most recent post before trial start, not all posts in window
  - Multiple conversions on same day attributed to correct posts
  - Diagnostic framework: all 4 quadrants produce correct diagnosis
  - Graceful skip when RC not configured (no crash, warning logged)
  - Handles RC API errors (429 rate limit, auth error, network timeout)

  **Verification:**
  - RC snapshot data appears in rc_snapshots table with deltas computable
  - Conversions attributed to specific slideshows in slideshow_performance table
  - System works with RC disabled (Postiz-only mode)

---

- [ ] **Unit 4: Intelligence module (`pipeline/intelligence.py`)**

  **Goal:** Aggregate performance data across 6 dimensions, compute Bayesian-smoothed weights, apply decision rules, write `performance_weights.json`.

  **Requirements:** R7, R8, R9, R10, R13

  **Dependencies:** Unit 1, Unit 2, Unit 3

  **Files:**
  - Create: `pipeline/intelligence.py`
  - Test: `tests/test_intelligence.py`

  **Approach:**
  - `evaluate_slideshows(conn)` — For each published slideshow that has matured (48h+ since publish) and has analytics data: compute `views_at_48h` (from the `slideshow_analytics` snapshot closest to 48h post-publish), compute `composite_score` using the configurable blend (`normalized_views * alpha + conversions_per_1k * (1 - alpha)`), apply decision rules, and insert an immutable evaluation row into `slideshow_performance`. The append-only design supports the "<1K twice" rule by querying past evaluations for the same dimension combination
  - `compute_dimension_weights(conn)` — For each of the 9 dimensions (category, city, format, time_of_day, weather, perspective, color_mood, CTA, virality_band):
    1. Query `slideshow_performance` joined with `slideshows` for posts within `WEIGHT_DECAY_DAYS` (30 days)
    2. Apply exponential decay: each post's contribution weighted by `WEIGHT_DECAY_FACTOR ^ days_since_post`
    3. For groups with >= `MIN_POSTS_FOR_WEIGHT` (3) matured posts: compute decay-weighted average composite_score
    4. Compute raw weight as ratio to overall decay-weighted average
    5. Apply Bayesian smoothing: `adjusted = (n * raw + WEIGHT_PRIOR_N * 1.0) / (n + WEIGHT_PRIOR_N)`
    6. Clamp to `[MIN_WEIGHT, MAX_WEIGHT]` (0.5 to 2.0)
    7. Apply rate limiter: if previous weight exists, clamp delta to `MAX_WEIGHT_DELTA_PER_DAY` (20%)
    8. For groups with < 3 matured posts: weight stays at 1.0
    9. Return nested dict with 9 dimension keys
  - `check_circuit_breaker(conn)` — If 7-day average views < 50% of 30-day average, reset all weights to 1.0, log critical warning, and include prominent alert in the daily report
  - `apply_decision_rules(conn)` — For each slideshow in the analysis window, using `views_at_48h`:
    - 50K+ → tag as "scale" (generate variations)
    - 10K-50K → tag as "keep"
    - 1K-10K → tag as "test" (one more chance)
    - <1K on two separate matured evaluations for same dimension combination → tag as "drop"
    - Stored immutably in `slideshow_performance.decision_tag`
  - `write_weights(weights_dict, previous_weights)` — Compute combined weights for each candidate to verify `MAX_COMBINED_WEIGHT_RATIO` is respected. Write to `.tmp` then `os.replace()`. Include metadata: `{"_meta": {"updated_at": ..., "post_count": ..., "alpha": ..., "circuit_breaker": false}, ...}`
  - `read_weights()` — Read `performance_weights.json`, return defaults (all 1.0) if file missing or corrupt. Log which weight version was loaded (from `_meta.updated_at`)
  - The "place virality band" dimension: bucket `places.virality_score` into bands (0-25, 25-50, 50-75, 75-100 percentile) and track performance per band
  - Visual style decomposition: parse the `visual_style` JSON column into 4 separate sub-dimensions for grouping. Each axis tracked independently

  **Patterns to follow:**
  - Constants from `config.py` (MIN_POSTS_FOR_WEIGHT, WEIGHT_PRIOR_N, EXPLOIT_RATIO, view thresholds)
  - DB query pattern: `pipeline/db.py` (parameterized queries, conn as first param)
  - JSON file I/O: `pipeline/slideshow_types.py:to_meta_json()` pattern

  **Test scenarios:**
  - Weights are 1.0 for dimensions with < 3 matured posts (cold start)
  - Weights diverge correctly when one category has 10 posts averaging 20K views and another has 10 posts averaging 5K
  - Bayesian smoothing dampens small-sample outliers (1 post with 100K views doesn't produce weight of 10.0)
  - Per-dimension clamp: no individual weight exceeds 2.0 or falls below 0.5
  - Rate limiter: weight cannot change more than 20% in a single update
  - Exponential decay: a 28-day-old post contributes ~25% as much as a 1-day-old post
  - Composite score blends views and conversions correctly (alpha=1.0 is views-only, alpha=0.6 blends)
  - Posts younger than 48h are excluded from weight computation
  - Circuit breaker triggers when 7-day avg < 50% of 30-day avg → all weights reset to 1.0
  - Decision rules use `views_at_48h`, not cumulative views
  - "<1K twice" detection queries immutable past evaluations, not overwritten rows
  - Visual style decomposed into 4 sub-dimensions for weight grouping
  - 9 dimension keys in output (category, city, format, time_of_day, weather, perspective, color_mood, cta, virality_band)
  - Atomic write: JSON file is valid even if interrupted (tmp + rename)
  - `read_weights()` returns defaults for missing or corrupt file
  - Stale drafts excluded; recovered drafts included
  - Empty database produces all-1.0 weights without error

  **Verification:**
  - `performance_weights.json` is written with valid structure and reasonable values
  - All individual weights within [0.5, 2.0] clamp
  - Combined weight ratio between best and worst candidate ≤ 10
  - Weights shift toward high-performing dimensions over multiple daily runs
  - Cold-start behavior produces neutral weights and no crashes

---

- [ ] **Unit 5: Persist visual style and CTA in `generate_slideshow.py`**

  **Goal:** Store the visual style and CTA text on each slideshow record so performance can be tracked by these dimensions.

  **Requirements:** R7 (visual style and CTA dimensions)

  **Dependencies:** Unit 1

  **Files:**
  - Modify: `generate_slideshow.py`
  - Modify: `pipeline/db.py` (add `update_slideshow_style` helper)
  - Test: `tests/test_generate_slideshow.py` (update existing tests)

  **Approach:**
  - After `select_slideshow_style()` on line 208, serialize the style axis names to a JSON string and store it on the slideshow record
  - Add a DB helper `update_slideshow_metadata(conn, slideshow_id, visual_style_json, cta_text)` that updates the new columns. Call it after `create_slideshow()` but before `conn.commit()` to stay atomic
  - Extract the CTA text to a variable (currently hardcoded at line 265) so it can be both stored and later varied
  - No behavioral changes — same visual style selection, same CTA text. This unit only adds persistence

  **Patterns to follow:**
  - `db.create_slideshow()` / `db.add_slideshow_place()` pattern (no internal commit, caller commits)
  - `json.dumps()` for style serialization (match `pipeline/slideshow_types.py` pattern)

  **Test scenarios:**
  - Slideshow record in DB has non-null `visual_style` and `cta_text` after generation
  - Visual style JSON round-trips correctly (write then read back)
  - Existing tests still pass (backward compatibility)

  **Verification:**
  - Run `generate_slideshow.py` and verify the slideshows table row has `visual_style` and `cta_text` populated

---

- [ ] **Unit 6: Weighted selection in `generate_slideshow.py`**

  **Goal:** Read `performance_weights.json` and use it to bias category, hook format, visual style, CTA, and place selection when generating slideshows.

  **Requirements:** R9, R10

  **Dependencies:** Unit 4, Unit 5

  **Files:**
  - Modify: `generate_slideshow.py`
  - Modify: `pipeline/image_styles.py` (add weighted style selection variant)
  - Modify: `pipeline/hooks.py` (accept format as suggestion, not just hardcoded choice)
  - Create: `pipeline/weighted_selection.py` (shared selection utilities)
  - Test: `tests/test_weighted_selection.py`

  **Approach:**
  - Create `pipeline/weighted_selection.py` with:
    - `weighted_choice(options, weights, default_weight=1.0)` — given a list of options and a weight dict, return a weighted random choice. If exploit roll (70%), use weights. If explore roll (30%), use uniform random
    - `clamped_combined_weight(dimension_weights, candidate)` — multiply per-dimension weights for a candidate, then cap ratio vs best/worst at `MAX_COMBINED_WEIGHT_RATIO`
    - `weighted_rank(items, score_fn, weight)` — re-rank a list by multiplying each item's score by its weight. For place selection: `final_score = virality_score * place_band_weight`
  - In `generate_slideshow.py`:
    - Read `performance_weights.json` via `intelligence.read_weights()` at startup
    - If `--category` is not provided, auto-select category using `weighted_choice(VALID_CATEGORIES, weights["category"])`
    - Select hook format using `weighted_choice(["listicle", "story"], weights["format"])` instead of always using `--format` default
    - Select CTA text from a pool of variants using `weighted_choice(CTA_VARIANTS, weights["cta"])`. Define initial `CTA_VARIANTS` list in `config.py`
    - Replace `available[:slide_count]` with `weighted_rank` that multiplies virality_score by the place's virality band weight
  - In `pipeline/image_styles.py`:
    - **Replace the deterministic `select_slideshow_style()` with per-axis weighted selection.** The current SHA256-seeded approach cannot accept weights (you can't "bias a seed"). New approach: 4 separate `random.choices()` calls, one per style axis, using weights from `performance_weights.json` (keys: `time_of_day`, `weather`, `perspective`, `color_mood`). Check `_is_compatible()` after selection; re-roll if incompatible (keep `_MAX_REROLLS` pattern). This explicitly abandons `(city, date)` determinism, which has no production value — you don't re-run the same city+date
    - `get_perspectives_for_slides()` also uses seeded RNG — leave it unweighted (per-slide perspective rotation is cosmetic, not a tracked dimension)
  - CLI behavior: `--category` and `--format` still override weights when explicitly provided (hard contract). Weights only apply when the user doesn't specify these arguments

  **Patterns to follow:**
  - `random.choices()` with `weights` parameter for weighted random selection
  - Existing CLI override pattern: `args.category or auto_selected_category`

  **Test scenarios:**
  - With all-1.0 weights, selection is effectively uniform random (no bias)
  - With extreme weights (e.g., food_and_drink: 10.0, everything else: 0.1), food_and_drink is selected in >90% of exploit runs
  - Explore mode (30%) produces uniform distribution regardless of weights
  - `--category food_and_drink` overrides weight-based selection
  - `--format story` overrides weight-based format selection
  - Place ranking changes when place band weights are non-uniform
  - Missing or corrupt weights file → all defaults (1.0), no crash
  - CTA variant pool correctly used; selected CTA stored on slideshow record

  **Verification:**
  - Run `generate_slideshow.py` without `--category` multiple times with skewed weights → confirm selection is biased
  - Run with `--category` → confirm override works
  - Verify selected CTA text varies across runs

---

- [ ] **Unit 7: Daily report script (`daily_report.py`)**

  **Goal:** Orchestrate the full daily analytics cycle and generate a comprehensive markdown report.

  **Requirements:** R11, R12

  **Dependencies:** Unit 2, Unit 3, Unit 4

  **Files:**
  - Create: `daily_report.py`
  - Create: `reports/` directory (auto-created)
  - Test: `tests/test_daily_report.py`

  **Approach:**
  - New CLI entry point at project root (same level as `discover.py`, `generate_slideshow.py`)
  - Arguments: `--days` (default 3), `--verbose`, `--quiet`
  - Orchestration flow (3 commit phases for idempotency around external mutations):
    - **Phase 1 — Postiz data (commit after):** Release ID connections via PUT are irreversible external mutations. `connect_release_ids` must reconcile: if Postiz shows a release ID but local DB does not, update locally rather than re-connecting. Re-check stale-status slideshows for newly-connected IDs (stale recovery)
      1. `analytics.connect_release_ids(conn, days)` — connect + recover stale drafts
      2. `analytics.fetch_post_analytics(conn, days)` — pull per-post stats (upsert via UNIQUE constraint)
      3. `analytics.fetch_platform_stats(conn)` — pull platform-level stats
      4. `analytics.detect_stale_drafts(conn)` — mark slideshows >72h without release ID
      5. `conn.commit()`
    - **Phase 2 — RevenueCat data (commit after, if configured):**
      6. `conversions.fetch_rc_snapshot(conn)` — pull overview metrics
      7. `conversions.attribute_conversions(conn, days)` — last-touch attribution
      8. `conn.commit()`
    - **Phase 3 — Intelligence + report (commit after):**
      9. `intelligence.evaluate_slideshows(conn)` — maturation-normalized evaluation, composite scoring
      10. `intelligence.check_circuit_breaker(conn)` — safety check before weight update
      11. `intelligence.compute_dimension_weights(conn)` — with decay, smoothing, clamping, rate limiting
      12. `intelligence.write_weights(...)` — atomic JSON write
      13. Generate markdown report from all collected data
      14. `conn.commit()`
  - Report sections (adapted from Larry's `daily-report.js`):
    - Header with date and lookback window
    - Per-slideshow performance table: date, hook, city, category, views, likes, comments, shares, conversions, diagnosis
    - Platform growth since last report (followers, views deltas)
    - RevenueCat summary (if configured): MRR, active trials, active subs, revenue deltas, funnel health diagnosis
    - Conversion attribution: which slideshows drove trials/conversions
    - Per-post funnel diagnosis using the 4-quadrant framework (SCALE / FIX CTA / FIX HOOKS / NEEDS WORK)
    - Weight changes: what shifted, what was promoted to "double down", what was dropped
    - Hook recommendations: top-performing hook patterns with suggested variations
    - Cold-start notice: when < 10 posts exist, display "Insufficient data — weights not adjusted"
  - Save to `reports/YYYY-MM-DD.md`
  - Print summary to stdout

  **Patterns to follow:**
  - CLI structure: `generate_slideshow.py` (argparse, setup_logging, main with try/finally conn.close())
  - Error handling: catch `AnalyticsAuthError` and `RevenueCatAuthError` at CLI level with user-friendly messages
  - Report format: reference Larry's `daily-report.js` output format but adapt for Atlasi's multi-dimensional data

  **Test scenarios:**
  - Full report generates correctly with mocked Postiz and RC API responses
  - Report generates in Postiz-only mode when RC not configured (no RC section)
  - Cold-start: report indicates "insufficient data" when < 10 posts
  - Report file is saved to correct path with correct date
  - Weight changes section reflects actual changes from intelligence module
  - Handles API failures gracefully (partial report with error notes)
  - Empty date range (no posts in window) produces a valid but minimal report

  **Verification:**
  - Run `daily_report.py --days 3` and confirm a markdown file appears in `reports/`
  - Report is readable and actionable within 2 minutes (success criterion)
  - Weights file is updated after report runs

## System-Wide Impact

- **Interaction graph:** `daily_report.py` calls `analytics.py`, `conversions.py`, `intelligence.py` in 3 commit phases. `generate_slideshow.py` reads `performance_weights.json` written by `intelligence.py`. The only shared mutable state is the SQLite database (WAL mode handles concurrency) and the JSON weights file (atomic writes handle concurrent reads).
- **Error propagation:** API errors in analytics/conversions are caught per-phase; earlier phases commit independently so later failures don't roll back external mutation effects. Auth errors surface to the user with actionable messages. `generate_slideshow.py` treats missing/corrupt weights as "use defaults" — never crashes on weights issues.
- **State lifecycle risks:** (1) `performance_weights.json` — corrupt writes cause silent fallback to defaults; mitigated by atomic tmp+rename. (2) Negative feedback compounding — weights that degrade content quality compound daily because worse content → worse data → worse weights; mitigated by circuit breaker (50% drop → reset), rate limiter (20% max change/day), weight clamps ([0.5, 2.0]), combined ratio cap (10:1), and temporal decay (30-day window). (3) External mutation inconsistency — Postiz release ID PUTs can't be rolled back; mitigated by phased commits and reconciliation on re-run.
- **Data integrity:** `slideshow_analytics` has `UNIQUE(slideshow_id, DATE(fetched_at))` for idempotent re-runs. `slideshow_performance` is append-only (immutable evaluations) to support decision rules that query history. Maturation-normalized views (48h) prevent temporal bias in weight computation.
- **API surface parity:** No external APIs are exposed. The Flask dashboard is not modified.
- **Integration coverage:** The daily report end-to-end flow (Postiz mock → 3 commit phases → SQLite → RC mock → weights → report) should be covered by an integration test. The generate_slideshow weighted-selection flow should be tested with pre-seeded weights. Circuit breaker should be tested with synthetic degradation data.

## Risks & Dependencies

- **Negative feedback compounding** — The most dangerous risk. Weights that degrade content quality compound daily: worse content → worse data → worse weights → even worse content. Mitigated by 5 layers: (1) circuit breaker resets weights if 7-day avg drops below 50% of 30-day avg, (2) per-update rate limiter caps changes at 20%/day, (3) per-dimension clamps [0.5, 2.0], (4) combined weight ratio cap of 10:1, (5) 30% explore mode always generates some unweighted content.
- **Weight oscillation** — Category A performs well → system posts more A → audience saturates → A drops → system pivots to B → repeat. Mitigated by exponential decay (recent data matters more), rate limiter (slow pivots), and combined weight cap (no single dimension can fully dominate).
- **Postiz per-post analytics may not include views for TikTok** — The delta method fallback is designed for this, but estimated views are noisier. Mitigated by flagging estimated values with `views_confidence = 0.5` so they contribute half weight to dimension calculations.
- **Metric transition (views-only → views+conversions)** — When RevenueCat is first configured, the scoring function changes from alpha=1.0 to alpha=0.6. Old weights based on views-only data are still valid but suboptimal. Mitigated by the 30-day decay window: old views-only data naturally phases out as new blended data accumulates. No special migration needed.
- **RevenueCat V2 key requires manual dashboard setup** — The user must create a V2 API key with specific permissions (`charts_metrics:overview:read`, `customer_information:subscriptions:read`). The system fails gracefully if not configured.
- **Early-stage data sparsity** — In the first 1-2 weeks, there may not be enough data for meaningful weights. Mitigated by minimum sample size threshold (3 matured posts), Bayesian smoothing (prior_n=5), and cold-start behavior (explicit "insufficient data" notice in report, weights stay at 1.0).
- **Attribution accuracy** — Last-touch attribution is a simplification. Acceptable given daily-posting cadence and simplicity goals. Future improvement: log `attribution_candidates` (all posts in window) alongside the last-touch winner to enable eventual migration to distributed attribution.

## Sources & References

- **Origin document:** [docs/brainstorms/2026-03-25-analytics-intelligence-loop-requirements.md](docs/brainstorms/2026-03-25-analytics-intelligence-loop-requirements.md)
- **Larry reference architecture:** `docs/larry-1.0.0/references/analytics-loop.md`, `docs/larry-1.0.0/references/revenuecat-integration.md`
- **Larry reference scripts:** `docs/larry-1.0.0/scripts/check-analytics.js`, `docs/larry-1.0.0/scripts/daily-report.js`
- **RevenueCat V2 API docs:** https://www.revenuecat.com/docs/api-v2
- **Postiz API:** Endpoints documented in `docs/larry-1.0.0/references/analytics-loop.md`
- **Institutional learnings:** `docs/solutions/logic-errors/batch-resolve-slideshow-pipeline-code-review-findings.md`, `docs/solutions/logic-errors/batch-resolve-category-feature-code-review-findings.md`
