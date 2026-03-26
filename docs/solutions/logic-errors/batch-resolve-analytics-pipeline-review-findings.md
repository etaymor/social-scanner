---
title: "Analytics pipeline batch review fixes: attribution dedup, evaluation filter, report joins, error classification"
category: logic-errors
date: 2026-03-25
problem_type: logic_error
component: service_object
root_cause: logic_error
resolution_type: code_fix
severity: high
tags: [analytics, conversions, intelligence, attribution, sqlite, idempotency, n-plus-1, error-handling, delta-fallback, gitignore]
module: pipeline
files_changed:
  - pipeline/conversions.py
  - pipeline/analytics.py
  - pipeline/intelligence.py
  - daily_report.py
  - pipeline/db.py
  - config.py
  - .gitignore
related_docs:
  - docs/solutions/logic-errors/batch-resolve-category-feature-code-review-findings.md
  - docs/solutions/logic-errors/batch-resolve-slideshow-pipeline-code-review-findings.md
---

# Analytics Pipeline Batch Review Fixes

## Problem

The Atlasi analytics intelligence pipeline had 11 code review findings across its Python/SQLite stack, ranging from critical data corruption bugs to hygiene issues. The most severe issues caused conversion counts to double on every pipeline re-run, evaluation to permanently skip attributed slideshows, and daily reports to return Cartesian product row explosions. Several bugs compounded each other: the same re-run that doubled conversions also overwrote delta-estimated analytics values with zeros.

## Symptoms

- Conversion counts climbing indefinitely on each pipeline run (2x, 3x, 4x real conversions)
- Phase 3 evaluation silently producing zero decisions for any slideshow that had gone through Phase 2 attribution, starving the weights engine
- Daily reports returning hundreds of rows for a small catalog (Cartesian product of analytics x performance rows per slideshow)
- Draft slideshows with a `posted_at` timestamp receiving attribution credit they were never entitled to
- Second same-day analytics fetch resetting delta-estimated view counts to zero
- A 400 Bad Request from RevenueCat printing "Check REVENUECAT_V2_SECRET_KEY" -- a false diagnostic
- Delta fallback silently doing nothing for any fetch crossing midnight UTC
- `performance_weights.json` and `reports/` appearing in git diffs with business intelligence data

## What Didn't Work

N/A -- these findings were identified by automated code review (`ce:review`), not through debugging a live failure.

## Solution

### Theme 1: Data Integrity (Fixes 1, 4, 5, 8)

**Fix 1 -- Duplicate conversion attribution** (`pipeline/conversions.py`, `pipeline/db.py`)

Created a `trial_attributions` table with a `UNIQUE` constraint on `trial_id`:

```sql
CREATE TABLE IF NOT EXISTS trial_attributions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trial_id TEXT UNIQUE NOT NULL,
    slideshow_id INTEGER NOT NULL REFERENCES slideshows(id) ON DELETE CASCADE,
    attributed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Before attributing, check if already processed:

```python
# Before:
for trial in trials:
    # No dedup -- re-runs re-attribute everything
    row = conn.execute("SELECT id FROM slideshows WHERE posted_at < ?", ...)

# After:
for trial in trials:
    already = conn.execute(
        "SELECT 1 FROM trial_attributions WHERE trial_id = ?", (trial_id,)
    ).fetchone()
    if already:
        continue
    # ... find slideshow, attribute, then record:
    conn.execute(
        "INSERT OR IGNORE INTO trial_attributions (trial_id, slideshow_id) VALUES (?, ?)",
        (trial_id, slideshow_id),
    )
```

**Fix 4 -- Missing publish_status filter** (`pipeline/conversions.py`)

Added `AND publish_status = 'published'` to the attribution candidate query. Drafts that happen to have a `posted_at` value are now excluded.

**Fix 5 -- INSERT OR REPLACE destroying delta values** (`pipeline/analytics.py`)

```python
# Before: INSERT OR REPLACE deletes the existing row entirely
conn.execute(
    "INSERT OR REPLACE INTO slideshow_analytics ..."
)

# After: ON CONFLICT preserves the higher views value
conn.execute("""
    INSERT INTO slideshow_analytics (slideshow_id, fetched_at, views, ...)
    VALUES (?, CURRENT_TIMESTAMP, ?, ...)
    ON CONFLICT(slideshow_id, DATE(fetched_at)) DO UPDATE SET
      views = MAX(excluded.views, slideshow_analytics.views),
      likes = excluded.likes,
      views_estimated = CASE
        WHEN excluded.views > slideshow_analytics.views THEN excluded.views_estimated
        ELSE slideshow_analytics.views_estimated
      END
""")
```

**Fix 8 -- Delta fallback midnight race** (`pipeline/analytics.py`)

```python
# Before: DATE('now') may differ from DATE(fetched_at) across midnight
conn.execute(
    "UPDATE slideshow_analytics SET views = ? "
    "WHERE slideshow_id = ? AND DATE(fetched_at) = DATE('now')", ...
)

# After: target by row identity, not time string
conn.execute(
    "UPDATE slideshow_analytics SET views = ? "
    "WHERE id = (SELECT id FROM slideshow_analytics "
    "WHERE slideshow_id = ? ORDER BY fetched_at DESC LIMIT 1)", ...
)
```

### Theme 2: Query Correctness (Fixes 2, 3)

**Fix 2 -- Evaluate filter skipping attributed slideshows** (`pipeline/intelligence.py`)

```sql
-- Before: any performance row blocks evaluation
AND NOT EXISTS (
    SELECT 1 FROM slideshow_performance sp WHERE sp.slideshow_id = s.id
)

-- After: only fully-evaluated rows block re-evaluation
AND NOT EXISTS (
    SELECT 1 FROM slideshow_performance sp
    WHERE sp.slideshow_id = s.id AND sp.decision_tag IS NOT NULL
)
```

**Fix 3 -- Report duplicate rows** (`daily_report.py`)

```sql
-- Before: flat join against multi-row table = Cartesian product
LEFT JOIN slideshow_analytics sa ON sa.slideshow_id = s.id

-- After: correlated subquery pins to latest row
LEFT JOIN slideshow_analytics sa ON sa.id = (
    SELECT id FROM slideshow_analytics
    WHERE slideshow_id = s.id ORDER BY fetched_at DESC LIMIT 1
)
```

Same pattern applied to `slideshow_performance` join.

### Theme 3: Performance (Fix 6)

**Fix 6 -- N+1 virality query** (`pipeline/intelligence.py`)

```python
# Before: per-slideshow query in loop (~30 queries)
for row in rows:
    vr = conn.execute(
        "SELECT AVG(p.virality_score) ... WHERE sp_link.slideshow_id = ?", (sid,)
    ).fetchone()

# After: single batch query
placeholders = ",".join("?" * len(slideshow_ids))
vr_rows = conn.execute(f"""
    SELECT sp_link.slideshow_id, AVG(p.virality_score) as avg_v
    FROM slideshow_places sp_link
    JOIN places p ON p.id = sp_link.place_id
    WHERE sp_link.slideshow_id IN ({placeholders})
    GROUP BY sp_link.slideshow_id
""", slideshow_ids).fetchall()
```

Also merged triple iteration over `rows` into a single pass.

### Theme 4: Error Handling (Fix 7)

**Fix 7 -- 4xx error misclassification** (`pipeline/conversions.py`)

```python
# Before: all non-429 4xx raised AuthError
class RevenueCatAuthError(RevenueCatError): ...

# After: separate error for non-auth client errors
class RevenueCatClientError(RevenueCatError):
    """Non-retryable client error (400/404/422 etc.)."""

# In _get():
if resp.status_code in (401, 403):
    raise RevenueCatAuthError(...)
if resp.status_code == 429:
    raise RevenueCatError(...)  # retryable
if 400 <= resp.status_code < 500:
    raise RevenueCatClientError(...)  # non-retryable, non-auth
```

Updated `non_retryable` tuple and fixed `AnalyticsError` docstring.

### Theme 5: Security and Hygiene (Fixes 9, 10, 11)

**Fix 9** -- Deleted dead module-level `_HEADERS` dict that captured API key at import time.

**Fix 10** -- Added `performance_weights.json`, `performance_weights.tmp`, `reports/` to `.gitignore`.

**Fix 11** -- Removed `ATTRIBUTION_WINDOW_HOURS` (never referenced), `OVERVIEW_METRIC_IDS` (never referenced), `import math` (unused), `phase2_data` parameter (never read). Updated test call sites.

## Why This Works

**Root cause patterns identified:**

1. **Missing idempotency guards.** The pipeline was designed to be re-runnable, but `attribute_conversions` had no mechanism to remember prior work. The `trial_attributions` table gives the function a durable, queryable log of completed work.

2. **Ambiguous row identity in multi-row tables.** Three separate bugs (INSERT OR REPLACE, delta fallback, report joins) assumed 1:1 relationship between a slideshow and a row in a related table. Using `MAX()` conflict resolution and correlated `ORDER BY ... LIMIT 1` subqueries gives each query a deterministic target regardless of accumulated rows.

3. **Phase boundary leakage.** Phase 2 left a side effect (a bare performance row) that Phase 3's guard predicate did not anticipate. The fix encodes the actual semantic ("has this been decided") rather than the structural proxy ("does any row exist").

4. **Overly broad exception hierarchies.** A single exception class for all 4xx forced callers to choose between "retry everything" and "surface auth error for everything." Separating by HTTP semantics allows accurate error messages and retry logic.

5. **Import-time side effects.** `_HEADERS` capturing the API key at import time is subtle coupling; `_get_headers()` defers that lookup correctly.

## Prevention

### Idempotency by default
Any pipeline step that writes derived data should have a deduplication primitive (unique key, `INSERT OR IGNORE`, "already processed" check) before business logic. Treat re-runnability as a first-class requirement.

### One semantic, one query target
When joining against a table that accumulates rows over time, always anchor to a specific row using `ORDER BY ... LIMIT 1` or a window function. A flat join against a multi-row table is a latent Cartesian product.

### Phase contracts in tests
Integration tests for each pipeline phase should assert the exact state of every related table after the phase completes, including which columns are NULL and populated. Fix 2's bug would have been caught by a test asserting Phase 2 does not set `decision_tag`.

### Exception taxonomy matches HTTP semantics
Define one exception subclass per meaningful HTTP error category (auth failure, client error, server error, rate limit) rather than one for all non-2xx.

### SQL queries batched; no N+1 per-row fetches
When iterating over a result set, never issue a per-row query inside the loop. Batch into `IN (...)` with `GROUP BY` or use a CTE/subquery.

### CI checks for .gitignore coverage
Scan for files matching sensitive patterns (`*.json` weight files, `reports/`, `*.tmp`) and fail if they are not gitignored.

### Dead code as a code smell signal
Enforce `ruff` or `flake8` with `F401` (unused imports) and periodic audits of module-level constants. A constant defined but never read indicates a feature not completed or a refactor not fully propagated.

## Quick Review Checklist

Use this when reviewing analytics pipeline changes:

1. Any data-writing function is idempotent (dedup before write)
2. JOINs against time-series tables use `ORDER BY ... LIMIT 1` subqueries
3. `INSERT OR REPLACE` is never used -- prefer `ON CONFLICT DO UPDATE`
4. Exception classes match HTTP semantics (auth vs. client vs. transient)
5. No per-row queries inside loops -- batch with `IN (...)`
6. Phase boundaries are explicit (Phase N does not assume Phase N-1's row shape)
7. Delta/fallback queries match on row identity, not time strings
8. Sensitive output files are in `.gitignore`
9. No dead constants, imports, or unused parameters
10. API keys are never captured at module level -- use deferred accessors
