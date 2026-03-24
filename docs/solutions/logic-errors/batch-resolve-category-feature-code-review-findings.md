---
title: "Resolve 11 PR review TODOs: eliminate conflicting category mappings, duplicate queries, double LLM calls, and harden input validation across discovery pipeline"
category: logic-errors
date: 2026-03-24
tags:
  - code-review
  - single-source-of-truth
  - database-indexes
  - query-deduplication
  - llm-optimization
  - input-validation
  - pagination
  - exception-handling
  - flask
  - python
  - refactoring
severity: P1
component:
  - config.py
  - pipeline/db.py
  - pipeline/hashtags.py
  - pipeline/extractor.py
  - dashboard.py
  - templates/dashboard.html
problem_type:
  - conflicting-data-definitions
  - missing-indexes
  - duplicated-logic
  - redundant-api-calls
  - per-request-initialization
  - missing-input-validation
  - hardcoded-magic-strings
  - unbounded-query-results
  - overly-broad-exception-handling
related_docs:
  - docs/plans/2026-03-24-001-feat-category-based-search-filter-plan.md
  - docs/plans/2026-03-23-001-feat-atlasi-place-discovery-pipeline-plan.md
---

# Batch Resolve: Category Feature Code Review Findings

## Problem

PR #2 added category-based search filtering to the Atlasi social media place discovery pipeline. A multi-agent code review (Python, architecture, simplicity, performance, and security reviewers) surfaced 11 TODOs spanning data structure inconsistency, missing DB indexes, query duplication, redundant LLM calls, per-request initialization, missing validation, and overly broad exception handling.

The highest-priority issue: `CATEGORIES["food_and_drink"]["types"]` included "bar" and "market", but `TYPE_TO_CATEGORY["bar"]` mapped to "nightlife" and `TYPE_TO_CATEGORY["market"]` mapped to "shopping" -- two data structures telling conflicting stories about where these types belong.

## Root Cause

The category feature was layered on top of existing code without consolidating what already existed. The core pattern: **additive development without refactoring**, producing duplicate sources of truth, copy-pasted query branches, and unguarded new input paths.

Specific manifestations:

- **Two manually-maintained dicts** (`CATEGORIES` and `TYPE_TO_CATEGORY`) that contradicted each other
- **Full if/else duplication** of COUNT and SELECT SQL queries (with-category vs. without-category)
- **Two LLM calls** where one sufficed -- a category-aware prompt followed by a redundant generic prompt
- **No validation** on the new `category` and `page` query parameters
- **Bare `except OperationalError`** swallowing schema-migration errors
- **`init_db()` called per-request** instead of once at startup

## Solution

Batch-resolve all 11 issues in a single pass, guided by three principles: **derive rather than duplicate**, **parameterize rather than branch**, **validate at the boundary**.

### 1. Single source of truth for type mappings (`config.py`)

Derived `TYPE_TO_CATEGORY` from `CATEGORIES` instead of maintaining both by hand. Removed ~35 LOC. Fixed "bar" -> nightlife only, "market" -> shopping only.

```python
TYPE_TO_CATEGORY = {
    place_type: cat_key
    for cat_key, cat_val in CATEGORIES.items()
    for place_type in cat_val["types"]
}
VALID_PLACE_TYPES = frozenset(TYPE_TO_CATEGORY.keys()) | {"other"}
```

### 2. Dynamic WHERE clause (`pipeline/db.py`)

Replaced duplicated if/else query blocks with parameterized clause building:

```python
where = "WHERE city_id = ?"
params: list = [city_id]
if category:
    where += " AND category = ?"
    params.append(category)
total = conn.execute(
    f"SELECT COUNT(*) as cnt FROM places {where}", params,
).fetchone()["cnt"]
```

### 3. COALESCE for optional updates (`pipeline/db.py`)

Eliminated an if/else branch around UPDATE by letting SQLite handle the "category may be None" case:

```python
conn.execute(
    "UPDATE places SET mention_count = mention_count + 1, category = COALESCE(?, category) WHERE id = ?",
    (category, place_id),
)
```

### 4. Single LLM call (`pipeline/hashtags.py`)

Increased category prompt from 12 to 15 tags, removed redundant second generic LLM call. Halves latency and API cost.

### 5. App startup init (`dashboard.py`)

Moved `init_db()` from per-request to module-level startup block.

### 6. Input validation (`dashboard.py`)

```python
page = max(1, request.args.get("page", 1, type=int))
if category_filter and category_filter not in VALID_CATEGORIES:
    category_filter = ""
```

### 7. Composite database indexes (`pipeline/db.py`)

```sql
CREATE INDEX IF NOT EXISTS idx_places_city_category_score
    ON places(city_id, category, virality_score DESC);
CREATE INDEX IF NOT EXISTS idx_hashtags_city_status_category
    ON hashtags(city_id, scrape_status, category);
```

### 8. Narrowed exception handling (`pipeline/db.py`)

```python
except sqlite3.OperationalError as e:
    if "duplicate column name" not in str(e):
        raise
```

### 9. Additional fixes

- **URL encoding**: Added `|urlencode` to all string query params in pagination links
- **DEFAULT_CATEGORY constant**: Extracted hardcoded `"sights_and_attractions"` fallback
- **Type hint fix**: `sample_caption: str | None = None`
- **Module-level import**: Moved `import re` out of function body
- **Paginated API**: `/api/places` now accepts `page`, `per_page`, `category` with per_page capped at 500

## Prevention Strategies

### Derive, never duplicate

When two data structures must agree, one must be computed from the other. The test: if I change a mapping in one place, does the system break silently elsewhere?

### Index-alongside-column rule

Every new column should include an explicit decision about indexing in the same migration. If the column appears in WHERE, ORDER BY, or JOIN clauses, add the index.

### Parameterize, don't branch-and-copy

If two if/else branches differ only in a query filter, extract the varying part into a variable and write the query once. The test: if you'd have to apply a bugfix to both branches, the code is duplicated.

### Validate at the boundary

Every user-facing query parameter must be validated before reaching business logic. Use schema validation libraries (`pydantic`, `webargs`) rather than hand-rolled checks.

### Default to paginated

No endpoint should return an unbounded result set. Enforce pagination with a sensible max page size.

### Catch specific exceptions

Never `except SomeBaseError` -- catch the narrowest exception that represents the actual failure mode.

### Batch external API calls

Before adding a new external API call, verify that no existing call already produces the needed data.

## Quick Review Checklist

| # | Check |
|---|-------|
| 1 | No parallel data structures encoding the same knowledge |
| 2 | New columns have explicit index decisions |
| 3 | All query parameters validated |
| 4 | All list endpoints paginated with server-side cap |
| 5 | No branch-and-copy duplication in query building |
| 6 | Per-request scope contains no startup-level initialization |
| 7 | Imports at top, type hints accurate |
| 8 | External API calls batched; no redundant calls |
| 9 | Template variables context-encoded for URL/HTML/JS |
| 10 | Exception handlers catch narrowest applicable type |
| 11 | String literals with domain meaning are named constants |

## Files Changed

- `config.py` -- Derived TYPE_TO_CATEGORY/VALID_PLACE_TYPES, added DEFAULT_CATEGORY
- `pipeline/db.py` -- Indexes, COALESCE, dynamic WHERE, type hint, import re, narrowed exception
- `pipeline/hashtags.py` -- Single LLM call, removed GENERIC_PROMPT_TEMPLATE
- `pipeline/extractor.py` -- DEFAULT_CATEGORY constant reference
- `dashboard.py` -- init_db at startup, input validation, paginated API
- `templates/dashboard.html` -- URL encoding for pagination links
- `tests/test_category.py` -- Updated to expect single LLM call

## Commit

`3493020` on `feat/category-based-search-filter` branch.
