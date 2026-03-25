---
title: Batch resolve 16 code review findings across slideshow generation pipeline
category: logic-errors
date: 2026-03-24
severity: critical
tags:
  - security
  - correctness
  - performance
  - code-quality
  - dead-code
  - path-traversal
  - memory-leak
  - database-atomicity
  - prompt-injection
  - image-handling
  - retry-logic
  - pillow
components:
  - generate_slideshow.py
  - pipeline/posting.py
  - pipeline/image_gen.py
  - pipeline/overlay.py
  - pipeline/db.py
  - pipeline/hooks.py
  - pipeline/enrichment.py
  - pipeline/llm.py
  - pipeline/slideshow_types.py
problem_type: code_review_batch
status: resolved
commit: 92c035e
pr: 3
---

# Batch Resolve: 16 Code Review Findings Across Slideshow Pipeline

## Overview

16 findings from an automated multi-agent code review (PR #3) resolved in a single commit. The findings spanned security, correctness, performance, and code quality across the entire slideshow generation pipeline. Net result: -223 lines (376 added, 599 deleted), 259 tests passing.

## Security Fixes

### 1. Path Traversal via City Name (P1)

**Problem:** City names from the database (scraped from social media) were sanitized with only `.lower().replace(" ", "-")`, preserving `/` and `.` characters. A name like `../../etc/cron.d` could escape the output directory via `mkdir(parents=True)`.

**Root cause:** Insufficient input sanitization at the boundary where external data enters filesystem operations.

**Fix:** Two-layer defense:
```python
city_slug = re.sub(r"[^a-z0-9-]", "", city_name.lower().replace(" ", "-"))
# ...
if not output_dir.resolve().is_relative_to(SLIDESHOW_OUTPUT_DIR.resolve()):
    print("Error: Invalid city name produces unsafe path", file=sys.stderr)
    sys.exit(1)
```

### 2. Unvalidated Base64 from API (P1)

**Problem:** Image generation API responses were base64-decoded and written directly to disk with no size limit, format check, or integrity validation. A compromised API could write arbitrary data.

**Fix:** Size cap + PNG magic byte validation before write:
```python
MAX_IMAGE_SIZE = 50 * 1024 * 1024  # 50 MB
if len(image_bytes) > MAX_IMAGE_SIZE:
    raise GeminiError(f"Decoded image too large: {len(image_bytes)} bytes")
if not image_bytes[:8] == b'\x89PNG\r\n\x1a\n':
    raise GeminiError("Decoded data is not a valid PNG image")
```

### 3. Prompt Injection via Place Names (P2)

**Problem:** Place names, types, and categories from scraped social media were interpolated raw into LLM prompts. Only `sample_caption` had `sanitize_text()` applied.

**Fix:** Applied `sanitize_text()` with length limits to all DB-derived strings before prompt insertion in both `enrichment.py` and `hooks.py`.

## Correctness Fixes

### 4. Posting Glob Uploads Duplicates (P1)

**Problem:** `output_dir.glob("slide_*.png")` matched ALL files starting with `slide_` — including `slide_1_raw.png`, `slide_1_hook_raw.png`, and `slide_1.png`. For a 10-slide slideshow, ~30 files were uploaded to TikTok instead of 10.

**Root cause:** Glob pattern too broad; no distinction between intermediate and final output files.

**Fix:** Regex filter to match only final overlay outputs:
```python
slide_files = sorted(
    (p for p in output_dir.glob("slide_*.png")
     if re.fullmatch(r"slide_\d+\.png", p.name)),
    key=lambda p: int(re.search(r"slide_(\d+)", p.stem).group(1)),
)
```

### 5. Database Atomicity Broken (P2)

**Problem:** `create_slideshow()` used `with conn:` (auto-commit), then each `add_slideshow_place()` called `conn.commit()` individually. A crash mid-loop left orphaned slideshow rows with partial place associations.

**Fix:** Removed commits from both helpers. The caller performs a single `conn.commit()` after all operations succeed, making the entire slideshow creation + place linking atomic.

### 6. Regex Crash in Posting Sort (P2)

**Problem:** `re.search(r"slide_(\d+)", p.stem).group(1)` without a None check. Resolved automatically by fix #4's tighter glob pattern.

### 7. Mutable Type Discriminator (P2)

**Problem:** `HookSlideText(type="location")` silently created an object with the wrong type discriminator, causing overlay dispatch to apply the wrong function.

**Fix:** `__post_init__` validation on each dataclass:
```python
def __post_init__(self) -> None:
    if self.type != "hook":
        raise ValueError(f"HookSlideText.type must be 'hook', got {self.type!r}")
```

## Performance Fixes

### 8. Pillow Image Handles Never Closed (P1)

**Problem:** `Image.open()` + `.copy()` per slide, never closed. Two uncompressed RGBA images per slide at 1152x2048 = ~19MB per slide. 17 slides = ~320MB of uncollected memory.

**Fix:** Context manager + explicit close:
```python
with Image.open(raw_path) as image:
    result = overlay_fn(image, slide_text)
    result.save(out_path, format="PNG")
    result.close()
```

### 9. Font Loaded from Disk ~40 Times (P2)

**Problem:** `load_font()` called per-slide, up to 3 times per location slide. Since font sizes are constant across slides, all calls are redundant after the first.

**Fix:** `@functools.lru_cache(maxsize=16)` on `load_font()`.

## Code Quality Fixes

### 10. `format` Shadows Python Builtin (P2)

**Problem:** `format` used as parameter name in function signatures across 6 files.

**Fix:** Renamed to `hook_format` in Python code. CLI `--format` flag preserved via `dest="hook_format"`. DB column `format` unchanged.

### 11. Duplicated Retry Logic (P3)

**Problem:** Same retry-with-exponential-backoff pattern copy-pasted across `llm.py`, `image_gen.py`, and `posting.py` (~80 lines of duplication).

**Fix:** Extracted `pipeline/retry.py` with shared helper:
```python
def retry_with_backoff(fn, max_retries, base_delay, *, non_retryable=()):
```
Each callsite shrunk from ~30 lines to ~5.

### 12. Duplicated JSON Parsing (P3)

**Problem:** `enrichment.py` called `call_llm()` then re-implemented markdown fence stripping and JSON extraction. This logic already existed in `call_llm_json()`.

**Fix:** Replaced `call_llm` + `_parse_enrichment_response` with `call_llm_json` directly.

### 13. Dead Code Removal (P3)

Removed ~130 lines: `_handle_response` (posting), `validate_slides` (types), `from_meta_json` (types), `_apply_override` (image_gen), unused `places` parameter (hooks).

### 14. Broken Test Method (P2)

`get_flattened_data()` was flagged as non-existent in Pillow but is actually the newer API (replacing deprecated `getdata()`). Verified it works with current Pillow 12.1.1.

### 15. Misc Quality (P3)

Fixed `import re` placement in db.py, `logger` vs `log` naming inconsistency in overlay.py, removed duplicate `sys.path.insert` from 3 test files.

## Deferred

**Untested error paths (P2, TODO 010):** Error handling branches in `main()` (PostingAuthError, PostingError, CreditsExhaustedError, GeminiQuotaError) and API key guard clauses lack test coverage. Deferred as it requires 1-2 hours of new test development.

## Prevention Strategies

### File Matching
When filenames share prefixes but have different suffixes (e.g., `slide_1.png` vs `slide_1_raw.png`), always use `re.fullmatch()` instead of glob alone. Glob patterns cannot exclude substrings.

### Transaction Boundaries
Database helper functions should NOT commit. Let callers control transaction scope so related operations are atomic. Use a single `conn.commit()` at the caller level.

### Resource Cleanup
Always use `with` statements for `Image.open()`, file handles, and database connections. If a resource has `.close()`, it needs a context manager or try/finally.

### API Response Validation
Validate external API responses before acting on them: check status, structure, size, and format. Never write decoded binary data to disk without at least magic byte validation.

### Input Sanitization for LLM Prompts
All database-derived strings that originated from external sources (scraped content, user input) must be sanitized before LLM prompt interpolation. Apply `sanitize_text()` with length limits.

### Avoid Builtin Shadowing
Never use Python builtin names (`format`, `type`, `id`, `input`, `list`) as function parameters. Use descriptive alternatives (e.g., `hook_format`).

### DRY Threshold
When the same pattern appears in 3+ files, extract it. The retry logic duplication was a textbook signal.

## Related Documentation

- [Batch resolve category feature code review findings](batch-resolve-category-feature-code-review-findings.md) — Previous batch resolution following the same review process
