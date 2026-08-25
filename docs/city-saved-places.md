---
title: "City → saved places: can Social Scanner feed Atlasi city guides?"
type: investigation
status: draft
date: 2026-08-25
---

# City-saved places for Atlasi

**Question:** Can this repo already find popular *saved* places in a given city (e.g. Tokyo) from TikTok, and what should we change so the output can feed Atlasi's city-guide recommendation pipeline?

**Short answer:** The pipeline can already turn a city name into a ranked list of *named places mentioned on TikTok*. It does capture TikTok saves (`collectCount`). It does **not** yet answer "popular saved places in this city." Ranking is a views-normalized engagement *rate*, city scoping is hashtag-only, and the export is a thin CSV that omits save totals, neighborhoods, and source URLs.

No live scrape was run for this note (needs Apify + OpenRouter keys). Assessment is from code, tests, and existing plans.

---

## 1. Can we get popular saved Tokyo places today?

**With API keys, yes — a list of places.** Run:

```bash
python discover.py --city "Tokyo" --export-csv
# optional: --category food_and_drink --max-posts 30 --skip-scrape
```

That path is implemented end-to-end:

1. LLM (+ hardcoded) city-prefixed hashtags → `hashtags`
2. Apify `clockworks/free-tiktok-scraper` → `raw_posts` including `saves`
3. Optional Gemini OCR of cover images, appended onto captions
4. LLM extracts place names → `places` + `place_posts`
5. Fuzzy dedup + virality score
6. LLM tourist-trap flag
7. Stdout top-20 + optional `{city}_places.csv`

**As "popular saved places in Tokyo," no.** Three gaps make the current list the wrong product:

| Requirement | Today |
|---|---|
| Places people *save* (TikTok collect) | Saves are stored and 5×-weighted, then **divided by views**. High-save, high-view videos lose to tiny high-engagement-rate videos. No min-saves filter. Export does not include save totals. |
| *In that city* | Scope is `{city}…` hashtags plus caption text. No lat/lng, no `locationCreated` filter, no geocode. LLM `in_city` exists but runs only during slideshow generation. |
| Repeating / consensus spots | `mention_count` and `log(posts+1)` exist, but a single high-ER post can outrank a place mentioned across many saved videos. |

There is no local `places.db` in the repo, so Tokyo results are not sitting on disk waiting to be exported.

---

## 2. What works, what's stubbed, what's broken

### Works (implemented and wired into `discover.py`)

- **TikTok scrape.** `pipeline/scraper.py` maps `collectCount` → `raw_posts.saves`, plus likes/comments/shares/views. Engagement floor: `MIN_VIEWS_TIKTOK=1000`, `MIN_LIKES_TIKTOK=50`. Location tags from `locationMeta` are appended to the caption as `📍 Location tag: …`.
- **Place extraction.** `pipeline/extractor.py` pulls named venues from captions, location tags, and OCR text. Skips the city name itself. Assigns type + category.
- **Dedup.** RapidFuzz `token_sort_ratio` (cutoff 85) + containment, then LLM confirmation before merge. Relative threshold avoids `Kat`/`Bar` false merges.
- **Scoring uses saves.** `pipeline/scorer.py` `_score_places()` really does `saves * 5 + shares * 4 + comments * 2 + likes * 1`, then `÷ views`. Tests lock this in (`tests/test_scorer.py`).
- **Tourist-trap filter.** Batched LLM classification; CSV/summary exclude traps.
- **Resume / cost controls.** Hashtag `scrape_status`, `INSERT OR IGNORE` posts, `--skip-scrape`, `--retry-failed`, `--reset`. TikTok is batched into one Apify run per city. `resultsPerPage` capped at 30 (`_TIKTOK_MAX_PER_HASHTAG`).
- **Downstream consumers.** Flask dashboard + `GET /api/places`. Slideshow generator (`generate_slideshow.py`) picks non-trap, non-hidden places by `virality_score`.

### Implemented but not on the discover path (stubbed for this use case)

| Piece | Where it lives | Why it doesn't help a city-guide export today |
|---|---|---|
| Instagram scraper | `scraper._map_instagram` | `insert_hashtags(..., platforms=("tiktok",))` since commit `c012391` (cost cut). IG `saves` and `shares` are hardcoded `0`. |
| LLM `in_city` hide | `pipeline/enrichment.py` | Only called from `generate_slideshow.py` after place *selection*. `discover.py` never enriches. Places that are not in the city stay in CSV/summary. |
| Google Places | `pipeline/photo_search.py` | Text-search + photo download for slides. Not used to confirm the venue is in the requested city or to attach a place_id / lat/lng. |
| Structured location | `locationMeta` | Name/address concatenated into caption only. City, country, and coordinates are discarded. |
| Visual OCR | `pipeline/ocr.py` | Wired in discover as step 2.5, but **one batch of 20 covers, no loop**. A typical scrape will OCR a sliver of posts. |
| City validation | `discover.validate_city` | `len(name) >= 2`. The original plan's "LLM check before burning Apify credits" was never built. |
| JSON export | — | CSV only (`rank, name, type, category, mention_count, virality_score`). Dashboard `/api/places` is paginated JSON of full place rows but still has no aggregated saves. |
| Score normalization | plan `2026-03-23-001` | Percentile rank 0–1 per city was recommended so scores are comparable. Never implemented. Raw rates are typically `≪ 1`. Slideshow virality bands (`0-25`, `25-50`, …) therefore put **every** place in the first band. |

### Broken, stale, or actively misleading

1. **Tests still assume Instagram.** `tests/test_pipeline.py::test_insert_hashtags_creates_both_platforms` and `tests/test_category.py` expect 2 platforms per tag. Default is TikTok-only. README still says "TikTok + Instagram" and prices a 2-platform run.
2. **`collectCount` has no `stats` fallback.** Likes/views/shares fall back to `item["stats"]`. Saves do not (`item.get("collectCount", 0)` only). If Clockworks puts collects only under `stats`, every TikTok save is stored as `0` and the 5× weight is a no-op.
3. **Hashtag attribution is wrong after batching.** One Apify call is sent for *all* pending tags; every returned post is then linked to *every* hashtag in the batch (`scraper.py` after `insert_post`). Fine for "posts about this city," useless for "which tag produced this place."
4. **Zero-view score explosion.** `views = max(row["views"], 1)`. A post with missing `playCount` and 50 likes scores like a 5000% engagement rate. Instagram photos (views=0) would have done the same — another reason IG was disabled.
5. **CSV / summary ignore `hidden`.** `get_all_places()` returns hidden rows. Enrichment can hide out-of-city places, but discover never hides them; if someone later enriches and re-exports, hidden places still appear unless they go through the dashboard filter.
6. **Universal hashtag comment vs code.** Comments/docs say 5 hardcoded tags; `_universal_hashtags()` emits 4 (`hiddengems`, `locals`, `secretspots`, `underrated`). Spec's `{city}foodie` was dropped.

Nothing in the 5-step discover path is an empty stub. The product gap is **ranking definition + geo + export shape**, not a missing module.

---

## 3. Is city scoping robust?

**No. It is hashtag-scoped, not geo-validated.**

How a city is applied today:

```
city string
  → LLM must emit tags that *start with* the city slug
  → hardcoded `{city}hiddengems` etc.
  → Apify hashtag search (no lat/lng, no country, no `locationCreated`)
  → extractor prompt says "captions about {city_name}"
  → tourist-trap prompt is city-aware
  → (slideshow only) enrichment asks `in_city` and may set `hidden=TRUE`
```

What that misses:

- Travel dump accounts tag `#tokyohiddengems` on a Seoul or Dubai reel.
- Chain names (`Sushi Dai`, `Blue Bottle`, `Starbucks Reserve`) resolve to the wrong city.
- Places *named after* the city (`Kochi Restaurant` in Düsseldorf) — enrichment's own example — stay in the discover output.
- Istanbul/Tokyo/Paris hashtags are especially leaky because they are global travel brands.

`locationMeta` is the best unused signal on TikTok. The scraper already reads `locationName` / `address` for the caption and then throws away the rest. Clockworks also often returns `locationCreated` (country). Neither is stored, filtered, or scored.

Google Places text search (`"{name}, {city}"`) is the most reliable cheap validator we already have a client for — but it is only used to fetch a photo.

**Practical risk:** a Tokyo run can and will include non-Tokyo venues. Treat today's list as "places mentioned under Tokyo-ish TikTok hashtags," not "places in Tokyo."

---

## 4. Does ranking actually use saves, or is it views-heavy?

**Saves are in the formula. Views still dominate the outcome.**

```python
engagement = saves*5 + shares*4 + comments*2 + likes*1
rate       = engagement / max(views, 1)
score      = sum(rate) * log(post_count + 1)
```

Worked examples (one post each):

| Place | Saves | Likes | Views | Score (approx) | What a human thinks |
|---|---|---|---|---|---|
| A. Niche save-bait | 200 | 0 | 1,000 | 0.69 | "People bookmark this" |
| B. Mega-viral, heavily saved | 10,000 | 0 | 2,000,000 | 0.017 | "Everyone saved this Tokyo spot" — **ranks near the bottom** |
| C. Low-view like-farm | 0 | 100 | 100 | 0.69 | Ties A with **zero saves** |
| D. Same likes, normal views | 0 | 100 | 10,000 | 0.007 | Views bury it |

So:

- The 5× save weight only wins **within the same view bucket**.
- Dividing by views is an *engagement-rate* ranker (good for "this clip punched above its weight"), not a *save-volume* ranker (what a city guide wants: "travelers keep bookmarking this place").
- The ingest filter is views/likes, not saves. Low-save viral sightseeing clips enter; high-save, sub-1k-view clips are dropped.
- Instagram, if re-enabled, has `saves=0` and often `views=0` → it would systematically distort the same ranking (already called out in `docs/plans/2026-03-23-001-feat-atlasi-place-discovery-pipeline-plan.md`).
- Repeating mentions help via `log(posts+1)`, which grows slowly (2 posts → 1.10, 10 posts → 2.40). A single high-ER post can still beat a consensus spot.

**Callout:** capturing `collectCount` is not the same as ranking by saves. If the actor ever stops returning top-level `collectCount`, ranking silently becomes likes+shares only.

---

## 5. Intended design vs what shipped

From `docs/initial-spec` and `docs/plans/2026-03-23-001-…`:

- Original product: trending **non-obvious** places to feed **TikTok slideshows**, not a city-guide catalog.
- Saves were weighted as an *intent* signal inside a virality score, not as the primary sort.
- Instagram was in the spec; it was later dropped to save Apify credits.
- Geo was explicitly out of slideshow scope ("NOT adding coordinates/geocoding… neighborhood is inferred by LLM").
- Percentile normalization and LLM city validation were recommended and not built.

That history explains the gaps. The repo did its original job. Atlasi city guides need a different ranking contract and a harder city gate.

---

## 6. Extension plan: city in → repeating saved places → ranked list → export

Reuse the existing pipeline. Do not rebuild scraping or extraction. Change **what we persist, how we filter, how we rank, and what we export.**

```
City
  → existing hashtag + TikTok scrape (keep)
  → persist structured location + per-post saves (new)
  → existing extract + dedup (keep)
  → geo gate: location fields → Google Places → LLM in_city (new, in discover)
  → consensus + save-first rank (new)
  → JSON / CSV city-guide export (new)
```

### Phase 0 — Honest export of what we already have (no new scrape)

Add a read-only exporter over an existing `places.db` so we can inspect a past Tokyo/Istanbul run without spending credits.

```
python discover.py --city Tokyo --skip-scrape --export-json tokyo_places.json
```

JSON (and a richer CSV) per place:

- identity: `name`, `type`, `category`, `neighborhood`, `is_tourist_trap`, `hidden`
- consensus: `mention_count`, `post_count`
- engagement **totals** (not just the rate): `total_saves`, `total_shares`, `total_likes`, `total_views`
- current `virality_score` (keep, labeled `engagement_rate_score`)
- sample caption + top source URLs (already in `get_place_source_posts`)

This is enough to see whether saves are actually populated and how often names leak across cities.

### Phase 1 — Make saves and city signals real

1. **Map `collectCount` from `stats` too**, same pattern as likes/views. Add a scraper test with stats-only collects.
2. **Persist structured TikTok location** on `raw_posts` (nullable): `location_name`, `location_address`, `location_city`, `location_country`, `location_created`. Still append the caption line so extraction keeps working.
3. **Loop OCR** until no unprocessed covers remain (or cap with `--max-ocr`), so on-screen place names are not limited to 20 posts.
4. **Run `enrich_places()` at the end of discover**, not only in slideshows, so `in_city` hiding applies to exports. Default `in_city` should be **false when uncertain** for city-guide mode (today it defaults true).
5. **Fix stale tests/README** (TikTok-only default). Do not turn Instagram back on for a save-based ranker.

### Phase 2 — City-guide ranker (the actual product change)

New score, stored beside virality (do not overwrite; slideshows still use the old one):

```
eligible =
    hidden = 0
    AND is_tourist_trap = 0
    AND mention_count >= 2          # repeating
    AND total_saves >= MIN_SAVES    # e.g. 50
    AND in_city_confidence >= threshold

guide_score =
    log1p(total_saves) * 1.0
  + log1p(mention_count) * 0.6
  + save_rate * 0.3                 # total_saves / max(total_views, 1)
```

Why this shape:

- **Volume of saves** is the primary sort (the thing travelers did).
- **Repeating mentions** are the consensus filter Atlasi wants ("discover repeating saved places").
- **Save rate** is a tie-breaker, not the leader — so mega-viral Tokyo spots are not buried.
- Keep tourist-trap + hidden as hard filters, not score terms.

CLI:

```
python discover.py --city Tokyo --mode city-guide --export-json --export-csv
```

`--mode city-guide` means: TikTok only, run enrichment, apply the geo gate, rank by `guide_score`, exclude singletons.

### Phase 3 — Geo gate (stop Dubai-in-Istanbul)

Apply in order, cheapest first:

1. **Structured tag match.** If `location_city` / `location_created` is present and clearly another city/country, reject or down-rank. Keep when it matches or is empty (most TikToks have no tag).
2. **Google Places text search** `"{place_name}, {city}"` (client already in `photo_search.py`). Accept if the returned formatted address contains the city or a known alias (Tokyo / 東京 / Tōkyō). Store `google_place_id`, lat, lng. Reject if the top result is in another admin area with high confidence.
3. **LLM `in_city`** only when (1) and (2) are empty or disagree. This is the current enrichment check, moved into discover.

Do **not** treat "hashtag contains tokyo" as proof.

Aliases matter: Tokyo → 東京, Edo-era neighborhood names; Istanbul → İstanbul, Constantinople in old captions. Keep a small alias table per city rather than another LLM call.

### Phase 4 — Export contract for Atlasi

`{city}-saved-places.json`:

```json
{
  "city": "Tokyo",
  "generated_at": "…",
  "mode": "city-guide",
  "source": "tiktok",
  "places": [
    {
      "rank": 1,
      "name": "…",
      "type": "ramen shop",
      "category": "food_and_drink",
      "neighborhood": "Ebisu",
      "google_place_id": "…",
      "lat": 35.64,
      "lng": 139.71,
      "mention_count": 7,
      "total_saves": 18420,
      "total_views": 2100000,
      "guide_score": 12.4,
      "engagement_rate_score": 0.031,
      "sample_caption": "…",
      "source_urls": ["https://www.tiktok.com/@…/video/…"]
    }
  ]
}
```

CSV is the same columns, one row per place. Dashboard `/api/places` can grow `total_saves` + `guide_score` without a new service.

### Out of scope (do not do in the first cut)

- Re-enabling Instagram for this ranker (no save field).
- Scraping TikTok "saved" collections / following users (different actor, ToS, not what `collectCount` is).
- Full geocoding of every caption without a named venue.
- Replacing the slideshow virality score — keep both columns.
- Live Apify runs as part of CI.

---

## 7. Recommended next steps (implementation order)

1. **Exporter only** (Phase 0) on whatever city DB you already have. Confirm whether `raw_posts.saves` is non-zero in production data. If saves are all zero, fix the `collectCount` mapping before anything else.
2. **Scraper robustness:** `stats.collectCount` fallback + persist `locationMeta` / `locationCreated`.
3. **Wire enrichment into discover** and default-hide `in_city=false` in `--mode city-guide`.
4. **Add `guide_score` + eligibility filters**; leave `virality_score` untouched.
5. **Google Places city check** using the existing client; store `google_place_id` + coords.
6. **Ship JSON/CSV contract** above; point Atlasi's guide pipeline at the file.

Success for a Tokyo dry run (after keys + one scrape):

- ≥ 2 distinct source posts for each exported place
- `total_saves > 0` on the majority of the top 20
- Spot-check: no obvious non-Japan venues in the top 20
- Slideshow generation still reads `virality_score` and is unchanged

---

## 8. File map

| File | Role for city-saved-places |
|---|---|
| `discover.py` | CLI; add `--mode`, JSON export; call enrichment |
| `pipeline/scraper.py` | Saves mapping + structured location |
| `pipeline/scorer.py` | Keep virality; add `guide_score` |
| `pipeline/extractor.py` | Reuse as-is |
| `pipeline/enrichment.py` | Move `in_city` into discover; flip uncertain default |
| `pipeline/hashtags.py` | Reuse; TikTok-only is correct for this product |
| `pipeline/ocr.py` | Loop / raise cap |
| `pipeline/photo_search.py` | Extend from photo-only to city validation |
| `pipeline/db.py` | Location columns, save aggregates, `guide_score` |
| `pipeline/filter.py` | Reuse tourist-trap as a hard filter |
| `dashboard.py` | Optional: expose new fields on `/api/places` |
