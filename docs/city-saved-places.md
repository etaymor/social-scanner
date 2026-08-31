# City-level TikTok saved places

Investigation of whether Social Scanner can already produce a ranked list of
places people actually save from TikTok in a given city (Tokyo first), and how
to extend this repo for a later Atlasi city-guide pipeline.

This note is research only. It does not implement the guide page, the
slideshow-as-guide, or any live scrape.

---

## Verdict

**Partial — not as stated.**

This repo can already take a city name, scrape TikTok posts under
city-prefixed hashtags (via an unofficial Apify actor), extract named places
from captions / location tags / some cover OCR, and rank those places by a
**virality score**. That score *weights* TikTok `collectCount` (video saves)
when the field is present.

It cannot already produce an honest ranked list of **popular saved places in
Tokyo**. It does not read anyone's TikTok saved/bookmark collection. It does
not geo-filter posts to a city boundary. Instagram save counts are hardcoded
to zero, and Instagram hashtags are not even inserted on the current default
path. Ranking is engagement-rate virality, not "this place keeps repeating as
a save."

Closest honest description of what works today:

> Places that keep showing up in high-engagement TikTok posts found via
> Tokyo-related hashtags, with video save-count as one weight in an
> engagement-rate score — if the unofficial scraper actually returns
> `collectCount`.

That is a useful discovery signal. It is not a "most-saved places" ranking.

---

## What this repo actually is

Two products share one SQLite file (`places.db`):

1. **Place discovery** (`discover.py` + `pipeline/{hashtags,scraper,ocr,extractor,scorer,filter}.py`)
   City in → hashtags → Apify scrape → LLM place names → fuzzy dedup →
   virality score → tourist-trap flag → optional CSV / dashboard.
2. **Acquisition slideshows** (`generate_slideshow.py` + analytics /
   intelligence / Postiz / RevenueCat)
   Takes *already discovered* places and generates Atlasi promo slideshows
   (hook + place slides + last-slide CTA). This is not a city guide.

The original spec (`docs/initial-spec`) and the completed plan
(`docs/plans/2026-03-23-001-feat-atlasi-place-discovery-pipeline-plan.md`)
are explicit: the pipeline was built to feed TikTok slideshow *content* for
Atlasi, not to publish a shareable city-guide page.

Tokyo is already listed in `docs/trending_destinations_2026.csv`. That CSV is
a destination list, not a geo index and not wired into `discover.py`.

---

## What works today

| Capability | Status | Notes |
|---|---|---|
| City as a run key | Works | `--city Tokyo` creates a `cities` row and scopes hashtags / posts / places by `city_id`. |
| Hashtag generation | Works | LLM + hardcoded `{city}hiddengems` etc. Category seeds exist (`--category food_and_drink`). Prompts require tags to start with the city name. |
| TikTok scrape | Works (paid, unofficial) | `clockworks/free-tiktok-scraper` via Apify (`config.TIKTOK_ACTOR`). Needs `APIFY_API_TOKEN`. Uses `build='latest'`. |
| Caption + location-tag text | Works | `_map_tiktok` appends `locationMeta.locationName` / address to the caption as `📍 Location tag:`. |
| Cover OCR | Partial | `pipeline/ocr.py` appends on-screen text. `discover.py` calls it **once** with default `LIMIT 20`. Most posts never get OCR. |
| LLM place extraction | Works | Batches of 20 captions. Needs `OPENROUTER_API_KEY`. |
| Mention counting | Works | `places.mention_count` + `place_posts` junction. This is the closest existing "keeps repeating" signal. |
| Dedup | Works | rapidfuzz + LLM confirmation. Identity is `(city_id, name)`. |
| Virality score | Works as specified | `saves*5 + shares*4 + comments*2 + likes*1`, divided by views, times `log(post_count+1)`. |
| CSV / dashboard / JSON API | Works | `--export-csv`, `dashboard.py`, `GET /api/places`. Sorted by `virality_score`. |
| Soft city check | Slideshow-only | `pipeline/enrichment.py` asks the LLM `in_city` and hides mismatches. Runs from `generate_slideshow.py`, **not** from `discover.py`. |
| Google Places | Photos only | `pipeline/photo_search.py` resolves a Place ID to fetch a photo for slides. It does **not** persist id / lat / lng / city on `places`. |

### Sources

- **TikTok:** unofficial community Apify actor. No official TikTok API, no
  Research API, no login-cookie scraper in this repo.
- **Instagram:** actor + mapper exist (`apify/instagram-hashtag-scraper`).
  Not invoked on the current default path (see below).
- **User saved collections:** not implemented on either platform.

### City / geo filter

There is no lat/lng, bounding box, or Places-API city membership on the
discovery path. "Tokyo" is:

1. a string used to generate hashtags (`tokyofoodie`, …)
2. a foreign key on rows written during that run
3. later, an LLM yes/no for slideshow enrichment

A post tagged `#tokyofood` that is actually about Osaka, or a place named
after Tokyo that lives in another city, can land in the Tokyo bucket until
(and unless) slideshow enrichment hides it.

### Save / bookmark signals

`raw_posts.saves` stores TikTok **video** `collectCount` — how many times
that video was saved. Instagram `saves` and `shares` are hardcoded `0`.

The pipeline never:

- opens a user's Saved / Favorites list
- counts unique users who saved a *place*
- treats a location pin as a bookmark

`collectCount` is a proxy for "people wanted to come back to this video,"
not "people saved this restaurant."

---

## Stubbed, disabled, or broken

These are conclusions from the current code, including cases that contradict
the README.

1. **Instagram is disabled at insert time.**
   `db.insert_hashtags(..., platforms=("tiktok",))` defaults to TikTok only.
   `generate_hashtags()` does not override that. The scraper will never see
   Instagram hashtags unless something else inserts them.
   `tests/test_pipeline.py::test_insert_hashtags_creates_both_platforms` still
   expects both platforms. README still says "TikTok + Instagram" and prices
   a two-platform run. Treat Instagram as leftover, not live.

2. **`collectCount` mapping is incomplete.**
   `_map_tiktok` reads `item.get("collectCount", 0)` only. Likes / comments /
   shares / views fall back to `item["stats"]`. The stats-fallback test fixture
   does not include `collectCount`. If the actor nests saves under `stats`
   (common), every TikTok save is stored as `0` and the 5x weight is a no-op.
   Ranking then becomes likes / comments / shares over views.

3. **`--max-posts` is silently capped for TikTok.**
   CLI default is 100; `_TIKTOK_MAX_PER_HASHTAG = 30` wins.

4. **Batch scrape loses hashtag attribution.**
   One Apify call is sent for *all* pending tags on a platform. Every
   returned post is linked to **every** hashtag in that batch
   (`scraper.py` after `insert_post`). Mention provenance is polluted.

5. **OCR is a single 20-post pass.**
   Not looped. Most cover text never reaches the extractor.

6. **City validation is not part of discovery.**
   Enrichment `in_city` is LLM-only, slideshow-only, and defaults to `true`
   when the model omits the field.

7. **No durable place identity.**
   Unique key is `(city_id, name)`. No Google Place ID, no coordinates, no
   resolution status. Dedup is string-fuzzy. The same venue can exist twice;
   two venues can be merged.

8. **`places.db` is local and gitignored.**
   Any existing file on a laptop is a stale cache, not a source of truth.
   Do not commit it.

9. **README / spec drift.**
   README "5-step" pipeline omits OCR. Universal hashtag helper comment says
   five tags; the list has four. Cost table still assumes two platforms.

None of these are "not yet written product." They are gaps inside the
discovery path that already exists.

---

## Credentials and ToS

| Dependency | Required for discovery? | Risk |
|---|---|---|
| `APIFY_API_TOKEN` | Yes, unless `--skip-scrape` on an existing DB | Paid. Community TikTok actor can break without notice. Scraping TikTok via a third-party actor is ToS-risky for TikTok (and possibly Apify's acceptable-use limits). |
| `OPENROUTER_API_KEY` | Yes (hashtags, extraction, dedup, tourist-trap) | Paid. Free-tier rate limits are not enough for a full city. |
| `GOOGLE_PLACES_API_KEY` | No for today's discover; yes for durable IDs later | Official API. Fine to use. Not persisted today. |
| `POSTIZ_*`, `REVENUECAT_*` | No | Slideshow posting / analytics only. Do not pull them into city-guide ranking. |

Do not add login-cookie scrapers, residential-proxy farms, or "download this
user's Liked / Saved" actors. That is a different (and worse) ToS and
privacy posture than hashtag search.

There is no official TikTok endpoint in this repo that lists saved places in
a city. Academic TikTok Research API access would not change that for a
consumer city-guide product.

---

## Hypothesis discarded

**Hypothesis:** this codebase can already find popular saved places in a
given city via TikTok.

**Discarded as a product claim.** Evidence:

- City is a hashtag seed + row key, not a geo query
  (`discover.py`, `pipeline/hashtags.py`, `pipeline/db.py`).
- Saves are video `collectCount`, not place bookmarks
  (`pipeline/scraper.py` `_map_tiktok`, Instagram mapper returns `saves: 0`).
- Sort key is engagement **rate**, so a low-view, high-save-rate clip can
  outrank a place that appears in many widely saved videos
  (`pipeline/scorer.py` `_score_places`).
- Repeat frequency (`mention_count`) exists but is not the ranking used by
  CSV, dashboard, or slideshow selection (`db.get_all_places`,
  `db.get_available_places`).
- Instagram dual-source story is stale (default platforms = TikTok only).
- Soft city filter does not run during discovery.

What the hypothesis *got right*: the scaffolding is here. City-scoped
ingest, post-level save field, mention graph, and CSV export are the right
bones. The missing piece is an honest ranking + durable identity + export
job on top of those bones — not a rewrite, and not "turn on saved-places
mode."

---

## Honest method we can claim later

For a Tokyo Eats guide, the defensible method is:

1. **Ingest** TikTok (and only later IG) posts under city + category
   hashtags, plus any structured location tags the scraper returns.
2. **Extract** named venues; **resolve** each name to a Google Place ID
   constrained to the city (official API). Drop or quarantine unresolved
   and out-of-city rows.
3. **Rank by repeat**, not by views:
   - primary: distinct source posts (and, better, distinct authors)
   - secondary: sum of video `collectCount` on those posts (labeled as
     "saves of videos that mention this place")
   - never: raw views, and never the current `virality_score` as the
     published rank
4. **Publish the method** next to the list. Do not title it "most saved
   on TikTok." Suggested label:
   `places that keep appearing in saved-heavy Tokyo TikToks`.

That is good enough to feed a later Atlasi page and a later slideshow that
*is* the guide. It is still a proxy.

---

## Extension plan (fit this codebase)

Prefer new modules and a new export job. Keep `discover.py` as ingest.
Do not overload `virality_score`. Do not generate the Atlasi page here.

Interface sketch: `pipeline/city_saved_places.py` (types + signatures only).

### 1. Data model (additive, SQLite)

Keep `cities`, `raw_posts`, `places`, `place_posts`. Add columns / tables:

**On `places` (or a 1:1 `place_resolutions` table):**

- `google_place_id TEXT`
- `lat REAL`, `lng REAL`
- `formatted_address TEXT`
- `resolved_city TEXT`
- `resolution_status TEXT` — `pending | resolved | out_of_city | unresolved`
- `hidden` already exists; use it for out-of-city / junk

**Computed ranking fields** (columns or a view, refreshed by a job):

- `distinct_posts INTEGER`
- `distinct_authors INTEGER`
- `total_collect_count INTEGER` — sum of TikTok `raw_posts.saves`
- `repeat_save_score REAL` — see formula below
- `rank_method TEXT` — honest label written at compute time

**Provenance (optional but cheap):**

- `discovery_runs(id, city_id, category, started_at, source, notes)`

Do not replace `virality_score`. Slideshow selection can keep using it.

Reuse `photo_search._find_place_id` (or extract a `pipeline/places_resolve.py`)
instead of a second Google client.

### 2. Ranking formula (v1, Tokyo food)

```
repeat_save_score =
    distinct_authors * 3.0
  + distinct_posts   * 1.0
  + log1p(total_collect_count) * 0.5
```

Require `distinct_posts >= 2` (or `distinct_authors >= 2`) before a place
can appear on the exported list. One viral video is not "keeps repeating."

Export both the score **and** the raw components so Atlasi can re-rank
later without re-scraping.

### 3. Jobs / CLI

| Job | Module | Role |
|---|---|---|
| Ingest (existing) | `discover.py` | Unchanged responsibility: hashtags → scrape → extract → dedup → virality → tourist trap. |
| Resolve (new) | `pipeline/places_resolve.py` | Google Text Search biased to city. Set resolution fields. Hide out-of-city. |
| Rank (new) | `pipeline/city_saved_places.py` | Compute repeat-save stats from `place_posts` × `raw_posts`. |
| Export (new) | `discover.py --export-city-guide` or `export_city_places.py` | JSON + CSV. No HTML page. |

Suggested export shape (versioned):

```json
{
  "schema_version": 1,
  "city": "Tokyo",
  "category": "food_and_drink",
  "method": "places that keep appearing in saved-heavy Tokyo TikToks",
  "method_limitations": [
    "Not TikTok saved-collection counts",
    "Hashtag recall, not a city geo query",
    "collectCount is video saves, not place bookmarks"
  ],
  "generated_at": "ISO-8601",
  "places": [
    {
      "rank": 1,
      "name": "…",
      "google_place_id": "…",
      "neighborhood": "…",
      "category": "food_and_drink",
      "distinct_posts": 7,
      "distinct_authors": 5,
      "total_collect_count": 12000,
      "repeat_save_score": 28.4,
      "virality_score": 0.21,
      "sample_urls": ["https://www.tiktok.com/…"]
    }
  ]
}
```

CSV can be a flat projection of `places[]`. `--export-csv` today is the
wrong ranking; leave it for slideshow/debug use or add a `--rank-by`
flag later. Do not silently change its meaning.

### 4. Discovery-path fixes (small, do these first)

Worth doing before a Tokyo production scrape, because they change the
inputs to any later rank:

1. Map `collectCount` from top-level **and** `stats` (mirror likes/views).
2. Persist `locationMeta` as columns (`location_name`, `location_address`),
   not only caption text.
3. Stop linking every batch post to every hashtag; map item → hashtag if
   the actor provides it, otherwise leave `post_hashtags` empty rather than
   lie.
4. Loop OCR until no remaining cover URLs, or drop the step from the
   "complete" story.
5. Update the Instagram tests / README to match TikTok-only default, or
   consciously re-enable IG knowing saves will be zero.

### 5. Module map

```
discover.py                         # ingest orchestrator (keep)
pipeline/scraper.py                 # fix collectCount + location fields
pipeline/extractor.py               # keep; optional: pass structured location
pipeline/scorer.py                  # keep virality for slideshows
pipeline/places_resolve.py          # NEW — Google Place ID + city check
pipeline/city_saved_places.py       # NEW — rank + export (stub exists)
pipeline/photo_search.py            # extract shared Place ID helper
generate_slideshow.py               # do not use for the guide list
pipeline/analytics.py etc.          # do not use for discovery ranking
```

---

## What not to bolt on

- **Atlasi city-guide UI / public page / iMessage landing.** Downstream.
  This repo should emit JSON/CSV.
- **Slideshow-as-guide.** `generate_slideshow.py` is an acquisition loop
  (weighted category/CTA, Postiz draft, last slide = app). A Tokyo Eats
  slideshow that *is* the guide is a later product. Do not overload this
  generator.
- **Guess Where sprint work.** Orthogonal.
- **Official "most saved" branding.** The data cannot support it.
- **User-bookmark scraping / session cookies / unofficial saved-list
  actors.** Privacy and ToS cost is not worth the signal.
- **Treating Instagram as an equal save source.** Actor does not return
  saves. Fine as a later recall booster, not as a rank input.
- **Replacing `virality_score` with the new rank.** Slideshow intelligence
  already consumes virality bands.
- **Rewriting ingest in a warehouse / Airflow / new DB.** SQLite + a
  second job is enough until Tokyo export is trusted.
- **Pulling Postiz / RevenueCat metrics into place rank.** Those measure
  Atlasi's own posts, not destination popularity.

---

## Risks

| Risk | Why it matters | Mitigation |
|---|---|---|
| Unofficial TikTok actor (`clockworks/free-tiktok-scraper`) | Breaks without notice; ToS-risky; field names drift (`collectCount` vs `stats`) | `build='latest'`; log a scrape sample of raw keys; degrade to mention-only rank if saves are all zero; no second unofficial stack. |
| Rate limits / cost | Apify ~$2/1k results; OpenRouter per batch; Google Places per resolve | Tokyo food-only first; keep `--max-posts`; cache Place IDs; `--skip-scrape` for rank/export reruns. |
| Stale `places.db` | Local file, gitignored, no run provenance | Treat as cache. `--reset` for a clean Tokyo run. Never commit. |
| Missing city filter | Off-city places in the Tokyo list | Google Places resolve + `out_of_city` before export. Do not rely on slideshow LLM hide. |
| Ranking that is views, not saves | Current score is engagement/views. If `collectCount` is missing it is even more like-weighted. | New `repeat_save_score`. Publish components. Gate on distinct authors. |
| Hashtag recall bias | Only venues that creators hashtag. Chains and already-viral spots dominate. | Category-specific tags; tourist-trap flag as a *label*, not automatic exclusion for a "people actually save" list (those places are often the ones people save). |
| Name-only identity | Dedup errors, missing Place IDs | Resolve before publish. Quarantine unresolved. |
| Batch hashtag pollution | Inflated "found via" stats | Fix attribution before using hashtags as a feature. |
| OCR / LLM miss | Places only on-screen or in video audio never extracted | Loop OCR; accept that audio-only mentions are out of scope for v1. |

---

## Tokyo-first next build steps

Do these in order. Stop after any step that disproves the proxy.

1. **Do not run a live scrape from CI / this agent.** Needs Emerson's
   Apify + OpenRouter keys. Do not commit `.env` or `places.db`.
2. **Instrument one small Tokyo food scrape** on a laptop:
   `python discover.py --city Tokyo --category food_and_drink --max-posts 20`.
   Immediately inspect `raw_posts`: is `saves` non-zero? Does `stats` contain
   `collectCount` when the top-level field is 0? How many captions have
   `📍 Location tag:`?
3. **Fix the ingest bugs in §4** if step 2 shows zero saves or garbage
   hashtag links. Re-run with `--skip-scrape` or `--reset` as needed.
4. **Implement `places_resolve.py`** using the existing Google Places
   text search, biased to Tokyo. Persist ids. Hide `out_of_city`.
5. **Implement `city_saved_places` rank + JSON/CSV export** against the
   resolved set. Primary sort = repeat-save score. Include method text.
6. **Manual review of the top 30.** If the list is Shibuya scramble +
   Disney + airport Starbucks, the method is recall-biased — tighten
   category seeds and the tourist-trap *label* (do not pretend it is
   a save count). If the list is repeating named restaurants with
   multiple authors, the proxy is good enough to hand to Atlasi.
7. **Only then** talk about a Tokyo Eats page and a slideshow that is
   the guide. This repo's job stops at the export file.

---

## Interface sketch

See `pipeline/city_saved_places.py`. Types and function signatures only;
every operation raises `NotImplementedError`. Next build should fill that
module in rather than adding a parallel ranker inside `scorer.py`.
