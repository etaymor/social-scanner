---
date: 2026-03-25
topic: analytics-intelligence-loop
---

# Analytics Intelligence Loop

## Problem Frame

Atlasi generates slideshows about trending places and posts them to TikTok via Postiz, but the pipeline is fire-and-forget. Once a slideshow is posted, there's no feedback on how it performed — no view counts, no engagement metrics, no conversion tracking. The system can't learn what works. Every day it generates content with the same blind assumptions instead of compounding knowledge from real performance data.

The goal is to close the loop: post → measure → learn → auto-adjust → post smarter content tomorrow. Optimizing for the full funnel: TikTok views → app downloads → trials → paid subscribers.

## Requirements

### Analytics Collection

- R1. Pull per-post TikTok analytics (views, likes, comments, shares, saves) from Postiz API and store them in the existing SQLite database, linked to the slideshow record via the Postiz post ID already saved in `slideshows.posted_at` / `post_meta.json`.
- R2. Handle the Postiz/TikTok release ID connection — posts go up as drafts, TikTok takes ~2 hours to index them. The analytics module must connect Postiz posts to their TikTok video IDs (using the `/posts/{id}/missing` endpoint and chronological matching from Larry's `check-analytics.js`) before pulling per-post stats.
- R3. Pull platform-level TikTok stats (followers, total views, total likes) from Postiz for delta tracking (growth over time).

### Conversion Tracking

- R4. Pull conversion data from RevenueCat API: trial starts, paid conversions (initial purchase + trial converted), revenue, active subscribers, MRR.
- R5. Cross-reference conversion timestamps with post publish times using a 72-hour attribution window. Score each slideshow: conversions-in-window / (views / 1000) = conversion rate per 1K views.
- R6. Detect and flag systemic app issues vs marketing issues: high views + low conversions = CTA or app onboarding problem; low views + high conversions = hook problem; high views + high conversions = scale it.

### Multi-Dimensional Intelligence

- R7. Track performance across six dimensions simultaneously and identify winning/losing combinations:
  - **Hook style** (listicle vs story, conflict hook vs curiosity vs budget)
  - **Category** (food_and_drink, nightlife, shopping, etc. — from the existing 8 categories)
  - **City** (which cities produce content that converts?)
  - **Visual style** (time of day, weather, perspective, color mood — from `image_styles.py`)
  - **Place virality band** (do places with higher discovery virality scores also make better slideshows?)
  - **CTA variant** (what call-to-action text on slide 6 drives downloads?)
- R8. Maintain a `performance_weights.json` file that stores learned preferences per dimension. Updated daily by the intelligence module. Example: `{"category": {"food_and_drink": 1.4, "nightlife": 0.7}, "city": {"paris": 1.6, "london": 0.9}}`.

### Auto-Adjustment

- R9. `generate_slideshow.py` reads `performance_weights.json` to bias place selection, category choice, hook style, and visual style toward historically winning combinations. Weights influence but don't fully determine — always leave room for exploration (e.g., 70% exploit winning patterns, 30% explore new ones).
- R10. Apply Larry's decision rules adapted for the multi-dimensional context:
  - 50K+ views → DOUBLE DOWN: generate 3 variations of that hook/category/city combo
  - 10K-50K → keep in rotation, test tweaks on one dimension at a time
  - 1K-10K → try 1 more variation before dropping
  - <1K twice → DROP: flag that combination as underperforming

### Daily Report

- R11. A daily report script that runs each morning, pulling the last 3 days of data (TikTok posts peak at 24-48h, conversions attribute over 24-72h). Generates a markdown report saved to `reports/YYYY-MM-DD.md`.
- R12. The report includes: per-slideshow performance table, platform growth deltas, RevenueCat conversion summary, per-post funnel diagnosis (using the diagnostic framework from R6), auto-generated hook recommendations based on winners, and a summary of what `performance_weights.json` changed.
- R13. Track hook performance history in `hook-performance.json` (or a database table), recording hook text, category, city, style, views, conversions, and CTA used — so the system builds a growing corpus of what works.

## Success Criteria

- After 2 weeks of daily operation, the system can demonstrate measurably improving average views-per-post and conversion-rate-per-post compared to the first week (or identify and flag why it isn't improving)
- The daily report surfaces actionable intelligence a human can act on in <2 minutes
- `generate_slideshow.py` produces noticeably different (better-informed) content choices after 1 week of data vs day 1
- No manual data entry required — everything flows from Postiz API + RevenueCat API automatically

## Scope Boundaries

- **Not building a web dashboard** — daily markdown reports are sufficient. The existing Flask dashboard stays as-is for place browsing.
- **Not automating music selection** — posts remain drafts; the user adds trending audio manually before publishing. This is intentional per Larry's proven approach.
- **Not changing the discovery pipeline** — `discover.py` and its scoring remain untouched. We're adding a feedback loop to the *output* side (slideshows), not changing the *input* side (place discovery).
- **Not building A/B testing infrastructure** — the exploration/exploitation ratio in R9 provides lightweight experimentation. Formal A/B testing is overkill at this stage.
- **RevenueCat webhook server is out of scope** — we use the RevenueCat REST API (V1/V2) to pull data on-demand during the daily report, not a persistent webhook listener.

## Key Decisions

- **Python, not Node**: Larry's scripts are Node.js, but Atlasi is a Python project. All new modules will be Python, adapting Larry's logic. The Larry docs serve as reference architecture, not code to port.
- **SQLite over JSON files**: Larry uses JSON files (`hook-performance.json`, `analytics-snapshot.json`). We'll store analytics in the existing SQLite database with new tables/columns, keeping JSON only for `performance_weights.json` (which `generate_slideshow.py` needs to read quickly at generation time).
- **72-hour attribution window**: Matches Larry's proven approach. TikTok posts peak at 24-48h, conversion attribution needs up to 72h.
- **Explore/exploit ratio 70/30**: Auto-adjustment biases toward winners (70%) but always tries new combinations (30%) to avoid local maxima.

## Dependencies / Assumptions

- Postiz API key is already configured in `config.py` and the API supports the analytics endpoints documented in Larry's `analytics-loop.md`
- RevenueCat V1 or V2 API key is available (user confirmed RevenueCat is live)
- The `slideshows` table already stores Postiz post IDs from the posting step
- Posts are published as drafts — analytics only populate after the user manually publishes from TikTok and ~2 hours pass for indexing

## Outstanding Questions

### Deferred to Planning

- [Affects R1, R2][Technical] What new SQLite tables/columns are needed? Likely `slideshow_analytics` table or columns on `slideshows`.
- [Affects R4][Needs research] Which RevenueCat API version (V1 vs V2) is available for the Atlasi project? V2 has better endpoints for metrics overview but may require different auth.
- [Affects R8, R9][Technical] How should `performance_weights.json` be structured to handle the six-dimensional space without combinatorial explosion? Likely independent per-dimension weights rather than full cross-product.
- [Affects R9][Technical] How does the 70/30 explore/exploit ratio integrate with `generate_slideshow.py`'s existing place selection logic? Likely a weighted random selection step.
- [Affects R11][Technical] How should the daily cron be scheduled? System cron, or a Claude Code scheduled trigger?
- [Affects R13][Technical] Should hook performance live in SQLite (cleaner, queryable) or JSON (simpler, matches Larry's approach)? Leaning SQLite since everything else is there.

## Next Steps

-> `/ce:plan` for structured implementation planning
