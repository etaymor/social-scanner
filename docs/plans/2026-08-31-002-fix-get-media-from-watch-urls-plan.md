---
title: "fix: GET media from TikTok watch/photo URLs — OCR real frames"
type: fix
status: active
date: 2026-08-31
---

# fix: GET media from TikTok watch/photo URLs — OCR real frames

## Problem (post PR #11)

Seoul Apify run returned coverUrl+duration+webVideoUrl with **empty downloadAddr / mediaUrls**.
PR #11 refused to treat `raw_posts.url` (webVideoUrl) as a video source, so OCR fell back to
covers. Cover-only is the wrong unit for video posts.

## Settled

1. **Video** — When Apify omits a CDN `downloadAddr`, store/use the watch page (`webVideoUrl`) as
   the video source. Download via yt-dlp (or equivalent). ffmpeg sample every 1–2s. OCR those frames.
2. **Slideshow** — Prefer actor image fields (`mediaUrls` / `slideshowImageLinks`). If a photo post
   has no frame URLs, resolve all frames from the photo page URL the same way (yt-dlp). Never
   treat cover-only as success when N frames exist or can be resolved.
3. **Product filter** — last ~30 days, likes ≥ 100, food/restaurant queries, hard cap ~20.
4. **Never fail-closed** — one post miss continues the city run.
5. **OCR engines** — keep PR #10 chain; never `*-flash-image`.
6. **Tests** — webVideoUrl-only → multiple video samples (mock download+ffmpeg). N slideshow
   frames, not cover-only. Fixtures/mocks only — no live Apify spend.

## Rejected

- Empty Apify `video_url` ⇒ “nothing we can do”
- Cover-only OCR as success for a video post
- Hashtag fan-out / large paid spends for this path
