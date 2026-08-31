---
title: "fix: OCR media timeline — ingest slideshow URLs + video 1–2s samples"
type: fix
status: completed
date: 2026-08-31
---

# fix: OCR media timeline — ingest slideshow URLs + video 1–2s samples

## Architecture (read of main `8c83dd5`)

OCR is a **post-scrape caption enricher**. On main, `extract_cover_text` already OCRs `cover_url` **plus every** newline-separated `slideshow_urls` line. The Seoul cover-only run happened because those extra URLs were **empty in `raw_posts`**, not because the OCR loop cannot take multiple stills.

| Field | Mapper source (main) | Notes |
| --- | --- | --- |
| `cover_url` | `videoMeta.coverUrl` / `originalCoverUrl` | Always present on TikTok items |
| `slideshow_urls` | `mediaUrls` then `slideshowImageLinks` / `imageUrls` | Empty in Seoul fixture → cover-only OCR |
| `url` | `webVideoUrl` | Watch page, **not** bytes |
| video download | *(dropped)* | No `video_url` / duration / `isSlideshow` columns |
| Actor flags | `shouldDownloadSlideshowImages=True`, covers off, **no** video download flag | Instagram: `displayUrl` only |

## Settled

1. **Slideshow** — Prove ingest persists **every** slide URL (`mediaUrls` and link dicts incl. `tiktokLink`). OCR loop is not the slideshow bug unless URLs never land. Tests **must fail** if the mapper drops `mediaUrls`.
2. **Video 1–2s samples (NEW)** — Persist downloadable `video_url` + `video_duration` (+ `is_slideshow`) from Apify item fields when present. Land the code **without** a new paid Apify run (replay / synthetic JSON). `ffmpeg` samples every 1–2s; reuse PR #10 engine chain. Download/ffmpeg miss → `ocr_status=failed` for that post; **city continues**. Never OCR `*-flash-image`. Do **not** also OCR cover at t=0 when video samples include t=0 (no double-count).
3. **Tests that fail** cover-only success, dropped `mediaUrls`, and zero video samples on a synthetic clip of known duration.

## Rejected

- Treating cover-only OCR as a sufficient unit for slideshow/video posts.
- Soft-succeeding a video post via cover when download/ffmpeg misses (recreates Seoul false enrichment).
- Apify spend / live scrape to land this PR.

## Implementation

- Mapper: keep every non-mp4 `mediaUrls` entry; read `tiktokLink`; map `video_url`, `video_duration`, `is_slideshow`.
- OCR stills: `cover_url` + all `slideshow_urls` (deduped) — same unit as main, once URLs land.
- OCR video: samples from `video_url` only (no cover). Interval default 1.5s (clamped 1–2).
- Engine chain unchanged; never fail-closed on the city run.
