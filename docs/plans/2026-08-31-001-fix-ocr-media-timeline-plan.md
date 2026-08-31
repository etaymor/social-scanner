---
title: "fix: OCR unit = per-post media timeline (not cover_url)"
type: fix
status: completed
date: 2026-08-31
---

# fix: OCR unit = per-post media timeline (not cover_url)

## Problem

After PR #10, a Seoul skip-scrape OCR run on existing posts OCRed **cover thumbnails only**. Result: high "enriched" counts with low venue density (max `mention_count` 4, no 10+ venues). On-screen venue names live in:

1. **Every slideshow / photo-mode frame** (not the cover alone)
2. **Sampled frames of the actual video** (not the cover thumbnail)

Cover-only is the wrong OCR unit.

## Settled (user-directed)

| Decision | Choice |
| --- | --- |
| Slideshow OCR | OCR **all** slideshow/photo-mode frames for a post |
| Video OCR | Grab one frame every **1–2 seconds**, OCR those frames |
| Aggregation | **Union** on-screen text per post before extraction |
| Engine chain | Keep PR #10: Tesseract → `google/gemini-3.1-flash-lite` → `google/gemini-3-flash-preview` |
| Fail behavior | **Never fail-closed** — one frame/engine miss must not abort the city run |
| Apify | **No** Apify spend, no live scrape, no paid download-slideshows actor required to land the code |
| Image-gen models | Never OCR with `*-flash-image` |

## Rejected

- Cover-only OCR as the primary / sufficient unit for slideshow or video posts.

## Verified facts (inspect, not assume)

- `pipeline/ocr.py` `_image_urls_for_post` already concatenates `cover_url` + newline-split `slideshow_urls`, but Seoul Apify items in `tests/fixtures/seoul_apify_sample.json` have **empty `mediaUrls`** and no `slideshowImageLinks` — so OCR collapses to cover.
- Scraper already sets `shouldDownloadSlideshowImages=True` for new scrapes; `_slideshow_urls` misses `tiktokLink` on slideshow link dicts (Apify samples use `tiktokLink` + `downloadLink`).
- Free-scraper video items expose `videoMeta.duration` + cover URLs, not always a playable MP4 URL. Direct CDN / `downloadAddr` / `videoUrl` when present must be persisted; otherwise downloading the post's own media via the page `url` (yt-dlp) is allowed — Apify is not.
- `ffmpeg` is available for local frame sampling.

## Proposed solution (smallest change)

Replace cover-only as the OCR unit inside the **existing** `extract_cover_text` path (no parallel OCR pipeline):

1. **Persist media timeline fields** on scrape/replay:
   - Fix slideshow URL extraction (`tiktokLink`, nested image lists).
   - Store `video_url` on `raw_posts` when the Apify item already has a downloadable media URL.
2. **Build a per-post frame list**:
   - If `slideshow_urls` non-empty → OCR **all** those frames (cover alone is not enough; cover deduped if duplicated).
   - Else if `video_url` (or resolvable page `url` → local download) → sample JPEG frames every `OCR_VIDEO_FRAME_INTERVAL` (default **1.5s**, in 1–2s band) via ffmpeg; OCR each.
   - Else → cover fallback only (last resort for media-less rows).
3. **Union** successful frame texts into one on-screen block on the caption.
4. Keep PR #10 engine fallback; mark posts `failed`/`empty`/`done` without aborting discover.
5. Partial 404s on individual frames: skip that frame, continue the post and the city run.

## Out of scope

- Live Seoul/Tokyo scrape or Apify dataset refresh.
- Changing extractor / ranking / fail-closed policy (already removed in PR #10).
- Using `*-flash-image` for OCR.

## Acceptance tests

- Slideshow fixture with N frames → OCR invoked for all N; cover-only text alone is insufficient for the assertion.
- Synthetic video of known duration → ≈ `duration / 1.5` frames (± 1–2s interval tolerance).
- Some frames 404 → city run / `extract_cover_text` continues (no raise); remaining frames still enrich when possible.
- Default OCR models remain lite / preview vision, not image-gen.
