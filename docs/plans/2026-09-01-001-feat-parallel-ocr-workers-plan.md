---
title: "feat: parallelize per-post OCR with OCR_WORKERS pool"
type: feat
status: active
date: 2026-09-01
---

# feat: parallelize per-post OCR with OCR_WORKERS pool

## Problem

Seoul listicle OCR of ~71 videos took ~90 minutes. Wall clock matches **serial**
per-post work (yt-dlp + ffmpeg 1.5s samples + Flash Lite) even though
`extract_cover_text` already submits to a `ThreadPoolExecutor`. Worker count is
hardcoded (`min(5, len(posts))`), not tunable, and there are **no tests proving
N posts OCR concurrently**. Emerson: no reason this is not parallel.

## Settled

1. **Worker pool** — OCR many posts concurrently via a thread pool. Download +
   ffmpeg + engine chain stay in workers. Configurable `OCR_WORKERS` (env),
   default **4**. Clamp to a safe range (1–16) so we do not melt OpenRouter /
   TikTok.
2. **Serialize SQLite upserts** — **hunch verified as design constraint**:
   workers never touch `conn`. Only the orchestrator thread applies
   `UPDATE raw_posts` / `commit`, under an explicit write lock so DB writes
   cannot interleave corruptly even if we commit per-result while workers run.
3. **Never fail-closed** — one post miss continues the city run.
4. **Engine chain unchanged** — Tesseract → `gemini-3.1-flash-lite` →
   `gemini-3-flash-preview`. Never `*-flash-image`.
5. **Sampling unchanged** — every slideshow frame; video every
   `OCR_VIDEO_FRAME_INTERVAL` (default 1.5s, clamped 1–2).
6. **Tests** — prove N posts OCR concurrently (overlap / active workers);
   prove parallel OCR + serialized DB writes leave consistent
   `ocr_status` / captions. Fixtures/mocks only — **no Apify**.

## Rejected

- ProcessPool (heavier; pickle pain for mocks) for this I/O-bound path
- Parallelizing SQLite writers / sharing one `Connection` across threads
- Raising default workers high enough to hammer OpenRouter/TikTok
- Changing frame interval or OCR model chain
- Live Apify spend for verification

## Technical approach

- `config.OCR_WORKERS` from env, default 4, clamp 1–16.
- `_ocr_worker_count(n_posts)` → `min(OCR_WORKERS, n_posts, OCR_WORKERS_MAX)`.
- `extract_cover_text`: build media timelines on the orchestrator thread; submit
  `_process_one` to the pool; apply results via `_apply_ocr_result` under
  `threading.Lock`; commit under the same lock (per result or end-of-batch —
  prefer per-result so a mid-batch crash keeps progress).
- Keep existing soft-fail semantics (`failed` / `empty` / `done`).

## Success criteria

- [ ] `OCR_WORKERS` documented in `.env.example` / README
- [ ] Concurrency test fails if pool is effectively serial
- [ ] DB integrity test under parallel OCR
- [ ] Existing OCR chain / fail-open / interval tests still pass
- [ ] PR open; do not merge unless tests green
