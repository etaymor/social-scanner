#!/usr/bin/env python3
"""Atlasi Slideshow Generator — CLI entrypoint and orchestrator."""

import argparse
import logging
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

from config import (
    CATEGORIES,
    SLIDESHOW_OUTPUT_DIR,
    VALID_CATEGORIES,
)
from pipeline import db
from pipeline.image_gen import GeminiQuotaError
from pipeline.llm import CreditsExhaustedError
from pipeline.slideshow_types import (
    CTASlideText,
    HookSlideText,
    LocationSlideText,
    SlideshowMeta,
    to_meta_json,
    to_texts_json,
)


def setup_logging(verbose: bool, quiet: bool) -> None:
    level = logging.DEBUG if verbose else (logging.WARNING if quiet else logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def build_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Atlasi Slideshow Generator — create TikTok slideshows from discovered places",
    )
    parser.add_argument("--city", required=True, help="City name to generate slideshow for")
    parser.add_argument(
        "--category",
        choices=sorted(VALID_CATEGORIES),
        default=None,
        help="Focus on a specific category (e.g., food_and_drink)",
    )
    parser.add_argument(
        "--slide-count", type=int, default=8, help="Number of location slides (4-15, default: 8)"
    )
    parser.add_argument(
        "--format",
        dest="hook_format",
        choices=["listicle", "story"],
        default="listicle",
        help="Hook format (default: listicle)",
    )
    parser.add_argument(
        "--post", action="store_true", help="Post to TikTok as draft via Postiz after generation"
    )
    parser.add_argument(
        "--allow-reuse", action="store_true", help="Allow reusing places from recent slideshows"
    )
    parser.add_argument(
        "--cta-template",
        type=str,
        default=None,
        help="Path to CTA template image (default: docs/Atlasi Ingest 1.PNG)",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")
    return parser


def _normalize_raw_filenames(output_dir: Path, slide_count: int) -> None:
    """Rename hook/CTA raw files so overlay can find them as slide_{N}_raw.png."""
    hook_special = output_dir / "slide_1_hook_raw.png"
    hook_normal = output_dir / "slide_1_raw.png"
    if hook_special.exists() and not hook_normal.exists():
        shutil.copy2(hook_special, hook_normal)

    cta_num = slide_count + 2
    cta_special = output_dir / f"slide_{cta_num}_cta_raw.png"
    cta_normal = output_dir / f"slide_{cta_num}_raw.png"
    if cta_special.exists() and not cta_normal.exists():
        shutil.copy2(cta_special, cta_normal)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    setup_logging(args.verbose, args.quiet)
    log = logging.getLogger(__name__)

    # Validate slide count
    if not 4 <= args.slide_count <= 15:
        print("Error: --slide-count must be between 4 and 15", file=sys.stderr)
        sys.exit(1)

    conn = db.get_connection()
    try:
        db.init_db(conn)

        # Step 1/11: Validate city
        log.info("Step 1/11: Validating city...")
        city_name = args.city.strip()
        row = conn.execute("SELECT id FROM cities WHERE name = ?", (city_name,)).fetchone()
        if not row:
            print(
                f"Error: City '{city_name}' not found in database. Run discover.py first.",
                file=sys.stderr,
            )
            sys.exit(1)
        city_id = row["id"]
        log.info("City: %s (id=%d)", city_name, city_id)

        # Step 2/11: Query available places
        log.info("Step 2/11: Querying available places...")
        available = db.get_available_places(
            conn,
            city_id,
            category=args.category,
            allow_reuse=args.allow_reuse,
        )
        log.info("Found %d available places", len(available))

        slide_count = args.slide_count
        if len(available) < slide_count:
            if len(available) < 4:
                print(
                    f"Error: Only {len(available)} places available (minimum 4 required). "
                    "Run discover.py for more data or use --allow-reuse.",
                    file=sys.stderr,
                )
                sys.exit(1)
            log.warning(
                "Only %d places available (requested %d). Adjusting slide count.",
                len(available),
                slide_count,
            )
            slide_count = len(available)

        # Select top N places by virality score (already ordered)
        selected_places = available[:slide_count]
        place_names = [dict(p)["name"] for p in selected_places]
        log.info("Selected %d places: %s", slide_count, ", ".join(place_names))

        # Step 3/11: Enrich places
        log.info("Step 3/11: Enriching places with neighborhood + image prompts...")
        from pipeline.enrichment import enrich_places

        enriched_count = enrich_places(conn, selected_places, city_name)
        log.info("Enriched %d places", enriched_count)

        # Re-fetch places to get enrichment data
        place_ids = [dict(p)["id"] for p in selected_places]
        placeholders = ",".join("?" * len(place_ids))
        selected_places = conn.execute(
            f"SELECT * FROM places WHERE id IN ({placeholders}) ORDER BY virality_score DESC",
            place_ids,
        ).fetchall()

        # Step 4/11: Generate hook
        log.info("Step 4/11: Generating hook (%s format)...", args.hook_format)
        from pipeline.hooks import generate_hook

        hook_result = generate_hook(
            city_name=city_name,
            slide_count=slide_count,
            hook_format=args.hook_format,
            category=args.category,
        )
        hook_text = hook_result["hook_text"]
        hook_image_prompt = hook_result["hook_image_prompt"]
        caption = hook_result["caption"]
        log.info("Hook text: %s", hook_text.replace("\n", " | "))

        # Step 5/11: Create output directory
        log.info("Step 5/11: Creating output directory...")
        date_str = datetime.now().strftime("%Y-%m-%d")
        cat_slug = args.category or "all"
        city_slug = re.sub(r"[^a-z0-9-]", "", city_name.lower().replace(" ", "-"))
        base_dir = SLIDESHOW_OUTPUT_DIR / f"{city_slug}-{cat_slug}-{args.hook_format}-{date_str}"

        # Find next sequence number
        seq = 1
        output_dir = base_dir.parent / f"{base_dir.name}-{seq:03d}"
        while output_dir.exists() and (output_dir / "meta.json").exists():
            seq += 1
            output_dir = base_dir.parent / f"{base_dir.name}-{seq:03d}"

        if not output_dir.resolve().is_relative_to(SLIDESHOW_OUTPUT_DIR.resolve()):
            print("Error: Invalid city name produces unsafe path", file=sys.stderr)
            sys.exit(1)

        output_dir.mkdir(parents=True, exist_ok=True)
        log.info("Output directory: %s", output_dir)

        # Step 6/11: Generate images
        log.info("Step 6/11: Generating images...")
        from pipeline.image_gen import generate_slideshow_images
        from pipeline.image_styles import select_slideshow_style

        visual_style = select_slideshow_style(city_name, date_str)
        log.info(
            "Visual style: %s + %s + %s + %s",
            visual_style["time_of_day"]["name"],
            visual_style["weather"]["name"],
            visual_style["perspective"]["name"],
            visual_style["color_mood"]["name"],
        )

        places_for_gen = []
        for p in selected_places:
            pd = dict(p)
            places_for_gen.append(
                {
                    "name": pd["name"],
                    "image_prompt": pd.get("image_prompt")
                    or f"A beautiful {pd.get('type', 'place')} in {city_name}",
                }
            )

        cta_template = args.cta_template
        if not cta_template:
            default_cta = Path("docs/Atlasi Ingest 1.PNG")
            if default_cta.exists():
                cta_template = str(default_cta)

        img_result = generate_slideshow_images(
            output_dir=output_dir,
            places=places_for_gen,
            hook_image_prompt=hook_image_prompt,
            cta_template_path=cta_template,
            style=visual_style,
        )
        log.info(
            "Images: %d generated, %d skipped, %d failed",
            img_result["generated"],
            img_result["skipped"],
            img_result["failed"],
        )
        if img_result["failed_slides"]:
            log.warning("Failed slides: %s", img_result["failed_slides"])

        # Step 7/11: Build texts.json
        log.info("Step 7/11: Building texts.json...")
        slides = []
        slides.append(HookSlideText(text=hook_text))
        for i, p in enumerate(selected_places, start=1):
            pd = dict(p)
            slides.append(
                LocationSlideText(
                    name=pd["name"],
                    neighborhood=pd.get("neighborhood") or "",
                    number=f"{i}/{slide_count}",
                )
            )
        slides.append(CTASlideText(text="Find more hidden gems\non Atlasi"))

        texts_path = output_dir / "texts.json"
        texts_path.write_text(to_texts_json(slides), encoding="utf-8")
        log.info("Saved texts.json")

        # Step 8/11: Normalize filenames and add text overlays
        log.info("Step 8/11: Adding text overlays...")
        _normalize_raw_filenames(output_dir, slide_count)
        from pipeline.overlay import add_overlays

        overlay_count = add_overlays(output_dir)
        log.info("Applied %d overlays", overlay_count)

        # Verify all expected slide files exist before proceeding
        total_slides = slide_count + 2  # hook + locations + CTA
        missing_slides = [
            f"slide_{i}.png"
            for i in range(1, total_slides + 1)
            if not (output_dir / f"slide_{i}.png").exists()
        ]
        if missing_slides:
            log.error(
                "Aborting: %d slide(s) missing after overlay: %s",
                len(missing_slides),
                missing_slides,
            )
            print(
                f"\nError: {len(missing_slides)} slide(s) missing: {missing_slides}\n"
                f"  Partial output saved in: {output_dir}\n",
                file=sys.stderr,
            )
            sys.exit(1)

        # Step 9/11: Save metadata
        log.info("Step 9/11: Saving metadata...")
        meta = SlideshowMeta(
            city=city_name,
            category=args.category,
            format=args.hook_format,
            hook_text=hook_text,
            slide_count=slide_count,
            created_at=datetime.now().isoformat(),
            places=[
                {
                    "id": dict(p)["id"],
                    "name": dict(p)["name"],
                    "neighborhood": dict(p).get("neighborhood") or "",
                }
                for p in selected_places
            ],
        )
        meta_path = output_dir / "meta.json"
        meta_path.write_text(to_meta_json(meta), encoding="utf-8")
        log.info("Saved meta.json")

        # Step 10/11: Record in database
        log.info("Step 10/11: Recording slideshow in database...")
        slideshow_id = db.create_slideshow(
            conn,
            city_id,
            category=args.category,
            hook_format=args.hook_format,
            hook_text=hook_text,
            slide_count=slide_count,
            output_dir=str(output_dir),
        )
        for i, p in enumerate(selected_places, start=1):
            db.add_slideshow_place(conn, slideshow_id, dict(p)["id"], slide_number=i)
        conn.commit()
        log.info("Slideshow recorded (id=%d)", slideshow_id)

        # Step 11/11: Post to TikTok (optional)
        posted = False
        if args.post:
            log.info("Step 11/11: Posting to TikTok via Postiz...")
            from pipeline.posting import PostingAuthError, PostingError, post_slideshow

            try:
                post_meta = post_slideshow(output_dir, caption)
                db.mark_slideshow_posted(conn, slideshow_id, post_meta.postiz_post_id)
                log.info("Posted! Postiz ID: %s", post_meta.postiz_post_id)
                posted = True
            except PostingAuthError as e:
                log.error("Postiz authentication failed: %s", e)
                print(
                    "\n  Postiz auth error. Check POSTIZ_API_KEY and POSTIZ_TIKTOK_INTEGRATION_ID.\n",
                    file=sys.stderr,
                )
                sys.exit(1)
            except PostingError as e:
                log.error("Posting failed: %s", e)
                print(
                    f"\n  Posting failed: {e}\n"
                    f"  Slides are saved in: {output_dir}\n"
                    "  You can retry with: --post\n",
                    file=sys.stderr,
                )
                sys.exit(1)
        else:
            log.info("Step 11/11: Skipping posting (use --post to post)")

        # Summary
        print(f"\n{'=' * 50}")
        print(f"  Atlasi Slideshow: {city_name}")
        if args.category:
            print(f"  Category: {CATEGORIES[args.category]['label']}")
        print(f"  Format: {args.hook_format}")
        print(f"  Slides: {slide_count} locations + hook + CTA = {slide_count + 2} total")
        print(f"{'=' * 50}\n")
        print(f"  Hook: {hook_text.replace(chr(10), ' | ')}")
        print(f"  Places: {', '.join(dict(p)['name'] for p in selected_places)}")
        print(f"  Output: {output_dir}")
        if img_result["failed_slides"]:
            print(f"  ⚠ Failed slides: {img_result['failed_slides']}")
        if posted:
            print("  Posted: Yes (add trending music before publishing)")
        else:
            print("  Posted: No (use --post to post to TikTok)")
        print()

    except CreditsExhaustedError:
        print(
            "\n  OpenRouter credits exhausted.\n"
            "  Add credits at https://openrouter.ai and re-run.\n"
            "  Progress has been saved — the pipeline will resume where it left off.\n",
            file=sys.stderr,
        )
        sys.exit(1)
    except GeminiQuotaError:
        print(
            "\n  Gemini API quota exhausted.\n"
            "  Check your OpenRouter credits and re-run.\n"
            "  Progress has been saved — the pipeline will resume where it left off.\n",
            file=sys.stderr,
        )
        sys.exit(1)
    finally:
        conn.close()

    log.info("Slideshow generation complete for %s", city_name)


if __name__ == "__main__":
    main()
