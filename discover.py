#!/usr/bin/env python3
"""Atlasi Place Discovery Pipeline — CLI entrypoint and orchestrator."""

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path

from config import CATEGORIES, DEFAULT_MAX_POSTS, VALID_CATEGORIES
from pipeline import db
from pipeline.llm import CreditsExhaustedError


def setup_logging(verbose: bool, quiet: bool) -> None:
    level = logging.DEBUG if verbose else (logging.WARNING if quiet else logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def validate_city(city_name: str) -> str:
    """Basic city name validation."""
    city = city_name.strip()
    if not city or len(city) < 2:
        raise ValueError(f"Invalid city name: '{city_name}'")
    return city


def print_summary(
    conn: sqlite3.Connection, city_id: int, city_name: str, category: str | None = None
) -> None:
    stats = db.get_city_stats(conn, city_id)
    places = db.get_all_places(conn, city_id)
    non_traps = [p for p in places if not p["is_tourist_trap"]]

    print(f"\n{'=' * 50}")
    print(f"  Atlasi Place Discovery: {city_name}")
    if category:
        print(f"  Category: {CATEGORIES[category]['label']}")
    print(f"{'=' * 50}\n")
    print(f"  Scraped: {stats['posts']:,} posts across {stats['hashtags']} hashtags")
    print(f"  Extracted: {stats['places']} unique places")
    print(f"  Tourist traps filtered: {stats['tourist_traps']}\n")

    top = non_traps[:20]
    if top:
        print(f"  Top {len(top)} places by virality score (excluding tourist traps):\n")
        for i, p in enumerate(top, 1):
            cat_label = ""
            if p["category"]:
                cat_info = CATEGORIES.get(p["category"])
                cat_label = f" [{cat_info['label']}]" if cat_info else f" [{p['category']}]"
            print(
                f"  {i:>3}. {p['name']} ({p['type']}{cat_label}) — score: {p['virality_score']:.4f}"
            )
    else:
        print("  No places found.\n")

    print(f"\n  Full results in: {db.DB_PATH}")
    print()


def export_csv(
    conn: sqlite3.Connection, city_id: int, city_name: str, filepath: str | None = None
) -> str:
    places = db.get_all_places(conn, city_id)
    non_traps = [p for p in places if not p["is_tourist_trap"]]

    if not filepath:
        safe_name = city_name.lower().replace(" ", "_")
        filepath = Path(f"{safe_name}_places.csv").name

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "name", "type", "category", "mention_count", "virality_score"])
        for i, p in enumerate(non_traps, 1):
            writer.writerow(
                [
                    i,
                    p["name"],
                    p["type"],
                    p.get("category", ""),
                    p["mention_count"],
                    p["virality_score"],
                ]
            )

    print(f"\n  Exported {len(non_traps)} places to: {filepath}")
    return filepath


def build_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Atlasi Place Discovery Pipeline — discover trending places from social media",
    )
    parser.add_argument("--city", required=True, help="City name to research")
    parser.add_argument(
        "--category",
        choices=sorted(VALID_CATEGORIES),
        default=None,
        help="Focus discovery on a specific category (e.g., food_and_drink, nightlife)",
    )
    parser.add_argument(
        "--max-posts",
        type=int,
        default=DEFAULT_MAX_POSTS,
        help=f"Max posts per hashtag per platform (default: {DEFAULT_MAX_POSTS})",
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip Apify scraping, run extraction on existing data",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Reset failed hashtags to pending so they get re-scraped",
    )
    parser.add_argument(
        "--reset", action="store_true", help="Clear all data for this city before running"
    )
    parser.add_argument("--export-csv", action="store_true", help="Export results to CSV file")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    setup_logging(args.verbose, args.quiet)
    log = logging.getLogger(__name__)

    try:
        city_name = validate_city(args.city)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    conn = db.get_connection()
    try:
        db.init_db(conn)

        city_id = db.get_or_create_city(conn, city_name)

        if args.reset:
            log.info("Resetting all data for %s...", city_name)
            db.reset_city(conn, city_id)
            city_id = db.get_or_create_city(conn, city_name)

        # Recovery: reset any hashtags stuck in "running" from a previous crash
        conn.execute(
            "UPDATE hashtags SET scrape_status = 'pending' WHERE city_id = ? AND scrape_status = 'running'",
            (city_id,),
        )
        conn.commit()

        # Retry failed hashtags if requested
        if args.retry_failed:
            reset_count = conn.execute(
                "UPDATE hashtags SET scrape_status = 'pending' WHERE city_id = ? AND scrape_status = 'failed'",
                (city_id,),
            ).rowcount
            conn.commit()
            if reset_count:
                log.info("Reset %d failed hashtags to pending for retry", reset_count)

        # Step 1: Hashtag Generation
        from pipeline.hashtags import generate_hashtags

        category = args.category
        if category:
            log.info(
                "Step 1/5: Generating %s hashtags for %s...",
                CATEGORIES[category]["label"],
                city_name,
            )
        else:
            log.info("Step 1/5: Generating hashtags for %s...", city_name)
        tags = generate_hashtags(conn, city_id, city_name, category=category)
        log.info("Generated %d unique hashtags", len(tags))

        # Step 2: Apify Scraping
        if not args.skip_scrape:
            from pipeline.scraper import scrape_posts

            log.info("Step 2/5: Scraping social media posts...")
            scrape_posts(conn, city_id, city_name, max_posts=args.max_posts)
        else:
            log.info("Step 2/5: Skipping scraping (--skip-scrape)")

        # Step 2.5: Visual OCR — extract on-screen text from cover images
        from pipeline.ocr import extract_cover_text

        log.info("Step 2.5: Running visual OCR on cover images...")
        extract_cover_text(conn, city_id, city_name)

        # Step 3: LLM Place Extraction
        from pipeline.extractor import extract_places

        log.info("Step 3/5: Extracting places from captions...")
        extract_places(conn, city_id, city_name)

        # Step 4: Deduplication + Scoring
        from pipeline.scorer import deduplicate_and_score

        log.info("Step 4/5: Deduplicating and scoring places...")
        deduplicate_and_score(conn, city_id, city_name)

        # Step 5: Tourist Trap Filter
        from pipeline.filter import filter_tourist_traps

        log.info("Step 5/5: Filtering tourist traps...")
        filter_tourist_traps(conn, city_id, city_name)

        # Output
        print_summary(conn, city_id, city_name, category=category)

        if args.export_csv:
            export_csv(conn, city_id, city_name)

    except CreditsExhaustedError:
        print(
            "\n  OpenRouter credits exhausted.\n"
            "  Add credits at https://openrouter.ai and re-run.\n"
            "  Progress has been saved — the pipeline will resume where it left off.\n",
            file=sys.stderr,
        )
        sys.exit(1)
    finally:
        conn.close()

    log.info("Pipeline complete for %s", city_name)


if __name__ == "__main__":
    main()
