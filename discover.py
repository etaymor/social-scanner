#!/usr/bin/env python3
"""Atlasi Place Discovery Pipeline — CLI entrypoint and orchestrator."""

import argparse
import csv
import logging
import sys

import db
from config import DEFAULT_MAX_POSTS


def setup_logging(verbose: bool, quiet: bool):
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


def print_summary(conn, city_id: int, city_name: str):
    stats = db.get_city_stats(conn, city_id)
    places = db.get_all_places(conn, city_id)
    non_traps = [p for p in places if not p["is_tourist_trap"]]

    print(f"\n{'='*50}")
    print(f"  Atlasi Place Discovery: {city_name}")
    print(f"{'='*50}\n")
    print(f"  Scraped: {stats['posts']:,} posts across {stats['hashtags']} hashtags")
    print(f"  Extracted: {stats['places']} unique places")
    print(f"  Tourist traps filtered: {stats['tourist_traps']}\n")

    top = non_traps[:20]
    if top:
        print(f"  Top {len(top)} places by virality score (excluding tourist traps):\n")
        for i, p in enumerate(top, 1):
            print(f"  {i:>3}. {p['name']} ({p['type']}) — score: {p['virality_score']:.4f}")
    else:
        print("  No places found.\n")

    print(f"\n  Full results in: {db.DB_PATH}")
    print()


def export_csv(conn, city_id: int, city_name: str, filepath: str = None):
    places = db.get_all_places(conn, city_id)
    non_traps = [p for p in places if not p["is_tourist_trap"]]

    if not filepath:
        safe_name = city_name.lower().replace(" ", "_")
        filepath = f"{safe_name}_places.csv"

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "name", "type", "mention_count", "virality_score"])
        for i, p in enumerate(non_traps, 1):
            writer.writerow([i, p["name"], p["type"], p["mention_count"], p["virality_score"]])

    print(f"\n  Exported {len(non_traps)} places to: {filepath}")
    return filepath


def main():
    parser = argparse.ArgumentParser(
        description="Atlasi Place Discovery Pipeline — discover trending places from social media",
    )
    parser.add_argument("--city", required=True, help="City name to research")
    parser.add_argument("--max-posts", type=int, default=DEFAULT_MAX_POSTS,
                        help=f"Max posts per hashtag per platform (default: {DEFAULT_MAX_POSTS})")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip Apify scraping, run extraction on existing data")
    parser.add_argument("--reset", action="store_true",
                        help="Clear all data for this city before running")
    parser.add_argument("--export-csv", action="store_true",
                        help="Export results to CSV file")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")
    args = parser.parse_args()

    setup_logging(args.verbose, args.quiet)
    log = logging.getLogger("discover")

    try:
        city_name = validate_city(args.city)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    conn = db.get_connection()
    db.init_db(conn)

    city_id = db.get_or_create_city(conn, city_name)

    if args.reset:
        log.info("Resetting all data for %s...", city_name)
        db.reset_city(conn, city_id)
        city_id = db.get_or_create_city(conn, city_name)

    # Step 1: Hashtag Generation
    from hashtags import generate_hashtags
    log.info("Step 1/5: Generating hashtags for %s...", city_name)
    tags = generate_hashtags(conn, city_id, city_name, verbose=args.verbose)
    log.info("Generated %d unique hashtags", len(tags))

    # Step 2: Apify Scraping
    if not args.skip_scrape:
        from scraper import scrape_posts
        log.info("Step 2/5: Scraping social media posts...")
        scrape_posts(conn, city_id, city_name, max_posts=args.max_posts, verbose=args.verbose)
    else:
        log.info("Step 2/5: Skipping scraping (--skip-scrape)")

    # Step 3: LLM Place Extraction
    from extractor import extract_places
    log.info("Step 3/5: Extracting places from captions...")
    extract_places(conn, city_id, city_name, verbose=args.verbose)

    # Step 4: Deduplication + Scoring
    from scorer import deduplicate_and_score
    log.info("Step 4/5: Deduplicating and scoring places...")
    deduplicate_and_score(conn, city_id, city_name, verbose=args.verbose)

    # Step 5: Tourist Trap Filter
    from filter import filter_tourist_traps
    log.info("Step 5/5: Filtering tourist traps...")
    filter_tourist_traps(conn, city_id, city_name, verbose=args.verbose)

    # Output
    print_summary(conn, city_id, city_name)

    if args.export_csv:
        export_csv(conn, city_id, city_name)

    conn.close()
    log.info("Pipeline complete for %s", city_name)


if __name__ == "__main__":
    main()
