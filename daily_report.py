#!/usr/bin/env python3
"""Atlasi Daily Analytics Report — orchestrate the analytics intelligence loop.

Runs the full daily cycle in 3 commit phases:
  Phase 1: Postiz data (release IDs, per-post stats, platform stats, stale drafts)
  Phase 2: RevenueCat data (overview snapshot, conversion attribution)
  Phase 3: Intelligence (evaluate, circuit breaker, weights, report)

Generates a markdown report saved to reports/YYYY-MM-DD.md.
"""

import argparse
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import config
from pipeline import db
from pipeline import analytics
from pipeline import conversions
from pipeline import intelligence
from pipeline.analytics import AnalyticsAuthError
from pipeline.conversions import RevenueCatAuthError

log = logging.getLogger(__name__)


def setup_logging(verbose: bool, quiet: bool) -> None:
    level = logging.DEBUG if verbose else (logging.WARNING if quiet else logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Atlasi Daily Analytics Report — run the analytics intelligence loop",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=config.ANALYTICS_LOOKBACK_DAYS,
        help=f"Lookback window in days (default: {config.ANALYTICS_LOOKBACK_DAYS})",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")
    return parser


# ---------------------------------------------------------------------------
# Phase runners — each returns a dict of collected data for the report
# ---------------------------------------------------------------------------


def run_phase1(conn, days: int) -> dict:
    """Phase 1: Postiz data collection. Commits on success."""
    result = {
        "posts_fetched": [],
        "release_ids_connected": 0,
        "analytics_upserted": 0,
        "platform_stats": None,
        "stale_marked": 0,
        "error": None,
    }
    try:
        # Step 1: Fetch posts from Postiz
        posts = analytics.fetch_posts(days)
        result["posts_fetched"] = posts

        # Step 2: Connect release IDs (+ recover stale)
        result["release_ids_connected"] = analytics.connect_release_ids(conn, posts)

        # Step 3: Fetch per-post analytics
        result["analytics_upserted"] = analytics.fetch_post_analytics(conn, days)

        # Step 4: Fetch platform-level stats
        result["platform_stats"] = analytics.fetch_platform_stats(conn)

        # Step 5: Detect stale drafts
        result["stale_marked"] = analytics.detect_stale_drafts(conn)

        # Commit Phase 1
        conn.commit()
        log.info(
            "Phase 1 complete: %d release IDs connected, %d analytics rows, %d stale",
            result["release_ids_connected"],
            result["analytics_upserted"],
            result["stale_marked"],
        )
    except AnalyticsAuthError:
        raise  # Let CLI handle auth errors
    except Exception as e:
        log.error("Phase 1 error: %s", e)
        result["error"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
    return result


def run_phase2(conn, days: int) -> dict:
    """Phase 2: RevenueCat data collection. Commits on success.

    Skips entirely if RC is not configured (returns rc_configured=False).
    """
    result = {
        "rc_configured": bool(config.REVENUECAT_V2_SECRET_KEY),
        "rc_snapshot": None,
        "rc_deltas": None,
        "conversions_attributed": 0,
        "error": None,
    }
    if not result["rc_configured"]:
        log.info("Phase 2 skipped: RevenueCat not configured")
        return result

    try:
        # Step 6: Fetch RC overview snapshot
        result["rc_snapshot"] = conversions.fetch_rc_snapshot(conn)

        # Step 7: Attribute conversions
        result["conversions_attributed"] = conversions.attribute_conversions(conn, days)

        # Compute deltas for the report
        result["rc_deltas"] = conversions.compute_rc_deltas(conn)

        # Commit Phase 2
        conn.commit()
        log.info(
            "Phase 2 complete: %d conversions attributed",
            result["conversions_attributed"],
        )
    except RevenueCatAuthError:
        raise  # Let CLI handle auth errors
    except Exception as e:
        log.error("Phase 2 error: %s", e)
        result["error"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
    return result


def run_phase3(conn, phase2_data: dict) -> dict:
    """Phase 3: Intelligence computation and report data. Commits on success."""
    result = {
        "evaluated": 0,
        "circuit_breaker_tripped": False,
        "previous_weights": {},
        "new_weights": {},
        "post_count": 0,
        "error": None,
    }
    try:
        # Step 9: Evaluate matured slideshows
        result["evaluated"] = intelligence.evaluate_slideshows(conn)

        # Step 10: Check circuit breaker
        result["circuit_breaker_tripped"] = intelligence.check_circuit_breaker(conn)

        # Read previous weights before computing new ones
        result["previous_weights"] = intelligence.read_weights()

        # Count published posts for cold-start detection
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM slideshows WHERE publish_status = 'published'"
        ).fetchone()
        result["post_count"] = row["cnt"] if row else 0

        # Step 11: Compute new weights
        if result["circuit_breaker_tripped"]:
            # Reset all weights to 1.0
            result["new_weights"] = {d: {} for d in intelligence.DIMENSIONS}
            log.warning("Circuit breaker tripped — resetting all weights to 1.0")
        else:
            result["new_weights"] = intelligence.compute_dimension_weights(
                conn, previous_weights=result["previous_weights"]
            )

        # Step 12: Write weights atomically
        intelligence.write_weights(
            result["new_weights"],
            post_count=result["post_count"],
            circuit_breaker=result["circuit_breaker_tripped"],
        )

        # Commit Phase 3
        conn.commit()
        log.info(
            "Phase 3 complete: %d evaluated, circuit_breaker=%s",
            result["evaluated"],
            result["circuit_breaker_tripped"],
        )
    except Exception as e:
        log.error("Phase 3 error: %s", e)
        result["error"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
    return result


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def _query_slideshow_table(conn, days: int) -> list[dict]:
    """Query slideshows with analytics + performance data for the report table."""
    cutoff = datetime.now(UTC) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    rows = conn.execute(
        """
        SELECT s.id, s.hook_text, s.category, s.format, s.posted_at,
               s.publish_status, s.city_id,
               c.name as city_name,
               sa.views, sa.likes, sa.comments, sa.shares, sa.saves,
               sa.views_estimated,
               sp.conversions, sp.composite_score, sp.decision_tag,
               sp.views_at_48h
        FROM slideshows s
        LEFT JOIN cities c ON c.id = s.city_id
        LEFT JOIN slideshow_analytics sa ON sa.slideshow_id = s.id
        LEFT JOIN slideshow_performance sp ON sp.slideshow_id = s.id
        WHERE s.posted_at IS NOT NULL
          AND s.posted_at >= ?
        ORDER BY s.posted_at DESC
        """,
        (cutoff_str,),
    ).fetchall()

    result = []
    for r in rows:
        result.append({
            "id": r["id"],
            "hook_text": r["hook_text"] or "",
            "category": r["category"] or "",
            "format": r["format"] or "",
            "posted_at": r["posted_at"] or "",
            "publish_status": r["publish_status"] or "draft",
            "city_name": r["city_name"] or "",
            "views": r["views"] or 0,
            "likes": r["likes"] or 0,
            "comments": r["comments"] or 0,
            "shares": r["shares"] or 0,
            "saves": r["saves"] or 0,
            "views_estimated": bool(r["views_estimated"]) if r["views_estimated"] is not None else False,
            "conversions": r["conversions"] or 0,
            "composite_score": r["composite_score"] or 0.0,
            "decision_tag": r["decision_tag"] or "",
            "views_at_48h": r["views_at_48h"] or 0,
        })
    return result


def _query_platform_growth(conn) -> dict:
    """Compare latest two platform_stats rows to compute growth."""
    rows = conn.execute(
        "SELECT * FROM platform_stats ORDER BY fetched_at DESC LIMIT 2"
    ).fetchall()

    if len(rows) < 1:
        return {"available": False}

    latest = dict(rows[0])
    if len(rows) < 2:
        return {
            "available": True,
            "has_previous": False,
            "followers": latest.get("followers", 0),
            "total_views": latest.get("total_views", 0),
            "total_likes": latest.get("total_likes", 0),
            "videos": latest.get("videos", 0),
        }

    previous = dict(rows[1])
    return {
        "available": True,
        "has_previous": True,
        "followers": latest.get("followers", 0),
        "followers_delta": latest.get("followers", 0) - previous.get("followers", 0),
        "total_views": latest.get("total_views", 0),
        "total_views_delta": latest.get("total_views", 0) - previous.get("total_views", 0),
        "total_likes": latest.get("total_likes", 0),
        "total_likes_delta": latest.get("total_likes", 0) - previous.get("total_likes", 0),
        "videos": latest.get("videos", 0),
        "videos_delta": latest.get("videos", 0) - previous.get("videos", 0),
    }


def _query_attribution_table(conn, days: int) -> list[dict]:
    """Find slideshows that drove conversions in the lookback window."""
    cutoff = datetime.now(UTC) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    rows = conn.execute(
        """
        SELECT s.id, s.hook_text, s.category, c.name as city_name,
               sp.conversions, sp.views_at_48h, sp.conversion_rate
        FROM slideshow_performance sp
        JOIN slideshows s ON s.id = sp.slideshow_id
        LEFT JOIN cities c ON c.id = s.city_id
        WHERE sp.conversions > 0
          AND s.posted_at >= ?
        ORDER BY sp.conversions DESC
        """,
        (cutoff_str,),
    ).fetchall()

    return [dict(r) for r in rows]


def _query_top_hooks(conn, limit: int = 5) -> list[dict]:
    """Return top-performing hooks by composite score."""
    rows = conn.execute(
        """
        SELECT s.hook_text, s.category, c.name as city_name,
               sp.composite_score, sp.views_at_48h, sp.decision_tag
        FROM slideshow_performance sp
        JOIN slideshows s ON s.id = sp.slideshow_id
        LEFT JOIN cities c ON c.id = s.city_id
        WHERE sp.decision_tag IN ('scale', 'keep')
        ORDER BY sp.composite_score DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    return [dict(r) for r in rows]


def _truncate(text: str, max_len: int = 40) -> str:
    """Truncate text to max_len, adding ellipsis if truncated."""
    text = text.replace("\n", " ")
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _format_delta(value: float | int) -> str:
    """Format a delta value with + or - prefix."""
    if value > 0:
        return f"+{value:,}"
    if value < 0:
        return f"{value:,}"
    return "0"


def _compute_weight_changes(
    previous: dict[str, dict[str, float]],
    current: dict[str, dict[str, float]],
) -> list[dict]:
    """Compute per-dimension-value weight changes."""
    changes = []
    all_dims = set(list(previous.keys()) + list(current.keys()))
    for dim in sorted(all_dims):
        if dim.startswith("_"):
            continue
        prev_vals = previous.get(dim, {})
        curr_vals = current.get(dim, {})
        if not isinstance(prev_vals, dict) or not isinstance(curr_vals, dict):
            continue
        all_vals = set(list(prev_vals.keys()) + list(curr_vals.keys()))
        for val in sorted(all_vals):
            old_w = prev_vals.get(val, 1.0)
            new_w = curr_vals.get(val, 1.0)
            delta = new_w - old_w
            if abs(delta) > 0.001:
                changes.append({
                    "dimension": dim,
                    "value": val,
                    "old": old_w,
                    "new": new_w,
                    "delta": delta,
                })
    return changes


def generate_report(
    conn,
    days: int,
    phase1: dict,
    phase2: dict,
    phase3: dict,
    report_date: str,
) -> str:
    """Generate the markdown report string from collected phase data."""
    lines: list[str] = []

    # -- Header --
    lines.append(f"# Atlasi Daily Analytics Report")
    lines.append(f"")
    lines.append(f"**Date:** {report_date}")
    lines.append(f"**Lookback:** {days} days")
    lines.append(f"")

    # -- Phase errors --
    errors = []
    if phase1.get("error"):
        errors.append(f"- Phase 1 (Postiz): {phase1['error']}")
    if phase2.get("error"):
        errors.append(f"- Phase 2 (RevenueCat): {phase2['error']}")
    if phase3.get("error"):
        errors.append(f"- Phase 3 (Intelligence): {phase3['error']}")
    if errors:
        lines.append("## Warnings")
        lines.append("")
        for e in errors:
            lines.append(e)
        lines.append("")

    # -- Cold-start notice --
    post_count = phase3.get("post_count", 0)
    if post_count < 10:
        lines.append("## Cold Start")
        lines.append("")
        lines.append(
            f"Insufficient data -- only {post_count} published post(s). "
            "Weights not adjusted until at least 10 posts are published. "
            "Performance data below is preliminary."
        )
        lines.append("")

    # -- Circuit breaker --
    if phase3.get("circuit_breaker_tripped"):
        lines.append("## CIRCUIT BREAKER TRIPPED")
        lines.append("")
        lines.append(
            "7-day average views dropped below 50% of 30-day average. "
            "All weights have been reset to 1.0 as a safety measure."
        )
        lines.append("")

    # -- Slideshow performance table --
    slideshows = _query_slideshow_table(conn, days)
    lines.append("## Slideshow Performance")
    lines.append("")
    if slideshows:
        has_rc = phase2.get("rc_configured", False)
        lines.append(
            "| Date | Hook | City | Category | Views | Likes | Comments | Shares | "
            + ("Conversions | " if has_rc else "")
            + "Diagnosis |"
        )
        lines.append(
            "|------|------|------|----------|-------|-------|----------|--------|"
            + ("------------|" if has_rc else "")
            + "-----------|"
        )
        for s in slideshows:
            date_str = s["posted_at"][:10] if s["posted_at"] else "n/a"
            hook = _truncate(s["hook_text"])
            views_str = f"{s['views']:,}"
            if s["views_estimated"]:
                views_str += "*"

            # Per-slideshow diagnosis
            views_good = s["views"] >= config.VIEWS_GOOD
            conversions_good = s["conversions"] > 0
            diagnosis = s["decision_tag"] or conversions.diagnose_funnel(
                views_good, conversions_good, has_rc
            )

            row = (
                f"| {date_str} | {hook} | {s['city_name']} | {s['category']} | "
                f"{views_str} | {s['likes']:,} | {s['comments']:,} | {s['shares']:,} | "
            )
            if has_rc:
                row += f"{s['conversions']} | "
            row += f"{diagnosis} |"
            lines.append(row)

        lines.append("")
        estimated = [s for s in slideshows if s["views_estimated"]]
        if estimated:
            lines.append("*\\* Views marked with asterisk are estimated via delta method.*")
            lines.append("")
    else:
        lines.append("No slideshows posted in the last {days} days.".format(days=days))
        lines.append("")

    # -- Platform growth --
    growth = _query_platform_growth(conn)
    lines.append("## Platform Growth")
    lines.append("")
    if growth.get("available"):
        if growth.get("has_previous"):
            lines.append(f"| Metric | Current | Change |")
            lines.append(f"|--------|---------|--------|")
            lines.append(
                f"| Followers | {growth['followers']:,} | {_format_delta(growth['followers_delta'])} |"
            )
            lines.append(
                f"| Total Views | {growth['total_views']:,} | {_format_delta(growth['total_views_delta'])} |"
            )
            lines.append(
                f"| Total Likes | {growth['total_likes']:,} | {_format_delta(growth['total_likes_delta'])} |"
            )
            lines.append(
                f"| Videos | {growth['videos']:,} | {_format_delta(growth['videos_delta'])} |"
            )
        else:
            lines.append(
                f"| Metric | Current |"
            )
            lines.append(f"|--------|---------|")
            lines.append(f"| Followers | {growth['followers']:,} |")
            lines.append(f"| Total Views | {growth['total_views']:,} |")
            lines.append(f"| Total Likes | {growth['total_likes']:,} |")
            lines.append(f"| Videos | {growth['videos']:,} |")
            lines.append("")
            lines.append("*First snapshot -- deltas available after next run.*")
    else:
        lines.append("No platform stats available yet.")
    lines.append("")

    # -- RevenueCat summary --
    if phase2.get("rc_configured"):
        lines.append("## RevenueCat Summary")
        lines.append("")
        snapshot = phase2.get("rc_snapshot")
        deltas = phase2.get("rc_deltas")

        if snapshot:
            lines.append("| Metric | Current | Change |")
            lines.append("|--------|---------|--------|")
            for key in ("mrr", "active_trials", "active_subscriptions", "revenue"):
                val = snapshot.get(key, 0)
                if key in ("mrr", "revenue"):
                    val_str = f"${val:,.2f}"
                else:
                    val_str = f"{int(val):,}"
                delta_str = ""
                if deltas and key in deltas:
                    d = deltas[key]
                    if key in ("mrr", "revenue"):
                        delta_str = f"+${d:,.2f}" if d >= 0 else f"-${abs(d):,.2f}"
                    else:
                        delta_str = _format_delta(int(d))
                lines.append(f"| {key.replace('_', ' ').title()} | {val_str} | {delta_str} |")
            lines.append("")

            # Funnel diagnosis
            total_views = growth.get("total_views", 0) if growth.get("available") else 0
            views_good = total_views > 0
            conversions_good = snapshot.get("active_trials", 0) > 0
            funnel = conversions.diagnose_funnel(views_good, conversions_good, True)
            lines.append(f"**Funnel Diagnosis:** {funnel}")
            lines.append("")
        elif phase2.get("error"):
            lines.append(f"RevenueCat data unavailable: {phase2['error']}")
            lines.append("")
        else:
            lines.append("No RevenueCat snapshot available.")
            lines.append("")

        # -- Conversion attribution --
        attributions = _query_attribution_table(conn, days)
        if attributions:
            lines.append("## Conversion Attribution")
            lines.append("")
            lines.append("| Hook | City | Category | Conversions | Views (48h) |")
            lines.append("|------|------|----------|-------------|-------------|")
            for a in attributions:
                hook = _truncate(a.get("hook_text", ""), 35)
                lines.append(
                    f"| {hook} | {a.get('city_name', '')} | {a.get('category', '')} | "
                    f"{a.get('conversions', 0)} | {a.get('views_at_48h', 0):,} |"
                )
            lines.append("")

    # -- Weight changes --
    lines.append("## Weight Changes")
    lines.append("")
    weight_changes = _compute_weight_changes(
        phase3.get("previous_weights", {}),
        phase3.get("new_weights", {}),
    )
    if weight_changes:
        lines.append("| Dimension | Value | Previous | New | Delta |")
        lines.append("|-----------|-------|----------|-----|-------|")
        for wc in weight_changes:
            direction = "promoted" if wc["delta"] > 0 else "dropped"
            lines.append(
                f"| {wc['dimension']} | {wc['value']} | "
                f"{wc['old']:.4f} | {wc['new']:.4f} | "
                f"{wc['delta']:+.4f} ({direction}) |"
            )
        lines.append("")
    else:
        lines.append("No weight changes this cycle.")
        lines.append("")

    # -- Hook recommendations --
    top_hooks = _query_top_hooks(conn)
    lines.append("## Hook Recommendations")
    lines.append("")
    if top_hooks:
        lines.append("Top-performing hooks to replicate:")
        lines.append("")
        for i, h in enumerate(top_hooks, 1):
            hook = h.get("hook_text", "").replace("\n", " | ")
            score = h.get("composite_score", 0)
            tag = h.get("decision_tag", "")
            city = h.get("city_name", "")
            lines.append(f"{i}. **{hook}** ({city}, score={score:.2f}, tag={tag})")
        lines.append("")
    else:
        lines.append("Not enough data for hook recommendations yet.")
        lines.append("")

    # -- Summary stats --
    lines.append("## Run Summary")
    lines.append("")
    lines.append(f"- Release IDs connected: {phase1.get('release_ids_connected', 0)}")
    lines.append(f"- Analytics rows updated: {phase1.get('analytics_upserted', 0)}")
    lines.append(f"- Stale drafts marked: {phase1.get('stale_marked', 0)}")
    lines.append(f"- Conversions attributed: {phase2.get('conversions_attributed', 0)}")
    lines.append(f"- Slideshows evaluated: {phase3.get('evaluated', 0)}")
    lines.append(f"- Published posts total: {phase3.get('post_count', 0)}")
    lines.append("")

    return "\n".join(lines)


def save_report(report: str, report_date: str) -> Path:
    """Save the report to reports/YYYY-MM-DD.md and return the path."""
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    path = reports_dir / f"{report_date}.md"
    path.write_text(report, encoding="utf-8")
    return path


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    setup_logging(args.verbose, args.quiet)
    log = logging.getLogger(__name__)

    report_date = datetime.now(UTC).strftime("%Y-%m-%d")
    days = args.days

    conn = db.get_connection()
    try:
        db.init_db(conn)

        log.info("Starting daily analytics report (days=%d)", days)

        # Phase 1: Postiz data
        log.info("Phase 1: Collecting Postiz data...")
        phase1 = run_phase1(conn, days)

        # Phase 2: RevenueCat data
        log.info("Phase 2: Collecting RevenueCat data...")
        phase2 = run_phase2(conn, days)

        # Phase 3: Intelligence
        log.info("Phase 3: Running intelligence computations...")
        phase3 = run_phase3(conn, phase2)

        # Generate report
        log.info("Generating report...")
        report = generate_report(conn, days, phase1, phase2, phase3, report_date)

        # Save report
        report_path = save_report(report, report_date)
        log.info("Report saved to %s", report_path)

        # Print summary to stdout
        print(f"\n{'=' * 50}")
        print(f"  Atlasi Daily Analytics Report: {report_date}")
        print(f"  Lookback: {days} days")
        print(f"{'=' * 50}")
        print(f"  Release IDs connected: {phase1.get('release_ids_connected', 0)}")
        print(f"  Analytics rows updated: {phase1.get('analytics_upserted', 0)}")
        print(f"  Stale drafts marked: {phase1.get('stale_marked', 0)}")
        print(f"  Conversions attributed: {phase2.get('conversions_attributed', 0)}")
        print(f"  Slideshows evaluated: {phase3.get('evaluated', 0)}")
        print(f"  Published posts: {phase3.get('post_count', 0)}")
        if phase3.get("circuit_breaker_tripped"):
            print("  !! CIRCUIT BREAKER TRIPPED — weights reset to 1.0")
        print(f"  Report: {report_path}")
        for p_name, p_data in [("Postiz", phase1), ("RevenueCat", phase2), ("Intelligence", phase3)]:
            if p_data.get("error"):
                print(f"  WARNING: {p_name} phase error: {p_data['error']}")
        print()

    except AnalyticsAuthError as e:
        log.error("Postiz authentication failed: %s", e)
        print(
            "\n  Postiz auth error. Check POSTIZ_API_KEY and POSTIZ_TIKTOK_INTEGRATION_ID.\n",
            file=sys.stderr,
        )
        sys.exit(1)
    except RevenueCatAuthError as e:
        log.error("RevenueCat authentication failed: %s", e)
        print(
            "\n  RevenueCat auth error. Check REVENUECAT_V2_SECRET_KEY and REVENUECAT_PROJECT_ID.\n",
            file=sys.stderr,
        )
        sys.exit(1)
    finally:
        conn.close()

    log.info("Daily report complete")


if __name__ == "__main__":
    main()
