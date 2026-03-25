"""RevenueCat V2 API integration for conversion tracking and attribution."""

import logging
import sqlite3
from datetime import UTC, datetime, timedelta

import requests

import config
from pipeline.retry import retry_with_backoff

log = logging.getLogger(__name__)

# Retry settings
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds

# Metric IDs we care about from the overview endpoint
OVERVIEW_METRIC_IDS = frozenset(
    {
        "mrr",
        "active_trials",
        "active_subscriptions",
        "active_users",
        "new_customers",
        "revenue",
    }
)


# ---------------------------------------------------------------------------
# Error hierarchy
# ---------------------------------------------------------------------------


class RevenueCatError(Exception):
    """Retryable RevenueCat API error (5xx, timeout, rate-limit)."""

    pass


class RevenueCatAuthError(RevenueCatError):
    """Non-retryable auth failure (401/403)."""

    pass


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------


class RevenueCatClient:
    """Thin wrapper around the RevenueCat V2 REST API."""

    def __init__(self, v2_secret_key: str, project_id: str):
        self.project_id = project_id
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {v2_secret_key}",
                "Content-Type": "application/json",
            }
        )
        self.base_url = config.REVENUECAT_BASE_URL

    # -- helpers ----------------------------------------------------------

    def _get(self, path: str, params: dict | None = None, timeout: int = 30) -> dict:
        """Issue a GET request and return the parsed JSON body.

        Raises RevenueCatAuthError on 401/403.
        Raises RevenueCatError on other HTTP errors (retryable).
        """
        url = f"{self.base_url}{path}"

        def _do_get():
            resp = self.session.get(url, params=params, timeout=timeout)
            if resp.status_code in (401, 403):
                raise RevenueCatAuthError(
                    f"RevenueCat auth failed (HTTP {resp.status_code})"
                )
            if resp.status_code == 429:
                raise RevenueCatError(
                    f"RevenueCat rate-limited (HTTP 429)"
                )
            if 400 <= resp.status_code < 500:
                raise RevenueCatAuthError(
                    f"RevenueCat client error (HTTP {resp.status_code}): {resp.text[:200]}"
                )
            resp.raise_for_status()
            return resp.json()

        return retry_with_backoff(
            _do_get,
            max_retries=MAX_RETRIES,
            base_delay=RETRY_BASE_DELAY,
            non_retryable=(RevenueCatAuthError,),
        )

    # -- public API methods -----------------------------------------------

    def get_overview_metrics(self, currency: str = "USD") -> dict[str, float]:
        """Fetch aggregate metrics from /v2/projects/{id}/metrics/overview.

        Returns a dict mapping metric_id to its numeric value, e.g.
        ``{"mrr": 670.0, "active_trials": 12, ...}``.
        """
        path = f"/v2/projects/{self.project_id}/metrics/overview"
        data = self._get(path, params={"currency": currency})

        if not isinstance(data, dict) or "metrics" not in data:
            raise RevenueCatError(
                f"Unexpected overview response structure: {str(data)[:200]}"
            )

        result: dict[str, float] = {}
        for metric in data["metrics"]:
            mid = metric.get("id")
            val = metric.get("value")
            if mid and val is not None:
                result[mid] = float(val)
        return result

    def list_subscriptions(
        self, status: str | None = None, limit: int = 50
    ) -> list[dict]:
        """Paginate through /v2/projects/{id}/subscriptions.

        Returns the full collected list of subscription dicts.
        """
        path = f"/v2/projects/{self.project_id}/subscriptions"
        base_params: dict[str, str | int] = {"limit": limit}
        if status:
            base_params["status"] = status

        all_subs: list[dict] = []
        cursor: str | None = None
        while True:
            params = dict(base_params)
            if cursor:
                params["starting_after"] = cursor

            data = self._get(path, params=params)

            if not isinstance(data, dict):
                raise RevenueCatError(
                    f"Unexpected subscriptions response structure: {str(data)[:200]}"
                )

            subs = data.get("subscriptions", [])
            all_subs.extend(subs)

            next_page = data.get("next_page")
            if not next_page:
                break
            cursor = next_page

        return all_subs

    def get_recent_trials(self, days: int) -> list[dict]:
        """Return trials whose starts_at falls within the last *days* days.

        Client-side filter over list_subscriptions(status="trialing").
        """
        cutoff = datetime.now(UTC) - timedelta(days=days)
        trials = self.list_subscriptions(status="trialing")

        recent: list[dict] = []
        for trial in trials:
            starts_at_raw = trial.get("starts_at")
            if not starts_at_raw:
                continue
            try:
                starts_at = datetime.fromisoformat(starts_at_raw.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                log.warning("Skipping trial with unparseable starts_at: %s", starts_at_raw)
                continue
            if starts_at >= cutoff:
                recent.append(trial)

        return recent


# ---------------------------------------------------------------------------
# Module-level pipeline functions
# ---------------------------------------------------------------------------


def _get_client() -> RevenueCatClient | None:
    """Build a RevenueCatClient from config, or None if not configured."""
    if not config.REVENUECAT_V2_SECRET_KEY:
        log.warning("REVENUECAT_V2_SECRET_KEY not set — skipping RevenueCat operations")
        return None
    if not config.REVENUECAT_PROJECT_ID:
        log.warning("REVENUECAT_PROJECT_ID not set — skipping RevenueCat operations")
        return None
    return RevenueCatClient(
        config.REVENUECAT_V2_SECRET_KEY, config.REVENUECAT_PROJECT_ID
    )


def fetch_rc_snapshot(conn: sqlite3.Connection) -> dict[str, float] | None:
    """Pull overview metrics from RevenueCat and store a snapshot row.

    Returns the metrics dict on success, or None if RC is not configured.
    Does NOT commit — caller is responsible for committing.
    """
    client = _get_client()
    if client is None:
        return None

    metrics = client.get_overview_metrics()
    conn.execute(
        """INSERT INTO rc_snapshots (mrr, active_trials, active_subscriptions,
           active_users, new_customers, revenue)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            metrics.get("mrr", 0.0),
            metrics.get("active_trials", 0),
            metrics.get("active_subscriptions", 0),
            metrics.get("active_users", 0),
            metrics.get("new_customers", 0),
            metrics.get("revenue", 0.0),
        ),
    )
    return metrics


def compute_rc_deltas(conn: sqlite3.Connection) -> dict[str, float] | None:
    """Compare the latest two rc_snapshots rows and return deltas.

    Returns None if fewer than 2 snapshots exist.
    Does NOT commit.
    """
    rows = conn.execute(
        "SELECT * FROM rc_snapshots ORDER BY fetched_at DESC LIMIT 2"
    ).fetchall()

    if len(rows) < 2:
        return None

    latest, previous = rows[0], rows[1]
    delta_cols = ("mrr", "active_trials", "active_subscriptions", "active_users", "new_customers", "revenue")
    deltas: dict[str, float] = {}
    for col in delta_cols:
        deltas[col] = float(latest[col]) - float(previous[col])
    return deltas


def attribute_conversions(conn: sqlite3.Connection, days: int) -> int:
    """Attribute recent trials to slideshows using last-touch model.

    For each trial that started within the last *days* days, find the most
    recent published slideshow whose posted_at < trial starts_at.  Increment
    that slideshow's conversions count in slideshow_performance.

    Returns the number of conversions attributed.
    Does NOT commit — caller is responsible for committing.
    """
    client = _get_client()
    if client is None:
        return 0

    trials = client.get_recent_trials(days)
    if not trials:
        return 0

    attributed = 0
    for trial in trials:
        starts_at_raw = trial.get("starts_at")
        if not starts_at_raw:
            continue
        try:
            starts_at = datetime.fromisoformat(starts_at_raw.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue

        starts_at_str = starts_at.strftime("%Y-%m-%d %H:%M:%S")

        # Find the most recent published slideshow posted before this trial
        row = conn.execute(
            """SELECT id FROM slideshows
               WHERE posted_at IS NOT NULL
                 AND posted_at < ?
               ORDER BY posted_at DESC
               LIMIT 1""",
            (starts_at_str,),
        ).fetchone()

        if not row:
            continue

        slideshow_id = row["id"]

        # Upsert into slideshow_performance — increment conversions
        existing = conn.execute(
            "SELECT id, conversions FROM slideshow_performance WHERE slideshow_id = ?",
            (slideshow_id,),
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE slideshow_performance SET conversions = ? WHERE id = ?",
                (existing["conversions"] + 1, existing["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO slideshow_performance (slideshow_id, conversions) VALUES (?, ?)",
                (slideshow_id, 1),
            )

        attributed += 1

    return attributed


def diagnose_funnel(
    views_good: bool, conversions_good: bool, has_rc_data: bool
) -> str:
    """Return a diagnostic string based on a 2x2 framework.

    Quadrants:
        views_good + conversions_good  → SCALE
        views_good + !conversions_good → FIX CTA
        !views_good + conversions_good → FIX HOOKS
        !views_good + !conversions_good→ NEEDS WORK

    If *has_rc_data* is False, conversions_good is treated as unknown and
    the diagnosis is based solely on views.
    """
    if not has_rc_data:
        return "SCALE" if views_good else "FIX HOOKS"

    if views_good and conversions_good:
        return "SCALE"
    if views_good and not conversions_good:
        return "FIX CTA"
    if not views_good and conversions_good:
        return "FIX HOOKS"
    return "NEEDS WORK"
