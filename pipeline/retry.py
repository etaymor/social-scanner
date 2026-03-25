"""Shared retry-with-backoff helper."""

import logging
import time

log = logging.getLogger(__name__)


def retry_with_backoff(
    fn,
    max_retries: int,
    base_delay: float,
    *,
    non_retryable: tuple[type[Exception], ...] = (),
):
    """Call *fn* up to *max_retries* times with exponential backoff.

    *fn* is called with no arguments. It should raise on failure.
    Exceptions whose type is in *non_retryable* are re-raised immediately.
    Other exceptions are retried with exponential backoff.

    Returns the result of *fn* on success.
    """
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            return fn()
        except non_retryable:
            raise
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                log.warning(
                    "Attempt %d/%d failed: %s. Retrying in %ds...",
                    attempt + 1, max_retries, e, delay,
                )
                time.sleep(delay)
    raise last_error  # type: ignore[misc]
