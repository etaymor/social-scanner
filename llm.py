"""OpenRouter API wrapper with retry logic."""

import json
import logging
import time

import requests

from config import (
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    OPENROUTER_MAX_RETRIES,
    OPENROUTER_MODEL,
    OPENROUTER_RETRY_BASE_DELAY,
)

log = logging.getLogger(__name__)


class LLMError(Exception):
    pass


class CreditsExhaustedError(LLMError):
    pass


def call_llm(prompt: str, *, model: str = None, temperature: float = 0.7) -> str:
    """Send a prompt to OpenRouter and return the text response.

    Retries on transient errors with exponential backoff.
    Raises CreditsExhaustedError on 402 (no point retrying).
    """
    if not OPENROUTER_API_KEY:
        raise LLMError("OPENROUTER_API_KEY not set in environment")

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model or OPENROUTER_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "response_format": {"type": "json_object"},
    }

    last_error = None
    for attempt in range(OPENROUTER_MAX_RETRIES):
        try:
            resp = requests.post(
                OPENROUTER_BASE_URL, headers=headers, json=payload, timeout=120,
            )

            if resp.status_code == 402:
                raise CreditsExhaustedError(
                    "OpenRouter credits exhausted (HTTP 402). Add credits and retry."
                )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("retry-after", OPENROUTER_RETRY_BASE_DELAY * (2 ** attempt)))
                log.warning("Rate limited, waiting %ds before retry", retry_after)
                time.sleep(retry_after)
                continue

            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            return content.strip()

        except CreditsExhaustedError:
            raise
        except (requests.RequestException, KeyError, IndexError) as e:
            last_error = e
            if attempt < OPENROUTER_MAX_RETRIES - 1:
                delay = OPENROUTER_RETRY_BASE_DELAY * (2 ** attempt)
                log.warning("LLM call failed (attempt %d/%d): %s. Retrying in %ds...",
                            attempt + 1, OPENROUTER_MAX_RETRIES, e, delay)
                time.sleep(delay)

    raise LLMError(f"LLM call failed after {OPENROUTER_MAX_RETRIES} retries: {last_error}")


def call_llm_json(prompt: str, **kwargs) -> list | dict:
    """Call the LLM and parse the response as JSON.

    Handles cases where the LLM wraps JSON in markdown code fences.
    """
    raw = call_llm(prompt, **kwargs)

    # Strip markdown code fences if present
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last lines (``` markers)
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to extract JSON array or object from the response
        for start_char, end_char in [("[", "]"), ("{", "}")]:
            start = text.find(start_char)
            end = text.rfind(end_char)
            if start != -1 and end != -1 and end > start:
                try:
                    return json.loads(text[start:end + 1])
                except json.JSONDecodeError:
                    continue
        raise LLMError(f"Failed to parse LLM response as JSON: {raw[:200]}")
