"""OpenRouter API wrapper with retry logic."""

import json
import logging

import requests

from config import (
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    OPENROUTER_MAX_RETRIES,
    OPENROUTER_MODEL,
    OPENROUTER_RETRY_BASE_DELAY,
)
from pipeline.retry import retry_with_backoff

log = logging.getLogger(__name__)


class LLMError(Exception):
    pass


class CreditsExhaustedError(LLMError):
    pass


def call_llm(prompt: str, *, system: str = None, model: str = None, temperature: float = 0.7) -> str:
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
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    payload = {
        "model": model or OPENROUTER_MODEL,
        "messages": messages,
        "temperature": temperature,
        "response_format": {"type": "json_object"},
    }

    def _do_call():
        resp = requests.post(
            OPENROUTER_BASE_URL, headers=headers, json=payload, timeout=120,
        )
        if resp.status_code == 402:
            raise CreditsExhaustedError(
                "OpenRouter credits exhausted (HTTP 402). Add credits and retry."
            )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()

    try:
        return retry_with_backoff(
            _do_call,
            max_retries=OPENROUTER_MAX_RETRIES,
            base_delay=OPENROUTER_RETRY_BASE_DELAY,
            non_retryable=(CreditsExhaustedError,),
        )
    except CreditsExhaustedError:
        raise
    except Exception as e:
        raise LLMError(f"LLM call failed after {OPENROUTER_MAX_RETRIES} retries: {e}") from e


def sanitize_text(text: str, max_length: int = 2000) -> str:
    """Strip control characters and limit length for safe LLM prompt insertion."""
    import re
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    return cleaned[:max_length]


def call_llm_json(prompt: str, **kwargs) -> list[object] | dict[str, object]:
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
