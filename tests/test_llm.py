"""Tests for LLM wrapper: retry logic, JSON parsing, error handling."""

import json
from unittest.mock import patch, MagicMock

import pytest
import requests

from pipeline.llm import call_llm, call_llm_json, LLMError, CreditsExhaustedError, sanitize_text


class TestSanitizeText:
    def test_strips_control_chars(self):
        assert sanitize_text("hello\x00world\x07") == "helloworld"

    def test_preserves_newlines_and_tabs(self):
        assert sanitize_text("line1\nline2\ttab") == "line1\nline2\ttab"

    def test_truncates_to_max_length(self):
        assert len(sanitize_text("a" * 5000, max_length=100)) == 100

    def test_empty_string(self):
        assert sanitize_text("") == ""


class TestCallLlmJson:
    @patch("pipeline.llm.call_llm")
    def test_clean_json(self, mock_llm):
        mock_llm.return_value = '{"results": [1, 2, 3]}'
        result = call_llm_json("test prompt")
        assert result == {"results": [1, 2, 3]}

    @patch("pipeline.llm.call_llm")
    def test_fenced_json(self, mock_llm):
        mock_llm.return_value = '```json\n{"results": [1]}\n```'
        result = call_llm_json("test prompt")
        assert result == {"results": [1]}

    @patch("pipeline.llm.call_llm")
    def test_json_with_surrounding_text(self, mock_llm):
        mock_llm.return_value = 'Here is the result: {"key": "value"} done.'
        result = call_llm_json("test prompt")
        assert result == {"key": "value"}

    @patch("pipeline.llm.call_llm")
    def test_json_array_extraction(self, mock_llm):
        mock_llm.return_value = 'Result: [1, 2, 3] end'
        result = call_llm_json("test prompt")
        assert result == [1, 2, 3]

    @patch("pipeline.llm.call_llm")
    def test_unparseable_raises(self, mock_llm):
        mock_llm.return_value = "not json at all"
        with pytest.raises(LLMError, match="Failed to parse"):
            call_llm_json("test prompt")

    @patch("pipeline.llm.call_llm")
    def test_passes_kwargs(self, mock_llm):
        mock_llm.return_value = '{"ok": true}'
        call_llm_json("prompt", system="sys", temperature=0.1)
        mock_llm.assert_called_once_with("prompt", system="sys", temperature=0.1)


@patch("pipeline.llm.OPENROUTER_API_KEY", "test-key")
class TestCallLlm:
    @patch("pipeline.llm.requests.post")
    def test_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "hello"}}]
        }
        mock_post.return_value = mock_resp

        result = call_llm("test")
        assert result == "hello"

    @patch("pipeline.llm.requests.post")
    def test_402_raises_credits_exhausted(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 402
        mock_post.return_value = mock_resp

        with pytest.raises(CreditsExhaustedError):
            call_llm("test")

    @patch("pipeline.llm.time.sleep")
    @patch("pipeline.llm.requests.post")
    def test_retry_on_transient_error(self, mock_post, mock_sleep):
        fail_resp = MagicMock()
        fail_resp.status_code = 500
        fail_resp.raise_for_status.side_effect = requests.HTTPError("Server Error")

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {
            "choices": [{"message": {"content": "ok"}}]
        }

        mock_post.side_effect = [fail_resp, ok_resp]
        result = call_llm("test")
        assert result == "ok"
        assert mock_post.call_count == 2

    @patch("pipeline.llm.time.sleep")
    @patch("pipeline.llm.requests.post")
    def test_429_respects_retry_after(self, mock_post, mock_sleep):
        rate_resp = MagicMock()
        rate_resp.status_code = 429
        rate_resp.headers = {"retry-after": "5"}

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {
            "choices": [{"message": {"content": "ok"}}]
        }

        mock_post.side_effect = [rate_resp, ok_resp]
        result = call_llm("test")
        assert result == "ok"
        mock_sleep.assert_called_with(5)

    @patch("pipeline.llm.requests.post")
    def test_system_message_sent(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "hi"}}]
        }
        mock_post.return_value = mock_resp

        call_llm("user msg", system="sys msg")
        payload = mock_post.call_args[1]["json"]
        assert payload["messages"][0] == {"role": "system", "content": "sys msg"}
        assert payload["messages"][1] == {"role": "user", "content": "user msg"}
