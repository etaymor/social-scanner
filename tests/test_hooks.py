"""Tests for pipeline.hooks — hook generation for listicle and story formats."""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.hooks import generate_hook, HOOK_TEMPLATES, _IMAGE_PROMPT_TEMPLATE
from pipeline.llm import LLMError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_llm_response(hook_text: str, city: str, caption: str | None = None) -> str:
    """Build a JSON string mimicking a successful LLM response."""
    return json.dumps({
        "hook_text": hook_text,
        "hook_image_prompt": f"A stunning establishing shot of {city}, cinematic",
        "caption": caption or (
            f"{city} has amazing hidden spots — I found these on Atlasi "
            f"#travel #hiddengems #atlasi"
        ),
    })


# ---------------------------------------------------------------------------
# Listicle format
# ---------------------------------------------------------------------------

class TestListicleHook:
    @patch("pipeline.hooks.call_llm_json")
    def test_includes_city_name_and_slide_count(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "5 places in Tokyo\ntourists never find",
            "hook_image_prompt": "A stunning establishing shot of Tokyo",
            "caption": "Tokyo is unreal — found these on Atlasi #tokyo #atlasi #travel",
        }
        result = generate_hook("Tokyo", 5, "listicle")
        assert "Tokyo" in result["hook_text"]
        assert "5" in result["hook_text"]

    @patch("pipeline.hooks.call_llm_json")
    def test_hook_text_contains_line_breaks(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "5 places in Tokyo\ntourists never find",
            "hook_image_prompt": "A stunning establishing shot of Tokyo",
            "caption": "Found these on Atlasi #tokyo #atlasi",
        }
        result = generate_hook("Tokyo", 5, "listicle")
        assert "\n" in result["hook_text"]

    @patch("pipeline.hooks.call_llm_json")
    def test_returns_all_three_fields(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "5 spots in Paris\nyou need to visit",
            "hook_image_prompt": "A stunning establishing shot of Paris",
            "caption": "Paris gems via Atlasi #paris #atlasi #travel",
        }
        result = generate_hook("Paris", 5, "listicle")
        assert "hook_text" in result
        assert "hook_image_prompt" in result
        assert "caption" in result
        # All values should be non-empty strings
        for key in ("hook_text", "hook_image_prompt", "caption"):
            assert isinstance(result[key], str)
            assert len(result[key]) > 0

    @patch("pipeline.hooks.call_llm_json")
    def test_caption_includes_atlasi_and_hashtags(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "7 hidden gems in Rome\nlocals don't share",
            "hook_image_prompt": "A stunning establishing shot of Rome",
            "caption": "Rome blew my mind — found these on Atlasi #rome #atlasi #travel",
        }
        result = generate_hook("Rome", 7, "listicle")
        caption_lower = result["caption"].lower()
        assert "atlasi" in caption_lower
        assert "#" in result["caption"]

    @patch("pipeline.hooks.call_llm_json")
    def test_category_passed_to_llm(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "5 cafes in Tokyo\nlocals don't share",
            "hook_image_prompt": "A stunning shot of Tokyo cafe district",
            "caption": "Tokyo cafes via Atlasi #tokyofood #atlasi",
        }
        generate_hook("Tokyo", 5, "listicle", category="food_and_drink")

        # Verify the prompt passed to call_llm_json includes the category
        call_args = mock_llm.call_args
        prompt = call_args[0][0]  # first positional arg
        assert "food_and_drink" in prompt

    @patch("pipeline.hooks.call_llm_json")
    def test_no_category_omits_category_from_prompt(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "5 spots in Tokyo\nyou need to visit",
            "hook_image_prompt": "Shot of Tokyo",
            "caption": "Atlasi finds #atlasi",
        }
        generate_hook("Tokyo", 5, "listicle", category=None)

        prompt = mock_llm.call_args[0][0]
        assert "Category:" not in prompt


# ---------------------------------------------------------------------------
# Story format
# ---------------------------------------------------------------------------

class TestStoryHook:
    @patch("pipeline.hooks.call_llm_json")
    def test_story_hook_returns_all_three_fields(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "I showed my mom\nwhat tourists NEVER\nfind in Tokyo",
            "hook_image_prompt": "A stunning establishing shot of Tokyo at golden hour",
            "caption": "My mom couldn't believe Tokyo — Atlasi found these #tokyo #atlasi",
        }
        result = generate_hook("Tokyo", 5, "story")
        assert "hook_text" in result
        assert "hook_image_prompt" in result
        assert "caption" in result
        for key in ("hook_text", "hook_image_prompt", "caption"):
            assert isinstance(result[key], str)
            assert len(result[key]) > 0

    @patch("pipeline.hooks.call_llm_json")
    def test_story_hook_has_line_breaks(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "My local friend showed me\nthe REAL side of Paris",
            "hook_image_prompt": "A stunning establishing shot of Paris",
            "caption": "Paris like you've never seen — Atlasi #paris #atlasi #travel",
        }
        result = generate_hook("Paris", 5, "story")
        assert "\n" in result["hook_text"]

    @patch("pipeline.hooks.call_llm_json")
    def test_story_prompt_uses_person_conflict_pattern(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "Nobody told me about\nthese spots in Barcelona",
            "hook_image_prompt": "Shot of Barcelona",
            "caption": "Found on Atlasi #atlasi",
        }
        generate_hook("Barcelona", 6, "story")

        call_args = mock_llm.call_args
        system = call_args[1]["system"]
        assert "person+conflict" in system.lower() or "person" in system.lower()

    @patch("pipeline.hooks.call_llm_json")
    def test_story_category_passed_to_llm(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "Nobody told me about\nthese bars in Berlin",
            "hook_image_prompt": "Shot of Berlin",
            "caption": "Atlasi nightlife gems #atlasi",
        }
        generate_hook("Berlin", 4, "story", category="nightlife")

        prompt = mock_llm.call_args[0][0]
        assert "nightlife" in prompt

    @patch("pipeline.hooks.call_llm_json")
    def test_story_caption_includes_atlasi(self, mock_llm):
        mock_llm.return_value = {
            "hook_text": "I showed my friend\nwhat tourists miss in Lisbon",
            "hook_image_prompt": "Shot of Lisbon",
            "caption": "Lisbon is incredible — I mapped it all on Atlasi #lisbon #atlasi",
        }
        result = generate_hook("Lisbon", 5, "story")
        assert "atlasi" in result["caption"].lower()


# ---------------------------------------------------------------------------
# Fallback on LLM failure
# ---------------------------------------------------------------------------

class TestFallback:
    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("API down"))
    def test_listicle_fallback_does_not_raise(self, mock_llm):
        result = generate_hook("Tokyo", 5, "listicle")
        # Should not raise
        assert isinstance(result, dict)

    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("API down"))
    def test_listicle_fallback_returns_all_fields(self, mock_llm):
        result = generate_hook("Tokyo", 5, "listicle")
        assert "hook_text" in result
        assert "hook_image_prompt" in result
        assert "caption" in result
        for key in ("hook_text", "hook_image_prompt", "caption"):
            assert isinstance(result[key], str)
            assert len(result[key]) > 0

    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("API down"))
    def test_listicle_fallback_contains_city_and_count(self, mock_llm):
        result = generate_hook("Tokyo", 5, "listicle")
        assert "Tokyo" in result["hook_text"]
        assert "5" in result["hook_text"]

    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("API down"))
    def test_listicle_fallback_has_line_breaks(self, mock_llm):
        result = generate_hook("Tokyo", 5, "listicle")
        assert "\n" in result["hook_text"]

    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("API down"))
    def test_listicle_fallback_caption_has_atlasi(self, mock_llm):
        result = generate_hook("Tokyo", 5, "listicle")
        assert "atlasi" in result["caption"].lower()

    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("API down"))
    def test_story_fallback_does_not_raise(self, mock_llm):
        result = generate_hook("Paris", 5, "story")
        assert isinstance(result, dict)

    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("API down"))
    def test_story_fallback_returns_all_fields(self, mock_llm):
        result = generate_hook("Paris", 5, "story")
        assert "hook_text" in result
        assert "hook_image_prompt" in result
        assert "caption" in result

    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("API down"))
    def test_story_fallback_contains_city(self, mock_llm):
        result = generate_hook("Paris", 5, "story")
        assert "Paris" in result["hook_text"]

    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("API down"))
    def test_story_fallback_has_line_breaks(self, mock_llm):
        result = generate_hook("Paris", 5, "story")
        assert "\n" in result["hook_text"]

    @patch("pipeline.hooks.call_llm_json", side_effect=LLMError("timeout"))
    def test_story_fallback_caption_has_atlasi(self, mock_llm):
        result = generate_hook("Paris", 5, "story")
        assert "atlasi" in result["caption"].lower()


# ---------------------------------------------------------------------------
# Invalid LLM response triggers fallback
# ---------------------------------------------------------------------------

class TestInvalidLLMResponse:
    @patch("pipeline.hooks.call_llm_json")
    def test_missing_field_triggers_fallback(self, mock_llm):
        """LLM returns JSON but missing hook_image_prompt."""
        mock_llm.return_value = {
            "hook_text": "5 spots in Tokyo",
            # hook_image_prompt missing
            "caption": "Atlasi #atlasi",
        }
        result = generate_hook("Tokyo", 5, "listicle")
        # Should still get a valid result via fallback
        assert "hook_text" in result
        assert "hook_image_prompt" in result
        assert "caption" in result
        # Fallback should have Tokyo in it
        assert "Tokyo" in result["hook_text"]

    @patch("pipeline.hooks.call_llm_json")
    def test_empty_field_triggers_fallback(self, mock_llm):
        """LLM returns JSON but with empty hook_text."""
        mock_llm.return_value = {
            "hook_text": "",
            "hook_image_prompt": "Shot of Tokyo",
            "caption": "Atlasi #atlasi",
        }
        result = generate_hook("Tokyo", 5, "listicle")
        # Fallback kicks in
        assert len(result["hook_text"]) > 0
        assert "Tokyo" in result["hook_text"]

    @patch("pipeline.hooks.call_llm_json")
    def test_non_dict_response_triggers_fallback(self, mock_llm):
        """LLM returns a list instead of dict."""
        mock_llm.return_value = ["not", "a", "dict"]
        result = generate_hook("Tokyo", 5, "story")
        assert isinstance(result, dict)
        assert "hook_text" in result
        assert "Tokyo" in result["hook_text"]
