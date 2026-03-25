"""Tests for image generation via OpenRouter + Gemini Flash."""

import base64
import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from PIL import Image

from pipeline.image_gen import (
    GeminiError,
    GeminiQuotaError,
    generate_image,
    generate_slideshow_images,
    _slugify,
)


# ---------------------------------------------------------------------------
# Helpers — build a tiny valid PNG in base64
# ---------------------------------------------------------------------------

def _make_test_png_b64(width=100, height=100, color="blue"):
    """Create a small valid PNG image and return its base64 string."""
    img = Image.new("RGB", (width, height), color)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


def _make_mock_response(b64=None, status_code=200, content_text="test"):
    """Build a mock requests.Response with the expected image structure."""
    if b64 is None:
        b64 = _make_test_png_b64()
    resp = MagicMock()
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "choices": [{
            "message": {
                "role": "assistant",
                "content": content_text,
                "images": [{
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{b64}",
                    },
                }],
            },
        }],
    }
    return resp


def _make_blocked_response():
    """Build a mock response with no images (content filtering)."""
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "choices": [{
            "message": {
                "role": "assistant",
                "content": "I cannot generate that image.",
                "images": [],
            },
        }],
    }
    return resp


def _make_402_response():
    """Build a mock 402 (credits exhausted) response."""
    resp = MagicMock()
    resp.status_code = 402
    return resp


def _make_500_response():
    """Build a mock 500 (server error) response."""
    import requests as req
    resp = MagicMock()
    resp.status_code = 500
    resp.raise_for_status.side_effect = req.HTTPError("Internal Server Error")
    return resp


SAMPLE_PLACES = [
    {"name": "Blue Mosque", "image_prompt": "Majestic blue-tiled mosque interior with domed ceiling"},
    {"name": "Grand Bazaar", "image_prompt": "Colorful covered market with hanging lanterns"},
    {"name": "Galata Tower", "image_prompt": "Medieval stone tower overlooking the Bosphorus"},
]


# ---------------------------------------------------------------------------
# Test: _slugify
# ---------------------------------------------------------------------------

class TestSlugify:
    def test_basic(self):
        assert _slugify("Blue Mosque") == "blue_mosque"

    def test_special_chars(self):
        assert _slugify("Café d'Or!") == "caf_d_or"

    def test_already_clean(self):
        assert _slugify("simple") == "simple"


# ---------------------------------------------------------------------------
# Test: generate_image
# ---------------------------------------------------------------------------

@patch("pipeline.image_gen.OPENROUTER_API_KEY", "test-key")
class TestGenerateImage:

    @patch("pipeline.image_gen.requests.post")
    def test_success_saves_png(self, mock_post, tmp_path):
        """Generated image is decoded from base64 and saved as a PNG file."""
        mock_post.return_value = _make_mock_response()
        out = tmp_path / "test.png"

        result = generate_image("a blue sky", out)

        assert result is True
        assert out.exists()
        assert out.stat().st_size > 0
        # Verify it's a valid PNG
        img = Image.open(out)
        assert img.format == "PNG"

    @patch("pipeline.image_gen.requests.post")
    def test_sends_correct_payload(self, mock_post, tmp_path):
        """Verify the request payload structure sent to OpenRouter."""
        mock_post.return_value = _make_mock_response()
        out = tmp_path / "test.png"

        generate_image("a mountain sunset", out)

        payload = mock_post.call_args[1]["json"]
        assert payload["model"] == "google/gemini-3.1-flash-image-preview"
        assert payload["modalities"] == ["image", "text"]
        assert payload["image_config"]["aspect_ratio"] == "9:16"
        assert payload["image_config"]["image_size"] == "2K"
        assert payload["messages"][0]["content"] == "a mountain sunset"

    @patch("pipeline.image_gen.requests.post")
    def test_reference_images_encoded(self, mock_post, tmp_path):
        """Reference images are base64-encoded and included in the request."""
        mock_post.return_value = _make_mock_response()

        # Create a reference image file
        ref_img = tmp_path / "ref.png"
        Image.new("RGB", (50, 50), "red").save(ref_img)
        out = tmp_path / "test.png"

        generate_image("based on reference", out, reference_images=[ref_img])

        payload = mock_post.call_args[1]["json"]
        content = payload["messages"][0]["content"]
        assert isinstance(content, list)
        assert content[0]["type"] == "image_url"
        assert content[0]["image_url"]["url"].startswith("data:image/png;base64,")
        assert content[1]["type"] == "text"
        assert content[1]["text"] == "based on reference"

    @patch("pipeline.image_gen.requests.post")
    def test_402_raises_quota_error(self, mock_post, tmp_path):
        """HTTP 402 immediately raises GeminiQuotaError (no retries)."""
        mock_post.return_value = _make_402_response()
        out = tmp_path / "test.png"

        with pytest.raises(GeminiQuotaError, match="credits exhausted"):
            generate_image("test", out)

        # Only one attempt — no retries
        assert mock_post.call_count == 1

    @patch("pipeline.image_gen.requests.post")
    def test_blocked_response_raises_gemini_error(self, mock_post, tmp_path):
        """Empty images array raises GeminiError for content filtering."""
        mock_post.return_value = _make_blocked_response()
        out = tmp_path / "test.png"

        with pytest.raises(GeminiError, match="Content filtered"):
            generate_image("test", out)

    @patch("pipeline.retry.time.sleep")
    @patch("pipeline.image_gen.requests.post")
    def test_retries_on_5xx(self, mock_post, mock_sleep, tmp_path):
        """Transient 500 errors are retried with exponential backoff."""
        mock_post.side_effect = [
            _make_500_response(),
            _make_mock_response(),
        ]
        out = tmp_path / "test.png"

        result = generate_image("test", out)

        assert result is True
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(2)  # base delay 2s * 2^0

    @patch("pipeline.retry.time.sleep")
    @patch("pipeline.image_gen.requests.post")
    def test_exhausts_retries(self, mock_post, mock_sleep, tmp_path):
        """After max retries, raises GeminiError."""
        mock_post.side_effect = [_make_500_response()] * 5
        out = tmp_path / "test.png"

        with pytest.raises(GeminiError, match="failed after"):
            generate_image("test", out)


# ---------------------------------------------------------------------------
# Test: generate_slideshow_images
# ---------------------------------------------------------------------------

@patch("pipeline.image_gen.OPENROUTER_API_KEY", "test-key")
class TestGenerateSlideshowImages:

    @patch("pipeline.image_gen.requests.post")
    def test_generates_correct_number_of_images(self, mock_post, tmp_path):
        """Hook + N location slides + CTA = N+2 images generated."""
        mock_post.return_value = _make_mock_response()

        result = generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "Epic hook image prompt",
        )

        # 3 places → 5 total (1 hook + 3 locations + 1 CTA)
        assert result["generated"] == 5
        assert result["skipped"] == 0
        assert result["failed"] == 0
        assert result["failed_slides"] == []
        assert mock_post.call_count == 5

        # Verify files exist
        assert (tmp_path / "slide_1_hook_raw.png").exists()
        assert (tmp_path / "slide_2_raw.png").exists()
        assert (tmp_path / "slide_3_raw.png").exists()
        assert (tmp_path / "slide_4_raw.png").exists()
        assert (tmp_path / "slide_5_cta_raw.png").exists()

    @patch("pipeline.image_gen.requests.post")
    def test_skips_existing_images_on_resume(self, mock_post, tmp_path):
        """Files >10KB are skipped on resume."""
        mock_post.return_value = _make_mock_response()

        # Pre-create slide_1_hook_raw.png with >10KB content
        hook_path = tmp_path / "slide_1_hook_raw.png"
        hook_path.write_bytes(b"\x89PNG" + b"\x00" * 11000)

        # Pre-create slide_2_raw.png with >10KB content
        slide2_path = tmp_path / "slide_2_raw.png"
        slide2_path.write_bytes(b"\x89PNG" + b"\x00" * 11000)

        result = generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "hook prompt",
        )

        # 2 skipped (hook + slide_2), 3 generated (slide_3, slide_4, CTA)
        assert result["skipped"] == 2
        assert result["generated"] == 3

    @patch("pipeline.image_gen.requests.post")
    def test_small_existing_files_are_regenerated(self, mock_post, tmp_path):
        """Files <=10KB are NOT skipped (treated as incomplete)."""
        mock_post.return_value = _make_mock_response()

        # Pre-create a small file (<= 10KB)
        hook_path = tmp_path / "slide_1_hook_raw.png"
        hook_path.write_bytes(b"\x89PNG" + b"\x00" * 100)

        result = generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "hook prompt",
        )

        assert result["generated"] == 5
        assert result["skipped"] == 0

    @patch("pipeline.image_gen.requests.post")
    def test_individual_slide_failure_continues(self, mock_post, tmp_path):
        """GeminiError on one slide doesn't stop the rest."""
        ok_resp = _make_mock_response()
        blocked_resp = _make_blocked_response()

        # Hook OK, slide 2 blocked, slide 3 OK, slide 4 OK, CTA OK
        mock_post.side_effect = [ok_resp, blocked_resp, ok_resp, ok_resp, ok_resp]

        result = generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "hook prompt",
        )

        assert result["generated"] == 4
        assert result["failed"] == 1
        assert result["failed_slides"] == [2]

    @patch("pipeline.image_gen.requests.post")
    def test_content_filtering_reports_failure(self, mock_post, tmp_path):
        """Blocked responses (empty images) are reported as failures."""
        mock_post.return_value = _make_blocked_response()

        result = generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "hook prompt",
        )

        assert result["failed"] == 5
        assert result["generated"] == 0

    @patch("pipeline.image_gen.requests.post")
    def test_quota_error_stops_immediately(self, mock_post, tmp_path):
        """GeminiQuotaError (402) stops the entire batch immediately."""
        ok_resp = _make_mock_response()
        quota_resp = _make_402_response()

        # Hook OK, then slide 2 hits 402
        mock_post.side_effect = [ok_resp, quota_resp]

        with pytest.raises(GeminiQuotaError):
            generate_slideshow_images(
                tmp_path, SAMPLE_PLACES, "hook prompt",
            )

        # Only 2 API calls made (hook + first location)
        assert mock_post.call_count == 2

    @patch("pipeline.image_gen.requests.post")
    def test_cta_prompt_includes_place_names(self, mock_post, tmp_path):
        """CTA slide prompt mentions all place names."""
        mock_post.return_value = _make_mock_response()

        generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "hook prompt",
        )

        # The last call is the CTA slide
        last_call_payload = mock_post.call_args_list[-1][1]["json"]
        cta_prompt = last_call_payload["messages"][0]["content"]
        assert "Blue Mosque" in cta_prompt
        assert "Grand Bazaar" in cta_prompt
        assert "Galata Tower" in cta_prompt

    @patch("pipeline.image_gen.requests.post")
    def test_cta_with_template_reference(self, mock_post, tmp_path):
        """CTA slide includes template image as reference when provided."""
        mock_post.return_value = _make_mock_response()

        # Create a CTA template image
        template = tmp_path / "cta_template.png"
        Image.new("RGB", (50, 50), "green").save(template)

        generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "hook prompt",
            cta_template_path=template,
        )

        # The last call (CTA) should have multipart content with image reference
        last_call_payload = mock_post.call_args_list[-1][1]["json"]
        content = last_call_payload["messages"][0]["content"]
        assert isinstance(content, list)
        assert content[0]["type"] == "image_url"
        assert content[1]["type"] == "text"

    @patch("pipeline.image_gen.requests.post")
    def test_prompts_json_saved(self, mock_post, tmp_path):
        """prompts.json is written with all slide prompts."""
        mock_post.return_value = _make_mock_response()

        generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "Epic hook image",
        )

        prompts_path = tmp_path / "prompts.json"
        assert prompts_path.exists()
        prompts = json.loads(prompts_path.read_text())

        # Should have entries for hook, 3 locations, and CTA
        assert len(prompts) == 5
        assert "slide_1_hook" in prompts
        assert prompts["slide_1_hook"] == "Epic hook image"

        # CTA key should exist
        cta_key = "slide_5_cta"
        assert cta_key in prompts

    @patch("pipeline.image_gen.requests.post")
    def test_output_images_are_valid_pngs(self, mock_post, tmp_path):
        """All generated slide images are valid PNG files."""
        mock_post.return_value = _make_mock_response()

        generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "hook prompt",
        )

        for png_file in tmp_path.glob("slide_*_raw.png"):
            img = Image.open(png_file)
            assert img.format == "PNG"

    @patch("pipeline.image_gen.requests.post")
    def test_location_prompts_have_style_suffix(self, mock_post, tmp_path):
        """Location slide prompts include the photography style suffix."""
        mock_post.return_value = _make_mock_response()

        generate_slideshow_images(
            tmp_path, SAMPLE_PLACES, "hook prompt",
        )

        # Check slide 2 (first location) prompt
        slide2_payload = mock_post.call_args_list[1][1]["json"]
        prompt_text = slide2_payload["messages"][0]["content"]
        assert "Shot on iPhone 15 Pro" in prompt_text
        assert "natural lighting" in prompt_text
        assert "shallow depth of field" in prompt_text

    @patch("pipeline.image_gen.requests.post")
    def test_empty_places_list(self, mock_post, tmp_path):
        """Works with no places — just hook + CTA."""
        mock_post.return_value = _make_mock_response()

        result = generate_slideshow_images(
            tmp_path, [], "hook prompt",
        )

        assert result["generated"] == 2  # hook + CTA
        assert result["skipped"] == 0
        assert mock_post.call_count == 2
        assert (tmp_path / "slide_1_hook_raw.png").exists()
        assert (tmp_path / "slide_2_cta_raw.png").exists()
