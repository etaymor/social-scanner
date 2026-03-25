"""Tests for Postiz posting integration: upload, post creation, orchestration."""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import pytest
import requests

from pipeline.posting import (
    upload_image,
    create_tiktok_post,
    post_slideshow,
    PostingError,
    PostingAuthError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_slide_files(tmp_path: Path, count: int = 3) -> list[Path]:
    """Create dummy slide_N.png files and return them sorted."""
    paths = []
    for i in range(1, count + 1):
        p = tmp_path / f"slide_{i}.png"
        p.write_bytes(b"\x89PNG dummy")
        paths.append(p)
    return paths


def _ok_upload_response(index: int) -> MagicMock:
    """Return a mock response for a successful upload."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"id": f"img-{index}", "path": f"/uploads/img-{index}.png"}
    return resp


def _ok_post_response(post_id: str = "post-abc123") -> MagicMock:
    """Return a mock response for a successful post creation."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"id": post_id}
    return resp


# ---------------------------------------------------------------------------
# upload_image
# ---------------------------------------------------------------------------

class TestUploadImage:
    @patch("pipeline.posting.requests.post")
    def test_success(self, mock_post, tmp_path):
        img = tmp_path / "test.png"
        img.write_bytes(b"\x89PNG data")

        mock_post.return_value = _ok_upload_response(1)

        result = upload_image("key-123", img)
        assert result == {"id": "img-1", "path": "/uploads/img-1.png"}

        # Verify auth header
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["headers"]["Authorization"] == "Bearer key-123"

    @patch("pipeline.posting.requests.post")
    def test_401_raises_auth_error(self, mock_post, tmp_path):
        img = tmp_path / "test.png"
        img.write_bytes(b"\x89PNG data")

        resp = MagicMock()
        resp.status_code = 401
        resp.text = "Unauthorized"
        mock_post.return_value = resp

        with pytest.raises(PostingAuthError, match="auth failed"):
            upload_image("bad-key", img)

    @patch("pipeline.posting.requests.post")
    def test_403_raises_auth_error(self, mock_post, tmp_path):
        img = tmp_path / "test.png"
        img.write_bytes(b"\x89PNG data")

        resp = MagicMock()
        resp.status_code = 403
        resp.text = "Forbidden"
        mock_post.return_value = resp

        with pytest.raises(PostingAuthError, match="auth failed"):
            upload_image("bad-key", img)

    @patch("pipeline.posting.time.sleep")
    @patch("pipeline.posting.requests.post")
    def test_retries_on_5xx(self, mock_post, mock_sleep, tmp_path):
        img = tmp_path / "test.png"
        img.write_bytes(b"\x89PNG data")

        fail_resp = MagicMock()
        fail_resp.status_code = 500
        fail_resp.text = "Internal Server Error"

        mock_post.side_effect = [fail_resp, _ok_upload_response(1)]

        result = upload_image("key-123", img)
        assert result["id"] == "img-1"
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(2)  # base delay * 2^0

    @patch("pipeline.posting.time.sleep")
    @patch("pipeline.posting.requests.post")
    def test_retries_exhausted_raises(self, mock_post, mock_sleep, tmp_path):
        img = tmp_path / "test.png"
        img.write_bytes(b"\x89PNG data")

        fail_resp = MagicMock()
        fail_resp.status_code = 502

        mock_post.return_value = fail_resp

        with pytest.raises(PostingError, match="failed after 3 attempts"):
            upload_image("key-123", img)

        assert mock_post.call_count == 3  # initial + 2 retries

    @patch("pipeline.posting.requests.post")
    def test_4xx_raises_posting_error(self, mock_post, tmp_path):
        img = tmp_path / "test.png"
        img.write_bytes(b"\x89PNG data")

        resp = MagicMock()
        resp.status_code = 422
        resp.text = "Unprocessable Entity"
        mock_post.return_value = resp

        with pytest.raises(PostingError, match="Upload failed"):
            upload_image("key-123", img)


# ---------------------------------------------------------------------------
# create_tiktok_post
# ---------------------------------------------------------------------------

class TestCreateTiktokPost:
    @patch("pipeline.posting.requests.post")
    def test_success(self, mock_post):
        mock_post.return_value = _ok_post_response("post-xyz")

        post_id = create_tiktok_post(
            "key-123", "integ-1", ["/uploads/a.png", "/uploads/b.png"], "My caption"
        )
        assert post_id == "post-xyz"

        # Verify payload shape
        payload = mock_post.call_args[1]["json"]
        assert payload["type"] == "carousel"
        assert payload["integration_id"] == "integ-1"
        assert payload["content"] == "My caption"
        assert payload["media"] == ["/uploads/a.png", "/uploads/b.png"]
        assert payload["settings"]["privacy_level"] == "SELF_ONLY"
        assert payload["settings"]["autoAddMusic"] == "no"
        assert payload["settings"]["video_made_with_ai"] is True

    @patch("pipeline.posting.requests.post")
    def test_401_raises_auth_error(self, mock_post):
        resp = MagicMock()
        resp.status_code = 401
        resp.text = "Unauthorized"
        mock_post.return_value = resp

        with pytest.raises(PostingAuthError):
            create_tiktok_post("bad-key", "integ-1", ["/a.png"], "caption")

    @patch("pipeline.posting.time.sleep")
    @patch("pipeline.posting.requests.post")
    def test_retries_on_5xx(self, mock_post, mock_sleep):
        fail_resp = MagicMock()
        fail_resp.status_code = 503

        mock_post.side_effect = [fail_resp, _ok_post_response("post-ok")]

        post_id = create_tiktok_post("key-123", "integ-1", ["/a.png"], "caption")
        assert post_id == "post-ok"
        assert mock_post.call_count == 2


# ---------------------------------------------------------------------------
# post_slideshow (orchestrator)
# ---------------------------------------------------------------------------

@patch("pipeline.posting.config")
class TestPostSlideshow:
    def _setup_config(self, mock_config):
        mock_config.POSTIZ_API_KEY = "test-api-key"
        mock_config.POSTIZ_BASE_URL = "https://api.postiz.com/public/v1"
        mock_config.POSTIZ_TIKTOK_INTEGRATION_ID = "integ-tiktok"
        mock_config.POSTIZ_UPLOAD_DELAY = 1.5

    @patch("pipeline.posting.create_tiktok_post")
    @patch("pipeline.posting.upload_image")
    def test_uploads_correct_number_in_order(
        self, mock_upload, mock_create, mock_config, tmp_path
    ):
        self._setup_config(mock_config)
        _make_slide_files(tmp_path, count=4)

        mock_upload.side_effect = [
            {"id": f"img-{i}", "path": f"/uploads/img-{i}.png"}
            for i in range(1, 5)
        ]
        mock_create.return_value = "post-123"

        post_slideshow(tmp_path, "Test caption")

        assert mock_upload.call_count == 4
        # Verify order: slide_1, slide_2, slide_3, slide_4
        for i, c in enumerate(mock_upload.call_args_list, start=1):
            assert c[0][0] == "test-api-key"
            assert c[0][1].name == f"slide_{i}.png"

    @patch("pipeline.posting.create_tiktok_post")
    @patch("pipeline.posting.upload_image")
    def test_creates_post_with_correct_settings(
        self, mock_upload, mock_create, mock_config, tmp_path
    ):
        self._setup_config(mock_config)
        _make_slide_files(tmp_path, count=2)

        mock_upload.side_effect = [
            {"id": "img-1", "path": "/uploads/img-1.png"},
            {"id": "img-2", "path": "/uploads/img-2.png"},
        ]
        mock_create.return_value = "post-456"

        post_slideshow(tmp_path, "My caption #travel")

        mock_create.assert_called_once_with(
            "test-api-key",
            "integ-tiktok",
            ["/uploads/img-1.png", "/uploads/img-2.png"],
            "My caption #travel",
        )

    @patch("pipeline.posting.create_tiktok_post")
    @patch("pipeline.posting.upload_image")
    def test_saves_post_meta_json(
        self, mock_upload, mock_create, mock_config, tmp_path
    ):
        self._setup_config(mock_config)
        _make_slide_files(tmp_path, count=1)

        mock_upload.return_value = {"id": "img-1", "path": "/uploads/img-1.png"}
        mock_create.return_value = "post-meta-test"

        meta = post_slideshow(tmp_path, "caption")

        assert meta.postiz_post_id == "post-meta-test"
        assert meta.platform == "tiktok"
        assert meta.privacy_level == "SELF_ONLY"

        # Verify file was written
        meta_path = tmp_path / "post_meta.json"
        assert meta_path.exists()
        saved = json.loads(meta_path.read_text())
        assert saved["postiz_post_id"] == "post-meta-test"

    @patch("pipeline.posting.create_tiktok_post")
    @patch("pipeline.posting.upload_image")
    def test_upload_failure_propagates(
        self, mock_upload, mock_create, mock_config, tmp_path
    ):
        self._setup_config(mock_config)
        _make_slide_files(tmp_path, count=3)

        # First upload succeeds, second fails
        mock_upload.side_effect = [
            {"id": "img-1", "path": "/uploads/img-1.png"},
            PostingError("Upload of slide_2.png failed after 3 attempts"),
        ]

        with pytest.raises(PostingError, match="slide_2"):
            post_slideshow(tmp_path, "caption")

        # Post should not have been created
        mock_create.assert_not_called()

    @patch("pipeline.posting.create_tiktok_post")
    @patch("pipeline.posting.upload_image")
    def test_skips_if_post_meta_exists(
        self, mock_upload, mock_create, mock_config, tmp_path
    ):
        self._setup_config(mock_config)
        _make_slide_files(tmp_path, count=2)

        # Write existing post_meta.json
        existing = {
            "postiz_post_id": "already-posted",
            "posted_at": "2026-03-20T00:00:00+00:00",
            "platform": "tiktok",
            "privacy_level": "SELF_ONLY",
        }
        (tmp_path / "post_meta.json").write_text(json.dumps(existing))

        meta = post_slideshow(tmp_path, "new caption")

        assert meta.postiz_post_id == "already-posted"
        mock_upload.assert_not_called()
        mock_create.assert_not_called()

    @patch("pipeline.posting.time.sleep")
    @patch("pipeline.posting.create_tiktok_post")
    @patch("pipeline.posting.upload_image")
    def test_rate_limit_delay_between_uploads(
        self, mock_upload, mock_create, mock_sleep, mock_config, tmp_path
    ):
        self._setup_config(mock_config)
        _make_slide_files(tmp_path, count=3)

        mock_upload.side_effect = [
            {"id": f"img-{i}", "path": f"/uploads/img-{i}.png"}
            for i in range(1, 4)
        ]
        mock_create.return_value = "post-delay"

        post_slideshow(tmp_path, "caption")

        # Should sleep between uploads: after 1st and 2nd, but NOT after 3rd
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(1.5)
