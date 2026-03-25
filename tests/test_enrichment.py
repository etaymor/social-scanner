"""Tests for the LLM enrichment module (neighborhood + image_prompt)."""

from unittest.mock import patch

from pipeline import db
from pipeline.enrichment import _extract_results, _needs_enrichment, enrich_places

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _insert_place(
    conn,
    city_id,
    name,
    place_type="restaurant",
    category="food_and_drink",
    sample_caption="A great spot",
    neighborhood=None,
    image_prompt=None,
):
    """Insert a place and return its id."""
    cur = conn.execute(
        """INSERT INTO places (city_id, name, type, category, sample_caption,
                               neighborhood, image_prompt)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (city_id, name, place_type, category, sample_caption, neighborhood, image_prompt),
    )
    conn.commit()
    return cur.lastrowid


def _make_llm_response(enrichments: list[dict]) -> dict:
    """Build a dict mimicking what call_llm_json returns."""
    return {"results": enrichments}


def _get_place(conn, place_id):
    """Fetch a place row by id."""
    return conn.execute(
        "SELECT * FROM places WHERE id = ?",
        (place_id,),
    ).fetchone()


# ---------------------------------------------------------------------------
# Unit tests for _needs_enrichment
# ---------------------------------------------------------------------------


class TestNeedsEnrichment:
    def test_missing_both(self, conn, city_id):
        pid = _insert_place(conn, city_id, "Bare Place")
        place = _get_place(conn, pid)
        assert _needs_enrichment(place) is True

    def test_missing_neighborhood(self, conn, city_id):
        pid = _insert_place(conn, city_id, "Partial A", image_prompt="some prompt")
        place = _get_place(conn, pid)
        assert _needs_enrichment(place) is True

    def test_missing_image_prompt(self, conn, city_id):
        pid = _insert_place(conn, city_id, "Partial B", neighborhood="Beyoglu")
        place = _get_place(conn, pid)
        assert _needs_enrichment(place) is True

    def test_has_both(self, conn, city_id):
        pid = _insert_place(
            conn, city_id, "Complete", neighborhood="Beyoglu", image_prompt="A warm cafe"
        )
        place = _get_place(conn, pid)
        assert _needs_enrichment(place) is False


# ---------------------------------------------------------------------------
# Unit tests for _extract_results
# ---------------------------------------------------------------------------


class TestExtractResults:
    def test_dict_with_results_key(self):
        parsed = {"results": [{"place_id": 1, "neighborhood": "X", "image_prompt": "Y"}]}
        result = _extract_results(parsed)
        assert len(result) == 1
        assert result[0]["place_id"] == 1

    def test_list_input(self):
        parsed = [{"place_id": 1, "neighborhood": "X", "image_prompt": "Y"}]
        result = _extract_results(parsed)
        assert len(result) == 1

    def test_empty_dict_returns_empty(self):
        result = _extract_results({})
        assert result == []

    def test_non_list_results_returns_empty(self):
        result = _extract_results({"results": "not a list"})
        assert result == []

    def test_string_input_returns_empty(self):
        result = _extract_results("not a dict or list")
        assert result == []


# ---------------------------------------------------------------------------
# Integration tests for enrich_places
# ---------------------------------------------------------------------------


class TestEnrichPlaces:
    @patch("pipeline.enrichment.call_llm_json")
    def test_skips_already_enriched_places(self, mock_llm, conn, city_id):
        """Places with existing enrichment (both fields) should be skipped entirely."""
        _insert_place(
            conn,
            city_id,
            "Complete Place",
            neighborhood="Sultanahmet",
            image_prompt="A grand mosque",
        )

        places = db.get_all_places(conn, city_id)
        result = enrich_places(conn, places, "Istanbul")

        assert result == 0
        mock_llm.assert_not_called()

    @patch("pipeline.enrichment.call_llm_json")
    def test_enriches_places_missing_fields(self, mock_llm, conn, city_id):
        """Places missing either neighborhood or image_prompt should be enriched."""
        pid1 = _insert_place(conn, city_id, "Place A")  # missing both
        pid2 = _insert_place(
            conn, city_id, "Place B", neighborhood="Beyoglu"
        )  # missing image_prompt

        mock_llm.return_value = _make_llm_response(
            [
                {
                    "place_id": pid1,
                    "neighborhood": "Kadikoy",
                    "image_prompt": "A vibrant market scene",
                },
                {"place_id": pid2, "neighborhood": "Beyoglu", "image_prompt": "A cozy rooftop bar"},
            ]
        )

        places = db.get_all_places(conn, city_id)
        result = enrich_places(conn, places, "Istanbul")

        assert result == 2
        mock_llm.assert_called_once()

        place_a = _get_place(conn, pid1)
        assert place_a["neighborhood"] == "Kadikoy"
        assert place_a["image_prompt"] == "A vibrant market scene"

        place_b = _get_place(conn, pid2)
        assert place_b["neighborhood"] == "Beyoglu"
        assert place_b["image_prompt"] == "A cozy rooftop bar"

    @patch("pipeline.enrichment.call_llm_json")
    def test_handles_llm_failure_gracefully(self, mock_llm, conn, city_id):
        """LLM failure for a batch should log error and continue with remaining batches."""
        from pipeline.llm import LLMError

        # Create 15 places -> 2 batches (10 + 5)
        pids = []
        for i in range(15):
            pid = _insert_place(conn, city_id, f"Place {i}")
            pids.append(pid)

        # First batch fails, second batch succeeds
        mock_llm.side_effect = [
            LLMError("Simulated failure"),
            _make_llm_response(
                [
                    {
                        "place_id": pids[j],
                        "neighborhood": f"District {j}",
                        "image_prompt": f"Description {j}",
                    }
                    for j in range(10, 15)
                ]
            ),
        ]

        places = db.get_all_places(conn, city_id)
        result = enrich_places(conn, places, "Istanbul")

        # Only second batch (5 places) should be enriched
        assert result == 5
        assert mock_llm.call_count == 2

        # First batch places remain unenriched
        for pid in pids[:10]:
            place = _get_place(conn, pid)
            assert place["neighborhood"] is None
            assert place["image_prompt"] is None

        # Second batch places are enriched
        for pid in pids[10:15]:
            place = _get_place(conn, pid)
            assert place["neighborhood"] is not None
            assert place["image_prompt"] is not None

    @patch("pipeline.enrichment.call_llm_json")
    def test_partial_batch_processes_correctly(self, mock_llm, conn, city_id):
        """A batch with fewer than 10 places should process correctly."""
        pids = []
        for i in range(3):
            pid = _insert_place(conn, city_id, f"Small Batch Place {i}")
            pids.append(pid)

        mock_llm.return_value = _make_llm_response(
            [
                {"place_id": pids[j], "neighborhood": f"Area {j}", "image_prompt": f"Visual {j}"}
                for j in range(3)
            ]
        )

        places = db.get_all_places(conn, city_id)
        result = enrich_places(conn, places, "Istanbul")

        assert result == 3
        mock_llm.assert_called_once()

        for i, pid in enumerate(pids):
            place = _get_place(conn, pid)
            assert place["neighborhood"] == f"Area {i}"
            assert place["image_prompt"] == f"Visual {i}"

    @patch("pipeline.enrichment.call_llm_json")
    def test_idempotent_no_duplicate_calls(self, mock_llm, conn, city_id):
        """Running enrichment twice should not make duplicate LLM calls."""
        pid = _insert_place(conn, city_id, "Idempotent Place")

        mock_llm.return_value = _make_llm_response(
            [
                {"place_id": pid, "neighborhood": "Karakoy", "image_prompt": "A scenic dock"},
            ]
        )

        # First run
        places = db.get_all_places(conn, city_id)
        result1 = enrich_places(conn, places, "Istanbul")
        assert result1 == 1
        assert mock_llm.call_count == 1

        # Second run — place is now enriched, should skip
        places = db.get_all_places(conn, city_id)
        result2 = enrich_places(conn, places, "Istanbul")
        assert result2 == 0
        # No additional LLM call
        assert mock_llm.call_count == 1

    @patch("pipeline.enrichment.call_llm_json")
    def test_db_committed_after_each_batch(self, mock_llm, conn, city_id):
        """DB should be committed after each batch to preserve progress."""
        # Create 15 places -> 2 batches
        pids = []
        for i in range(15):
            pid = _insert_place(conn, city_id, f"Commit Place {i}")
            pids.append(pid)

        call_count = 0

        def side_effect_fn(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            # On the second batch call, check that the first batch was already committed
            if call_count == 2:
                # Verify first batch places are already enriched in DB
                for pid in pids[:10]:
                    place = _get_place(conn, pid)
                    assert place["neighborhood"] is not None, (
                        f"Place {pid} should have been committed after batch 1"
                    )
                    assert place["image_prompt"] is not None

            batch_pids = pids[:10] if call_count == 1 else pids[10:15]
            return _make_llm_response(
                [
                    {"place_id": pid, "neighborhood": f"N-{pid}", "image_prompt": f"Desc-{pid}"}
                    for pid in batch_pids
                ]
            )

        mock_llm.side_effect = side_effect_fn

        places = db.get_all_places(conn, city_id)
        result = enrich_places(conn, places, "Istanbul")

        assert result == 15
        assert mock_llm.call_count == 2

    @patch("pipeline.enrichment.call_llm_json")
    def test_skips_places_with_missing_enrichment_data(self, mock_llm, conn, city_id):
        """Places for which the LLM returns incomplete data should be skipped."""
        pid1 = _insert_place(conn, city_id, "Good Data")
        pid2 = _insert_place(conn, city_id, "Missing Neighborhood")
        pid3 = _insert_place(conn, city_id, "Missing Image Prompt")

        mock_llm.return_value = _make_llm_response(
            [
                {"place_id": pid1, "neighborhood": "Cihangir", "image_prompt": "Beautiful street"},
                {"place_id": pid2, "neighborhood": "", "image_prompt": "Some image"},
                {"place_id": pid3, "neighborhood": "Nisantasi", "image_prompt": ""},
            ]
        )

        places = db.get_all_places(conn, city_id)
        result = enrich_places(conn, places, "Istanbul")

        # Only pid1 gets enriched — pid2 and pid3 have empty fields
        assert result == 1

        place1 = _get_place(conn, pid1)
        assert place1["neighborhood"] == "Cihangir"

        place2 = _get_place(conn, pid2)
        assert place2["neighborhood"] is None  # not updated

        place3 = _get_place(conn, pid3)
        assert place3["image_prompt"] is None  # not updated

    @patch("pipeline.enrichment.call_llm_json")
    def test_empty_places_list(self, mock_llm, conn, city_id):
        """Empty places list should return 0 and not call LLM."""
        result = enrich_places(conn, [], "Istanbul")
        assert result == 0
        mock_llm.assert_not_called()

    @patch("pipeline.enrichment.call_llm_json")
    def test_llm_json_error_continues(self, mock_llm, conn, city_id):
        """An LLM JSON parse error should not crash; batch is skipped."""
        from pipeline.llm import LLMError

        pid = _insert_place(conn, city_id, "Garbled Place")

        mock_llm.side_effect = LLMError("Failed to parse LLM response as JSON")

        places = db.get_all_places(conn, city_id)
        result = enrich_places(conn, places, "Istanbul")

        assert result == 0
        place = _get_place(conn, pid)
        assert place["neighborhood"] is None
        assert place["image_prompt"] is None
