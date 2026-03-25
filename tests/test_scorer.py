"""Tests for virality scoring and fuzzy dedup logic."""

import math

import pytest

from pipeline.scorer import (
    _build_merge_groups,
    _find_candidate_pairs,
    _normalize_name,
    _score_places,
)

# ---------------------------------------------------------------------------
# Virality score tests
# ---------------------------------------------------------------------------


class TestViralityScore:
    def _make_post(
        self, conn, city_id, post_id, likes=0, comments=0, shares=0, saves=0, views=1000
    ):
        """Insert a raw post and return its row id."""
        cur = conn.execute(
            """INSERT INTO raw_posts
               (city_id, platform, post_id, caption, likes, comments,
                shares, saves, views, processed)
               VALUES (?, 'tiktok', ?, 'test caption', ?, ?, ?, ?, ?, FALSE)""",
            (city_id, post_id, likes, comments, shares, saves, views),
        )
        conn.commit()
        return cur.lastrowid

    def _make_place(self, conn, city_id, name, post_ids):
        """Insert a place and link it to posts."""
        cur = conn.execute(
            "INSERT INTO places (city_id, name, type, mention_count) VALUES (?, ?, 'restaurant', ?)",
            (city_id, name, len(post_ids)),
        )
        place_id = cur.lastrowid
        for pid in post_ids:
            conn.execute(
                "INSERT INTO place_posts (place_id, post_id) VALUES (?, ?)",
                (place_id, pid),
            )
        conn.commit()
        return place_id

    def test_basic_score_calculation(self, conn, city_id):
        """Score = sum(engagement_rate) * log(num_posts + 1)."""
        # Post with 100 likes, 10 comments, 5 shares, 2 saves, 10000 views
        pid = self._make_post(
            conn, city_id, "p1", likes=100, comments=10, shares=5, saves=2, views=10000
        )
        place_id = self._make_place(conn, city_id, "Test Place", [pid])

        _score_places(conn, city_id)

        place = conn.execute(
            "SELECT virality_score FROM places WHERE id = ?", (place_id,)
        ).fetchone()

        # engagement = 2*5 + 5*4 + 10*2 + 100*1 = 10+20+20+100 = 150
        # rate = 150 / 10000 = 0.015
        # mention_bonus = log(2) ≈ 0.6931
        # score = 0.015 * 0.6931 ≈ 0.0104
        expected = round(0.015 * math.log(2), 4)
        assert place["virality_score"] == expected

    def test_multiple_posts_aggregate(self, conn, city_id):
        """Score aggregates engagement across all linked posts."""
        pid1 = self._make_post(conn, city_id, "p1", likes=100, views=1000)
        pid2 = self._make_post(conn, city_id, "p2", likes=200, views=1000)
        place_id = self._make_place(conn, city_id, "Multi Post Place", [pid1, pid2])

        _score_places(conn, city_id)

        place = conn.execute(
            "SELECT virality_score FROM places WHERE id = ?", (place_id,)
        ).fetchone()

        # post1: engagement = 100*1 = 100, rate = 100/1000 = 0.1
        # post2: engagement = 200*1 = 200, rate = 200/1000 = 0.2
        # total_score = 0.3, mention_bonus = log(3) ≈ 1.0986
        # score = 0.3 * 1.0986 ≈ 0.3296
        expected = round(0.3 * math.log(3), 4)
        assert place["virality_score"] == expected

    def test_zero_views_handled(self, conn, city_id):
        """Views of 0 should use 1 to avoid division by zero."""
        pid = self._make_post(conn, city_id, "p1", likes=50, views=0)
        place_id = self._make_place(conn, city_id, "No Views Place", [pid])

        _score_places(conn, city_id)

        place = conn.execute(
            "SELECT virality_score FROM places WHERE id = ?", (place_id,)
        ).fetchone()
        # engagement = 50, rate = 50/1 = 50, mention_bonus = log(2)
        expected = round(50.0 * math.log(2), 4)
        assert place["virality_score"] == expected

    def test_no_posts_gives_zero_score(self, conn, city_id):
        """A place with no linked posts should get score 0."""
        cur = conn.execute(
            "INSERT INTO places (city_id, name, type) VALUES (?, 'Orphan Place', 'other')",
            (city_id,),
        )
        place_id = cur.lastrowid
        conn.commit()

        _score_places(conn, city_id)

        place = conn.execute(
            "SELECT virality_score FROM places WHERE id = ?", (place_id,)
        ).fetchone()
        assert place["virality_score"] == 0.0

    def test_saves_weighted_highest(self, conn, city_id):
        """Saves (5x) should produce higher scores than likes (1x)."""
        pid_saves = self._make_post(conn, city_id, "saves_post", saves=100, views=1000)
        pid_likes = self._make_post(conn, city_id, "likes_post", likes=100, views=1000)

        place_saves = self._make_place(conn, city_id, "Saves Place", [pid_saves])
        place_likes = self._make_place(conn, city_id, "Likes Place", [pid_likes])

        _score_places(conn, city_id)

        s_saves = conn.execute(
            "SELECT virality_score FROM places WHERE id = ?", (place_saves,)
        ).fetchone()["virality_score"]
        s_likes = conn.execute(
            "SELECT virality_score FROM places WHERE id = ?", (place_likes,)
        ).fetchone()["virality_score"]

        assert s_saves > s_likes
        assert s_saves / s_likes == pytest.approx(5.0, rel=0.01)


# ---------------------------------------------------------------------------
# Normalization tests
# ---------------------------------------------------------------------------


class TestNormalization:
    def test_lowercase(self):
        assert _normalize_name("Cafe Bistro") == "cafe bistro"

    def test_strip_the(self):
        assert _normalize_name("The Blue Mosque") == "blue mosque"

    def test_strip_whitespace(self):
        assert _normalize_name("  Cafe   Bistro  ") == "cafe bistro"

    def test_the_not_stripped_mid_word(self):
        assert _normalize_name("Therapy Bar") == "therapy bar"


# ---------------------------------------------------------------------------
# Fuzzy dedup candidate detection tests
# ---------------------------------------------------------------------------


class TestFuzzyDedupCandidates:
    def _make_fake_place(self, id: int, name: str):
        """Create a dict that looks like a sqlite3.Row with id and name."""
        return {
            "id": id,
            "name": name,
            "mention_count": 1,
            "type": "restaurant",
            "virality_score": 0.0,
            "is_tourist_trap": False,
            "sample_caption": None,
            "city_id": 1,
            "created_at": None,
        }

    def test_exact_match(self):
        places = [self._make_fake_place(1, "Mikla"), self._make_fake_place(2, "Mikla")]
        pairs = _find_candidate_pairs(places)
        assert (1, 2) in pairs

    def test_near_match(self):
        places = [
            self._make_fake_place(1, "Mikla Restaurant"),
            self._make_fake_place(2, "Mikla restaurant"),
        ]
        pairs = _find_candidate_pairs(places)
        assert (1, 2) in pairs

    def test_containment_match(self):
        places = [
            self._make_fake_place(1, "Karakoy"),
            self._make_fake_place(2, "Karakoy Lokantasi"),
        ]
        pairs = _find_candidate_pairs(places)
        assert (1, 2) in pairs

    def test_short_names_no_false_merge(self):
        """Short distinct names should NOT be matched."""
        places = [self._make_fake_place(1, "Kat"), self._make_fake_place(2, "Bar")]
        pairs = _find_candidate_pairs(places)
        assert len(pairs) == 0

    def test_similar_short_names_no_false_merge(self):
        """'The Loft' vs 'The Lost' should not match."""
        places = [self._make_fake_place(1, "The Loft"), self._make_fake_place(2, "The Lost")]
        pairs = _find_candidate_pairs(places)
        assert len(pairs) == 0

    def test_completely_different_names(self):
        places = [
            self._make_fake_place(1, "Mikla Restaurant"),
            self._make_fake_place(2, "Blue Mosque"),
        ]
        pairs = _find_candidate_pairs(places)
        assert len(pairs) == 0


# ---------------------------------------------------------------------------
# Union-find (merge groups) tests
# ---------------------------------------------------------------------------


class TestBuildMergeGroups:
    def test_single_pair(self):
        groups = _build_merge_groups([(1, 2)], {1, 2, 3})
        assert len(groups) == 1
        assert groups[0] == {1, 2}

    def test_transitive_merge(self):
        groups = _build_merge_groups([(1, 2), (2, 3)], {1, 2, 3})
        assert len(groups) == 1
        assert groups[0] == {1, 2, 3}

    def test_separate_groups(self):
        groups = _build_merge_groups([(1, 2), (3, 4)], {1, 2, 3, 4})
        assert len(groups) == 2
        ids = [sorted(g) for g in groups]
        assert [1, 2] in ids
        assert [3, 4] in ids

    def test_no_pairs(self):
        groups = _build_merge_groups([], {1, 2})
        assert len(groups) == 0

    def test_complex_chain(self):
        groups = _build_merge_groups([(1, 2), (3, 4), (2, 3)], {1, 2, 3, 4, 5})
        # 1-2-3-4 should be one group, 5 is alone (no group)
        assert len(groups) == 1
        assert groups[0] == {1, 2, 3, 4}
