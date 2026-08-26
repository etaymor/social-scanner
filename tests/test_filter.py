"""Tests for generic name and off-city location filtering."""

from pipeline.filter import _has_off_city_location, _is_generic_city_cuisine


class TestGenericCityCuisine:
    def test_generic_tokyo_sushi(self):
        assert _is_generic_city_cuisine("Tokyo Sushi", "Tokyo")

    def test_generic_tokyo_ramen(self):
        assert _is_generic_city_cuisine("Tokyo Ramen", "Tokyo")

    def test_generic_tokyo_cafe(self):
        assert _is_generic_city_cuisine("Tokyo Cafe", "Tokyo")

    def test_specific_business_name(self):
        # "Tokyo Station" is a real place
        assert not _is_generic_city_cuisine("Tokyo Station Ramen Street", "Tokyo")

    def test_cafe_with_name(self):
        # Specific cafe name
        assert not _is_generic_city_cuisine("Tokyo Blue Cafe", "Tokyo")

    def test_case_insensitive(self):
        assert _is_generic_city_cuisine("TOKYO SUSHI", "Tokyo")
        assert _is_generic_city_cuisine("tokyo sushi", "Tokyo")


class TestOffCityLocation:
    def test_tucson_location_tag(self):
        caption = "Great sushi!\n📍 Location tag: Tucson, Arizona"
        assert _has_off_city_location(caption)

    def test_aachen_location_tag(self):
        caption = "Amazing food\n📍 Location tag: Aachen, Germany"
        assert _has_off_city_location(caption)

    def test_little_tokyo_location(self):
        caption = "Best ramen\n📍 Location tag: Little Tokyo, Los Angeles"
        assert _has_off_city_location(caption)

    def test_japan_location_no_false_positive(self):
        # Tokyo, Japan should NOT be filtered
        caption = "Great spot\n📍 Location tag: Tokyo, Japan"
        assert not _has_off_city_location(caption)

    def test_no_location_tag(self):
        caption = "Amazing ramen spot in Shibuya!"
        assert not _has_off_city_location(caption)

    def test_united_states_tag(self):
        caption = "Sushi place\n📍 Location tag: United States"
        assert _has_off_city_location(caption)

    def test_empty_caption(self):
        assert not _has_off_city_location("")
        assert not _has_off_city_location(None)
