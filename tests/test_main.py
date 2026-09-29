import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import is_valid_reading, get_todays_location, LOCATIONS


def test_valid_reading_normal():
    assert is_valid_reading(22.5, 60) is True


def test_valid_reading_rejects_impossible_temp():
    assert is_valid_reading(500, 50) is False


def test_valid_reading_rejects_impossible_humidity():
    assert is_valid_reading(20, 150) is False


def test_valid_reading_boundaries():
    assert is_valid_reading(-90, 0) is True
    assert is_valid_reading(60, 100) is True


def test_todays_location_is_in_locations_list():
    assert get_todays_location() in LOCATIONS


if __name__ == "__main__":
    test_valid_reading_normal()
    test_valid_reading_rejects_impossible_temp()
    test_valid_reading_rejects_impossible_humidity()
    test_valid_reading_boundaries()
    test_todays_location_is_in_locations_list()
    print("✅ All tests passed")
