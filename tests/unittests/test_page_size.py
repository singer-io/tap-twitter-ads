import unittest

from tap_twitter_ads.sync import get_page_size, MIN_PAGE_SIZE, MAX_PAGE_SIZE


def get_config(value):
    return {"page_size": value}


class TestPageSize(unittest.TestCase):
    """Tests to validate different values of the page_size parameter.

    NOTE: unlike the old OAuth1 Ads tap (which raised an exception for any
    invalid page_size), the OAuth2 tap's `get_page_size` clamps to
    [MIN_PAGE_SIZE, MAX_PAGE_SIZE] and falls back to MAX_PAGE_SIZE for
    unset/invalid values - a bad page_size should never crash a sync that
    would otherwise succeed."""

    def test_integer_page_size_field(self):
        """ Verify that page_size is set to 50 if int 50 is given in the config """
        actual_value = get_page_size(get_config(50))
        self.assertEqual(actual_value, 50)

    def test_float_page_size_field(self):
        """ Verify that a float page_size is clamped/coerced without raising """
        actual_value = get_page_size(get_config(50.9))
        self.assertEqual(actual_value, 50)

    def test_zero_page_size_field_falls_back_to_default(self):
        """ Verify that 0 (falsy) falls back to MAX_PAGE_SIZE, same as unset """
        actual_value = get_page_size(get_config(0))
        self.assertEqual(actual_value, MAX_PAGE_SIZE)

    def test_missing_page_size_field_falls_back_to_default(self):
        """ Verify that page_size is set to MAX_PAGE_SIZE if unset in config """
        actual_value = get_page_size({})
        self.assertEqual(actual_value, MAX_PAGE_SIZE)

    def test_empty_string_page_size_field_falls_back_to_default(self):
        """ Verify that page_size is set to MAX_PAGE_SIZE if empty string is given """
        actual_value = get_page_size(get_config(""))
        self.assertEqual(actual_value, MAX_PAGE_SIZE)

    def test_string_page_size_field(self):
        """ Verify that page_size is set to 50 if string "50" is given in the config """
        actual_value = get_page_size(get_config("50"))
        self.assertEqual(actual_value, 50)

    def test_invalid_string_page_size_field_falls_back_to_default(self):
        """ Verify that an unparseable string falls back to MAX_PAGE_SIZE rather than raising """
        actual_value = get_page_size(get_config("dg%#"))
        self.assertEqual(actual_value, MAX_PAGE_SIZE)

    def test_negative_int_page_size_field_clamped_to_min(self):
        """ Verify that a negative page_size is clamped up to MIN_PAGE_SIZE """
        actual_value = get_page_size(get_config(-10))
        self.assertEqual(actual_value, MIN_PAGE_SIZE)

    def test_oversized_page_size_field_clamped_to_max(self):
        """ Verify that a page_size above MAX_PAGE_SIZE is clamped down to MAX_PAGE_SIZE """
        actual_value = get_page_size(get_config(10000))
        self.assertEqual(actual_value, MAX_PAGE_SIZE)


if __name__ == '__main__':
    unittest.main()
