import unittest

from tap_twitter_ads.streams import get_page_size

def get_config(value):
    return {"page_size": value}

DEFAULT_PAGE_SIZE = 1000

class TestPageSize(unittest.TestCase):

    """Tests to validate different values of the page_size parameter"""
    
    error_message = "The entered page size ({}) is invalid"

    def test_integer_page_size_field(self):
        """ Verify that page_size is set to 100 if int 100 is given in the config """
        expected_value = 100
        actual_value = get_page_size(get_config(100), DEFAULT_PAGE_SIZE)
        
        self.assertEqual(actual_value, expected_value)
        
    def test_float_page_size_field(self):
        """ Verify that exception is raised with proper error message, if float 100.05 is given in the config """

        with self.assertRaises(Exception) as err:
            get_page_size(get_config(100.05), DEFAULT_PAGE_SIZE)
        self.assertEqual(str(err.exception), self.error_message.format(100.05))

    def test_zero_int_page_size_field(self):
        """ Verify that exception is raised with proper error message, if 0 is given in the config """

        with self.assertRaises(Exception) as err:
            get_page_size(get_config(0), DEFAULT_PAGE_SIZE)
        self.assertEqual(str(err.exception), self.error_message.format(0))

    def test_empty_string_page_size_field(self):
        """ Verify that page_size is set to DEFAULT_PAGE_SIZE if empty string is given in the config """

        expected_value = DEFAULT_PAGE_SIZE
        actual_value = get_page_size(get_config(""), DEFAULT_PAGE_SIZE)

        self.assertEqual(actual_value, expected_value)

    def test_string_page_size_field(self):
        """ Verify that page_size is set to 100 if string "100" is given in the config """

        expected_value = 100
        actual_value = get_page_size(get_config("100"), DEFAULT_PAGE_SIZE)

        self.assertEqual(actual_value, expected_value)

    def test_invalid_string_page_size_field(self):
        """ Verify that exception is raised with proper error message, if invalid string is given in the config """

        with self.assertRaises(Exception) as err:
            get_page_size(get_config("dg%#"), DEFAULT_PAGE_SIZE)
        self.assertEqual(str(err.exception), self.error_message.format("dg%#"))

    def test_zero_float_page_size_field(self):
        """ Verify that exception is raised with proper error message, if 0.0 is given in the config """

        with self.assertRaises(Exception) as err:
            get_page_size(get_config(0.0), DEFAULT_PAGE_SIZE)
        self.assertEqual(str(err.exception), self.error_message.format(0.0))

    def test_negative_int_page_size_field(self):
        """ Verify that exception is raised with proper error message, if negative int is given in the config """

        with self.assertRaises(Exception) as err:
            get_page_size(get_config(-10), DEFAULT_PAGE_SIZE)
        self.assertEqual(str(err.exception), self.error_message.format(-10))

    def test_negative_float_page_size_field(self):
        """ Verify that exception is raised with proper error message, if negative float is given in the config """

        with self.assertRaises(Exception) as err:
            get_page_size(get_config(-10.5), DEFAULT_PAGE_SIZE)
        self.assertEqual(str(err.exception), self.error_message.format(-10.5))
