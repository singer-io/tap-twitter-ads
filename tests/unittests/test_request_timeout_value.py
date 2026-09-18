import unittest

from tap_twitter_ads.client import XApiClient, DEFAULT_REQUEST_TIMEOUT

BASE_CONFIG = {
    'client_id': 'cid',
    'client_secret': 'csecret',
    'access_token': 'at',
    'refresh_token': 'rt',
}


def make_config(**overrides):
    config = dict(BASE_CONFIG)
    config.update(overrides)
    return config


class TestRequestTimeoutValue(unittest.TestCase):
    """
    Test that XApiClient.request_timeout is resolved correctly in various cases.

    NOTE: unlike the old OAuth1 Ads tap (timeout threaded through
    tap_twitter_ads.Client's `options` kwarg), the OAuth2 tap resolves
    request_timeout directly in XApiClient.__init__ and uses it for every
    `requests` call - so this is tested against the client directly rather
    than by mocking main()/parse_args().
    """

    def test_timeout_value_in_config(self):
        """ Verify that request timeout is set based on config value """
        client = XApiClient(make_config(request_timeout=100))
        self.assertEqual(client.request_timeout, 100.0)

    def test_timeout_value_not_in_config(self):
        """ Verify that request timeout falls back to the default value """
        client = XApiClient(make_config())
        self.assertEqual(client.request_timeout, DEFAULT_REQUEST_TIMEOUT)

    def test_timeout_string_value_in_config(self):
        """ Verify that request timeout is set based on config if a string value is given """
        client = XApiClient(make_config(request_timeout='100'))
        self.assertEqual(client.request_timeout, 100.0)

    def test_timeout_empty_value_in_config(self):
        """ Verify that request timeout falls back to default if an empty value is given """
        client = XApiClient(make_config(request_timeout=''))
        self.assertEqual(client.request_timeout, DEFAULT_REQUEST_TIMEOUT)

    def test_timeout_0_value_in_config(self):
        """ Verify that request timeout falls back to default if 0 is given """
        client = XApiClient(make_config(request_timeout=0))
        self.assertEqual(client.request_timeout, DEFAULT_REQUEST_TIMEOUT)

    def test_timeout_string_0_value_in_config(self):
        """ Verify that request timeout is set to 0.0 if string "0" is given
        (only a falsy Python value like int 0 or an empty string falls back
        to the default - a non-empty string "0" is truthy and gets parsed). """
        client = XApiClient(make_config(request_timeout="0"))
        self.assertEqual(client.request_timeout, 0.0)

    def test_timeout_float_value_in_config(self):
        """ Verify that request timeout is set based on config float value """
        client = XApiClient(make_config(request_timeout=100.5))
        self.assertEqual(client.request_timeout, 100.5)

    def test_timeout_invalid_string_value_in_config_falls_back_to_default(self):
        """ Verify that request timeout falls back to default if an invalid string is given """
        client = XApiClient(make_config(request_timeout="not-a-number"))
        self.assertEqual(client.request_timeout, DEFAULT_REQUEST_TIMEOUT)


if __name__ == '__main__':
    unittest.main()
