import unittest
from unittest import mock

import tap_twitter_ads


class MockParseArgs:
    """Mock the parsed_args() in main"""
    def __init__(self, config, state, catalog, discover, config_path=None):
        self.config = config
        self.state = state
        self.catalog = catalog
        self.discover = discover
        self.config_path = config_path


def get_args(config, state, catalog, discover):
    return MockParseArgs(config, state, catalog, discover)


BASE_CONFIG = {
    'start_date': '2020-01-01T00:00:00Z',
    'client_id': 'cid',
    'client_secret': 'csecret',
    'access_token': 'at',
    'refresh_token': 'rt',
}


@mock.patch('tap_twitter_ads.do_discover')
@mock.patch('singer.utils.parse_args')
class TestCredCheckInDiscoverMode(unittest.TestCase):
    """
    Verify discover mode does NOT require valid/live OAuth 2.0 credentials
    (streams are statically defined - see discover.py's module docstring),
    while sync mode DOES check credentials before syncing anything (see
    __init__.py's main() and XApiClient.check_credentials).

    NOTE: this is the OPPOSITE invariant from the old OAuth1 Ads tap, whose
    do_discover() made a live API call and failed fast on bad credentials.
    The OAuth2 tap deliberately does not, so an expired/rotated access_token
    never blocks `--discover`.
    """

    @mock.patch('tap_twitter_ads.XApiClient.check_credentials')
    def test_discover_mode_does_not_check_credentials(self, mocked_check_credentials,
                                                       mocked_parse_args, mocked_do_discover):
        mocked_parse_args.return_value = get_args(BASE_CONFIG, {}, None, True)

        tap_twitter_ads.main()

        # discover() was called...
        mocked_do_discover.assert_called_once()
        # ...but credentials were never checked
        mocked_check_credentials.assert_not_called()

    @mock.patch('tap_twitter_ads.XApiClient.check_credentials')
    @mock.patch('tap_twitter_ads._sync')
    def test_sync_mode_checks_credentials_before_syncing(self, mocked_sync, mocked_check_credentials,
                                                          mocked_parse_args, mocked_do_discover):
        mocked_parse_args.return_value = get_args(BASE_CONFIG, {}, {'streams': []}, False)

        tap_twitter_ads.main()

        # credentials were checked before sync ran
        mocked_check_credentials.assert_called_once()
        mocked_sync.assert_called_once()
        # discover was never invoked in sync mode
        mocked_do_discover.assert_not_called()

    @mock.patch('tap_twitter_ads.XApiClient.check_credentials', side_effect=Exception('invalid credentials'))
    @mock.patch('tap_twitter_ads._sync')
    def test_sync_mode_aborts_if_credentials_invalid(self, mocked_sync, mocked_check_credentials,
                                                      mocked_parse_args, mocked_do_discover):
        mocked_parse_args.return_value = get_args(BASE_CONFIG, {}, {'streams': []}, False)

        with self.assertRaises(Exception):
            tap_twitter_ads.main()

        # sync() must never be reached if credential check fails
        mocked_sync.assert_not_called()


if __name__ == '__main__':
    unittest.main()
