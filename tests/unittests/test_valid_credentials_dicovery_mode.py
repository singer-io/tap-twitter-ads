import unittest
import tap_twitter_ads
from twitter_ads.client import Client
from tap_twitter_ads import do_discover
from unittest import mock


@mock.patch('tap_twitter_ads.discover', side_effect=tap_twitter_ads.discover)
@mock.patch('tap_twitter_ads.sync.Cursor')
class TestCredCheckInDiscoverMode(unittest.TestCase):

    @mock.patch('tap_twitter_ads.sync.Request', side_effect=Exception("Unauthorized access"))
    def test_invalid_creds_401(self, mocked_request, mock_cursor, mock_discover):
        """
            Verify exception is raised for no access(401) error code for auth
            and discover() is not called due to exception.
        """
        # Prepare config and mock request to raise 401 error code
        config = {
            "consumer_key": "test",
            "consumer_secret": "test",
            "access_token": "test",
            "access_token_secret": "test",
            "reports": []
        }
        client = Client(
            consumer_key=config.get('consumer_key'),
            consumer_secret=config.get('consumer_secret'),
            access_token=config.get('access_token'),
            access_token_secret=config.get('access_token_secret')
        )

        with self.assertRaises(Exception) as e:
            catalog = do_discover(config["reports"], client)

        # Verify that discover() is not called due to invalid credentials
        self.assertEqual(mock_discover.call_count, 0)
    
    @mock.patch('tap_twitter_ads.sync.Request', side_effect="OK")
    def test_valid_creds_200(self, mock_cursor, mocked_request, mock_discover):
        """
            Verify discover() is called if auth credentials are valid
            and catalog object is returned from discover().
        """
        # Prepare config and mock request to return 200 status code
        config = {
            "consumer_key": "test",
            "consumer_secret": "test",
            "access_token": "test",
            "access_token_secret": "test",
            "reports": []
        }
        client = Client(
            consumer_key=config.get('consumer_key'),
            consumer_secret=config.get('consumer_secret'),
            access_token=config.get('access_token'),
            access_token_secret=config.get('access_token_secret')
        )

        # Call discover mode
        catalog = do_discover(config["reports"], client)

        # Verify that get_schemas() called once and catalog object is returned from discover()
        self.assertEqual(mock_discover.call_count, 1)
