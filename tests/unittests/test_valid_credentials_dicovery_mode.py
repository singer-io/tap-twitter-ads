import unittest
import tap_twitter_ads
from twitter_ads.client import Client
from tap_twitter_ads import do_discover
from unittest import mock

@mock.patch('tap_twitter_ads.discover', side_effect=tap_twitter_ads.discover)
class TestCredCheckInDiscoverMode(unittest.TestCase):
    # client object for the api call
    client = Client(
        consumer_key="test",
        consumer_secret="test",
        access_token="test",
        access_token_secret="test"
    )

    @mock.patch('tap_twitter_ads.sync.Cursor', side_effect=Exception("Unauthorized access"))
    def test_invalid_get_resource_401(self, mocked_request, mocked_discover):
        """
            Verify exception is raised for no access(401) error code for auth
            and discover() is not called due to exception.
        """

        with self.assertRaises(Exception) as e:
            catalog = do_discover([], self.client, "test")

        # Verify that discover() is not called due to invalid credentials
        self.assertEqual(mocked_discover.call_count, 0)
    
    @mock.patch('tap_twitter_ads.get_resource')
    @mock.patch('tap_twitter_ads.Client',  side_effect=Exception("invalid"))
    def test_valid_credentials_invalid_account_id(self, mocked_client, mocked_get_resource, mocked_discover):
        """
            Verify that credential are valid and account ids are invalid, raise exception
        """

        # Call discover mode
        with self.assertRaises(Exception) as e:
            catalog = do_discover([], self.client, "test")

        # Verify that discover is not called due to invalid account ids
        self.assertEqual(str(e.exception), "Invalid Twitter Ads accounts provided during the configuration:['test']")
        self.assertEqual(mocked_client.call_count, 0)

    @mock.patch('tap_twitter_ads.check_credentials',  side_effect="OK")
    def test_valid_credentials_valid_account_id_200(self, mocked_client, mocked_discover):
        """
            Verify discover() is called if auth credentials and account ids are valid
            and catalog object is returned from discover().
        """

        # Call discover mode
        catalog = do_discover([], self.client, "test")

        self.assertEqual(mocked_client.call_count, 1)
