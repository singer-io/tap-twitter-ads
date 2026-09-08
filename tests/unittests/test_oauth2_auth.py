import unittest
from unittest import mock

from twitter_ads.client import Client
from twitter_ads.http import Request

from tap_twitter_ads import client_rest


class FakeResponse:
    """Minimal stand-in for a `requests` Response object."""

    def __init__(self, status_code, text='{}', headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.raw = mock.Mock()

    def json(self):
        import json as _json
        return _json.loads(self.text)


class TestRefreshAccessToken(unittest.TestCase):
    """Unit tests for the OAuth 2.0 refresh_token exchange."""

    @mock.patch('tap_twitter_ads.client_rest.requests.post')
    def test_refresh_access_token_success(self, mocked_post):
        mocked_post.return_value = mock.Mock(status_code=200)
        mocked_post.return_value.json.return_value = {
            'access_token': 'new_access', 'refresh_token': 'new_refresh'}

        access_token, refresh_token = client_rest.refresh_access_token(
            'cid', 'csecret', 'old_refresh')

        self.assertEqual(access_token, 'new_access')
        self.assertEqual(refresh_token, 'new_refresh')

        called_args, called_kwargs = mocked_post.call_args
        self.assertEqual(called_args[0], client_rest.OAUTH2_TOKEN_URL)
        self.assertEqual(called_kwargs['data']['grant_type'], 'refresh_token')
        self.assertEqual(called_kwargs['data']['refresh_token'], 'old_refresh')
        self.assertEqual(called_kwargs['data']['client_id'], 'cid')

    @mock.patch('tap_twitter_ads.client_rest.requests.post')
    def test_refresh_access_token_no_rotated_refresh_token_falls_back(self, mocked_post):
        mocked_post.return_value = mock.Mock(status_code=200)
        mocked_post.return_value.json.return_value = {'access_token': 'new_access'}

        access_token, refresh_token = client_rest.refresh_access_token(
            'cid', 'csecret', 'old_refresh')

        self.assertEqual(access_token, 'new_access')
        self.assertEqual(refresh_token, 'old_refresh')

    @mock.patch('tap_twitter_ads.client_rest.requests.post')
    def test_refresh_access_token_http_error_raises(self, mocked_post):
        mocked_post.return_value = mock.Mock(status_code=400, text='invalid_grant')

        with self.assertRaises(client_rest.TwitterOauth2Error):
            client_rest.refresh_access_token('cid', 'csecret', 'old_refresh')

    @mock.patch('tap_twitter_ads.client_rest.requests.post')
    def test_refresh_access_token_missing_access_token_raises(self, mocked_post):
        mocked_post.return_value = mock.Mock(status_code=200)
        mocked_post.return_value.json.return_value = {}

        with self.assertRaises(client_rest.TwitterOauth2Error):
            client_rest.refresh_access_token('cid', 'csecret', 'old_refresh')


class TestTwitterClientBearerAuth(unittest.TestCase):
    """Unit tests for tap_twitter_ads.client_rest.TwitterClient OAuth 2.0 auth."""

    def test_missing_auth_parameter_raises(self):
        with self.assertRaises(Exception):
            client_rest.TwitterClient(
                client_id='cid', client_secret='csecret',
                access_token=None, refresh_token='rtoken')

    @mock.patch('tap_twitter_ads.client_rest.refresh_access_token')
    @mock.patch('tap_twitter_ads.client_rest.requests.Session.request')
    def test_request_refreshes_and_retries_on_401(self, mocked_request, mocked_refresh):
        mocked_refresh.return_value = ('new_access', 'new_refresh')
        rate_limit_headers = {
            'x-rate-limit-limit': '100',
            'x-rate-limit-remaining': '99',
            'x-rate-limit-reset': '9999999999'
        }
        responses = [
            FakeResponse(401, text='{}', headers=rate_limit_headers),
            FakeResponse(200, text='{"data": []}', headers=rate_limit_headers),
        ]
        seen_auth_headers = []

        def fake_request(*args, **kwargs):
            seen_auth_headers.append(kwargs['headers']['Authorization'])
            return responses.pop(0)
        mocked_request.side_effect = fake_request

        config = {'access_token': 'old_access', 'refresh_token': 'old_refresh'}
        tap_client = client_rest.TwitterClient(
            client_id='cid', client_secret='csecret',
            access_token='old_access', refresh_token='old_refresh',
            config=config)

        result = tap_client.get(path='accounts')

        self.assertEqual(result, {'data': []})
        mocked_refresh.assert_called_once_with('cid', 'csecret', 'old_refresh')
        # Rotated tokens persisted back to the config dict
        self.assertEqual(config['access_token'], 'new_access')
        self.assertEqual(config['refresh_token'], 'new_refresh')
        # First call used the original bearer token, retry used the refreshed one
        self.assertEqual(seen_auth_headers, ['Bearer old_access', 'Bearer new_access'])


class TestPatchTwitterAdsSdkAuth(unittest.TestCase):
    """Unit tests for the twitter-ads SDK OAuth 2.0 Bearer auth monkey-patch."""

    @mock.patch('tap_twitter_ads.client_rest.refresh_access_token')
    @mock.patch('tap_twitter_ads.client_rest.requests.Session')
    def test_bearer_auth_and_refresh_on_401(self, mocked_session_cls, mocked_refresh):
        mocked_refresh.return_value = ('new_access', 'new_refresh')
        mocked_session = mocked_session_cls.return_value
        responses = [
            FakeResponse(401, text='{}'),
            FakeResponse(200, text='{"data": []}'),
        ]
        seen_auth_headers = []

        def fake_get(*args, **kwargs):
            seen_auth_headers.append(kwargs['headers']['Authorization'])
            return responses.pop(0)
        mocked_session.get.side_effect = fake_get

        config = {'access_token': 'old_access', 'refresh_token': 'old_refresh'}
        client_rest.patch_twitter_ads_sdk_auth(config, config_path=None)

        # consumer_key/secret/access_token_secret slots reused for
        # client_id/client_secret/refresh_token (see patch_twitter_ads_sdk_auth docstring)
        sdk_client = Client(
            consumer_key='cid', consumer_secret='csecret',
            access_token='old_access', access_token_secret='old_refresh',
            options={})

        response = Request(sdk_client, 'get', '/11/accounts').perform()

        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, {'data': []})
        mocked_refresh.assert_called_once_with('cid', 'csecret', 'old_refresh')
        self.assertEqual(config['access_token'], 'new_access')
        self.assertEqual(sdk_client.access_token, 'new_access')
        self.assertEqual(seen_auth_headers, ['Bearer old_access', 'Bearer new_access'])


if __name__ == '__main__':
    unittest.main()
