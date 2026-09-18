import json
import tempfile
import unittest
from unittest import mock

import requests

from tap_twitter_ads.client import XApiClient
from tap_twitter_ads.exceptions import XApiAuthenticationError, XApiBadRequestError


def make_config():
    return {
        'client_id': 'test_client_id',
        'client_secret': 'test_client_secret',
        'access_token': 'test_access_token',
        'refresh_token': 'test_refresh_token',
    }


def mock_response(status_code, json_body=None, headers=None, text=''):
    resp = mock.Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.text = text
    resp.json.return_value = json_body if json_body is not None else {}
    return resp


class TestXApiClientUserAuth(unittest.TestCase):
    def test_missing_credentials_raises(self):
        with self.assertRaises(XApiAuthenticationError):
            XApiClient({'client_id': 'x'})

    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_get_user_auth_success(self, mocked_get):
        mocked_get.return_value = mock_response(200, {'data': {'id': '123'}})
        client = XApiClient(make_config())

        result = client.get('/2/users/me', auth='user')

        self.assertEqual(result, {'data': {'id': '123'}})
        self.assertEqual(mocked_get.call_args.kwargs['headers']['Authorization'], 'Bearer test_access_token')

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_401_triggers_refresh_then_retries_successfully(self, mocked_get, mocked_post):
        mocked_get.side_effect = [
            mock_response(401, {'title': 'Unauthorized'}),
            mock_response(200, {'data': {'id': '123'}}),
        ]
        mocked_post.return_value = mock_response(200, {
            'access_token': 'new_access_token',
            'refresh_token': 'new_refresh_token',
        })

        client = XApiClient(make_config())
        result = client.get('/2/users/me', auth='user')

        self.assertEqual(result, {'data': {'id': '123'}})
        self.assertEqual(client.access_token, 'new_access_token')
        self.assertEqual(client.refresh_token, 'new_refresh_token')
        self.assertEqual(mocked_get.call_count, 2)

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_401_after_refresh_does_not_loop_forever(self, mocked_get, mocked_post):
        mocked_get.return_value = mock_response(401, {'title': 'Unauthorized'})
        mocked_post.return_value = mock_response(400, {
            'error': 'invalid_request',
            'error_description': 'Value passed for the token was invalid.',
        })

        client = XApiClient(make_config())
        with self.assertRaises(XApiBadRequestError):
            client.get('/2/users/me', auth='user')

        self.assertEqual(mocked_get.call_count, 1)

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    def test_refresh_persists_rotated_tokens_to_config_file(self, mocked_post):
        mocked_post.return_value = mock_response(200, {
            'access_token': 'rotated_access_token',
            'refresh_token': 'rotated_refresh_token',
        })

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(make_config(), f)
            config_path = f.name

        client = XApiClient(make_config(), config_path=config_path)
        client.refresh_access_token()

        with open(config_path, encoding='utf-8') as f:
            saved_config = json.load(f)

        self.assertEqual(saved_config['access_token'], 'rotated_access_token')
        self.assertEqual(saved_config['refresh_token'], 'rotated_refresh_token')


class TestXApiClientCheckCredentials(unittest.TestCase):
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_check_credentials_passes_on_success(self, mocked_get):
        mocked_get.return_value = mock_response(200, {'data': {'id': '123'}})
        client = XApiClient(make_config())

        client.check_credentials()  # should not raise

        self.assertEqual(mocked_get.call_args.kwargs['headers']['Authorization'], 'Bearer test_access_token')

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_check_credentials_survives_expired_access_token(self, mocked_get, mocked_post):
        # An expired access_token alone should NOT fail the check - get()'s
        # normal 401 refresh-and-retry path handles it transparently.
        mocked_get.side_effect = [
            mock_response(401, {'title': 'Unauthorized'}),
            mock_response(200, {'data': {'id': '123'}}),
        ]
        mocked_post.return_value = mock_response(200, {
            'access_token': 'new_access_token',
            'refresh_token': 'new_refresh_token',
        })
        client = XApiClient(make_config())

        client.check_credentials()  # should not raise

        self.assertEqual(client.access_token, 'new_access_token')

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_check_credentials_raises_clear_error_on_invalid_refresh_token(self, mocked_get, mocked_post):
        mocked_get.return_value = mock_response(401, {'title': 'Unauthorized'})
        mocked_post.return_value = mock_response(400, {
            'error': 'invalid_grant',
            'error_description': 'Value passed for the token was invalid.',
        })
        client = XApiClient(make_config())

        with self.assertRaises(XApiAuthenticationError):
            client.check_credentials()


class TestXApiClientAppAuth(unittest.TestCase):
    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_get_app_auth_mints_token_lazily(self, mocked_get, mocked_post):
        mocked_post.return_value = mock_response(200, {'access_token': 'app_token', 'token_type': 'bearer'})
        mocked_get.return_value = mock_response(200, {'data': [{'id': '1'}]})

        client = XApiClient(make_config())
        result = client.get('/2/users/1/tweets', auth='app')

        self.assertEqual(result, {'data': [{'id': '1'}]})
        self.assertEqual(client.app_access_token, 'app_token')
        self.assertEqual(mocked_get.call_args.kwargs['headers']['Authorization'], 'Bearer app_token')
        # app token should not require another mint on a second call
        client.get('/2/users/1/tweets', auth='app')
        self.assertEqual(mocked_post.call_count, 1)

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_app_token_401_remints_once(self, mocked_get, mocked_post):
        mocked_post.side_effect = [
            mock_response(200, {'access_token': 'app_token_1'}),
            mock_response(200, {'access_token': 'app_token_2'}),
        ]
        mocked_get.side_effect = [
            mock_response(401, {'title': 'Unauthorized'}),
            mock_response(200, {'data': [{'id': '1'}]}),
        ]

        client = XApiClient(make_config())
        result = client.get('/2/users/1/tweets', auth='app')

        self.assertEqual(result, {'data': [{'id': '1'}]})
        self.assertEqual(client.app_access_token, 'app_token_2')


class TestXApiClientRateLimitAndPagination(unittest.TestCase):
    @mock.patch('tap_twitter_ads.client.time.sleep', return_value=None)
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_rate_limit_429_is_retried(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = [
            mock_response(429, {}, headers={'x-rate-limit-reset': '9999999999'}),
            mock_response(200, {'data': {'id': '123'}}),
        ]
        client = XApiClient(make_config())

        result = client.get('/2/users/me', auth='user')

        self.assertEqual(result, {'data': {'id': '123'}})
        self.assertTrue(mocked_sleep.called)
        self.assertEqual(mocked_get.call_count, 2)

    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_get_paginated_follows_next_token(self, mocked_get):
        mocked_get.side_effect = [
            mock_response(200, {'data': [{'id': '1'}], 'meta': {'next_token': 'abc'}}),
            mock_response(200, {'data': [{'id': '2'}], 'meta': {}}),
        ]
        client = XApiClient(make_config())

        pages = list(client.get_paginated('/2/users/1/tweets', {}, auth='user'))

        self.assertEqual(len(pages), 2)
        self.assertEqual(mocked_get.call_args_list[1].kwargs['params'].get('pagination_token'), 'abc')

    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_get_paginated_stops_at_max_pages_safety_cap(self, mocked_get):
        mocked_get.return_value = mock_response(200, {'data': [{'id': '1'}], 'meta': {'next_token': 'loop'}})
        client = XApiClient(make_config())

        pages = list(client.get_paginated('/2/users/1/tweets', {}, auth='user', max_pages=3))

        self.assertEqual(len(pages), 3)


if __name__ == '__main__':
    unittest.main()
