import io
import json
import unittest
from contextlib import redirect_stdout
from unittest import mock

from tap_twitter_ads.discover import discover
from tap_twitter_ads.client import XApiClient
from tap_twitter_ads.sync import sync


def select_all_streams(catalog, only=None):
    for stream in catalog.streams:
        if only and stream.tap_stream_id not in only:
            continue
        for entry in stream.metadata:
            if entry.get('breadcrumb') == ():
                entry.setdefault('metadata', {})['selected'] = True
    return catalog


def parse_singer_output(raw_text):
    return [json.loads(line) for line in raw_text.strip().splitlines() if line.strip()]


def mock_app_token(mocked_post):
    resp = mock.Mock()
    resp.status_code = 200
    resp.json.return_value = {'access_token': 'app_token', 'token_type': 'bearer'}
    mocked_post.return_value = resp


CONFIG = {
    'start_date': '2023-01-01T00:00:00Z',
    'client_id': 'cid',
    'client_secret': 'csecret',
    'access_token': 'initial_access_token',
    'refresh_token': 'initial_refresh_token',
}


class TestSyncUsersMeGroup(unittest.TestCase):
    def _responses(self, url, params=None):
        resp = mock.Mock()
        resp.status_code = 200
        if url.endswith('/2/users/me'):
            resp.json.return_value = {'data': {'id': 'u1', 'username': 'xdevelopers'}}
        elif url.endswith('/2/users/u1/tweets'):
            resp.json.return_value = {'data': [
                {'id': '3', 'text': 'third', 'created_at': '2023-03-01T00:00:00Z'},
                {'id': '2', 'text': 'second', 'created_at': '2023-02-01T00:00:00Z'},
            ], 'meta': {}}
        elif url.endswith('/2/users/u1/followers'):
            resp.json.return_value = {'data': [{'id': 'f1', 'username': 'follower_one'}], 'meta': {}}
        else:
            raise AssertionError('Unexpected URL: {}'.format(url))
        return resp

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_parent_and_children_sync_with_incremental_bookmark(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)
        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: self._responses(url, params)

        catalog = select_all_streams(discover(), only=['users_me', 'user_tweets', 'user_followers'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())

        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'users_me']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'user_tweets']), 2)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'user_followers']), 1)
        self.assertEqual(state['bookmarks']['user_tweets']['u1'], '2023-03-01T00:00:00Z')

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_users_me_not_selected_still_fetched_for_children(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)
        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: self._responses(url, params)

        catalog = select_all_streams(discover(), only=['user_tweets'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())

        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'users_me']), 0)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'user_tweets']), 2)


class TestSyncErrorIsolation(unittest.TestCase):
    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_one_stream_failure_does_not_discard_other_streams_data(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)

        def response_for(url, params=None):
            resp = mock.Mock()
            if url.endswith('/2/users/me'):
                resp.status_code = 200
                resp.json.return_value = {'data': {'id': 'u1'}}
            elif url.endswith('/2/users/u1/tweets'):
                resp.status_code = 200
                resp.json.return_value = {'data': [{'id': '1', 'text': 'hi', 'created_at': '2023-01-01T00:00:00Z'}], 'meta': {}}
            elif url.endswith('/2/users/u1/mentions'):
                resp.status_code = 402
                resp.text = '{"detail": "credits depleted"}'
                resp.json.return_value = {'detail': 'credits depleted'}
            return resp

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        catalog = select_all_streams(discover(), only=['users_me', 'user_tweets', 'user_mentions'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with self.assertRaises(Exception) as ctx:
            with redirect_stdout(buf):
                sync(client, CONFIG, catalog, state)

        self.assertIn('user_mentions', str(ctx.exception))
        self.assertIn('credits depleted', str(ctx.exception))

        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'users_me']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'user_tweets']), 1)


class TestSyncConfigIdsAndLoop(unittest.TestCase):
    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_users_by_ids_batch_lookup(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)
        resp = mock.Mock()
        resp.status_code = 200
        resp.json.return_value = {'data': [{'id': '1', 'username': 'a'}, {'id': '2', 'username': 'b'}]}
        mocked_get.return_value = resp

        config = dict(CONFIG, user_ids='1,2')
        catalog = select_all_streams(discover(), only=['users_by_ids'])
        client = XApiClient(config)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, config, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD' and m['stream'] == 'users_by_ids']
        self.assertEqual(len(record_msgs), 2)
        self.assertEqual(mocked_get.call_args.kwargs['params']['ids'], '1,2')

    def test_users_by_ids_skipped_when_not_configured(self):
        with mock.patch('tap_twitter_ads.client.requests.Session.get') as mocked_get:
            catalog = select_all_streams(discover(), only=['users_by_ids'])
            client = XApiClient(CONFIG)
            state = {}

            buf = io.StringIO()
            with redirect_stdout(buf):
                sync(client, CONFIG, catalog, state)
            messages = parse_singer_output(buf.getvalue())
            record_msgs = [m for m in messages if m['type'] == 'RECORD']
            self.assertEqual(len(record_msgs), 0)
            mocked_get.assert_not_called()

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_list_by_id_and_children(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)

        def response_for(url, params=None):
            resp = mock.Mock()
            resp.status_code = 200
            if url.endswith('/2/lists/111'):
                resp.json.return_value = {'data': {'id': '111', 'name': 'My List'}}
            elif url.endswith('/2/lists/111/tweets'):
                resp.json.return_value = {'data': [{'id': 't1', 'text': 'hi', 'created_at': '2023-01-01T00:00:00Z'}], 'meta': {}}
            elif url.endswith('/2/lists/111/members'):
                resp.json.return_value = {'data': [{'id': 'm1', 'username': 'member'}], 'meta': {}}
            return resp

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        config = dict(CONFIG, list_ids='111')
        catalog = select_all_streams(discover(), only=['list_by_id', 'list_tweets', 'list_members'])
        client = XApiClient(config)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, config, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'list_by_id']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'list_tweets']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'list_members']), 1)

    def test_list_children_do_not_silently_no_op_when_list_ids_unconfigured(self):
        with mock.patch('tap_twitter_ads.client.requests.Session.get') as mocked_get:
            catalog = select_all_streams(discover(), only=['list_tweets', 'list_members'])
            client = XApiClient(CONFIG)
            state = {}

            buf = io.StringIO()
            with self.assertLogs('root', level='WARNING') as log_ctx:
                with redirect_stdout(buf):
                    sync(client, CONFIG, catalog, state)

            self.assertTrue(any('list_tweets' in line and 'SKIPPED' in line for line in log_ctx.output))
            self.assertTrue(any('list_members' in line and 'SKIPPED' in line for line in log_ctx.output))
            mocked_get.assert_not_called()

    def test_standalone_config_loop_stream_without_children_is_not_silently_skipped(self):
        """Regression test: a `config_loop` stream with NO registered children
        (e.g. `broadcast_by_id`, `community_by_id`) must still be dispatched
        and log an explicit SKIPPED warning when unconfigured - it must not
        be mistaken for a 'child' stream (whose `.parent` is truthy) and
        silently dropped from the standalone-stream loop."""
        with mock.patch('tap_twitter_ads.client.requests.Session.get') as mocked_get:
            catalog = select_all_streams(discover(), only=['broadcast_by_id', 'community_by_id', 'news_by_id'])
            client = XApiClient(CONFIG)
            state = {}

            buf = io.StringIO()
            with self.assertLogs('root', level='WARNING') as log_ctx:
                with redirect_stdout(buf):
                    sync(client, CONFIG, catalog, state)

            for stream_name in ('broadcast_by_id', 'community_by_id', 'news_by_id'):
                self.assertTrue(
                    any(stream_name in line and 'SKIPPED' in line for line in log_ctx.output),
                    'expected a SKIPPED warning for {}'.format(stream_name))
            mocked_get.assert_not_called()


class TestSyncSingletonListAndConfigLoopMulti(unittest.TestCase):
    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_compliance_jobs_singleton_list(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)
        resp = mock.Mock()
        resp.status_code = 200
        resp.json.return_value = {'data': [{'id': 'j1', 'type': 'tweets', 'status': 'complete'}]}
        mocked_get.return_value = resp

        catalog = select_all_streams(discover(), only=['compliance_jobs'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len(record_msgs), 1)
        # default compliance_job_type is 'tweets' when not configured
        self.assertEqual(mocked_get.call_args.kwargs['params']['type'], 'tweets')

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_trends_by_woeid_config_loop_multi(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)
        resp = mock.Mock()
        resp.status_code = 200
        resp.json.return_value = {'data': [{'trend_name': '#Foo', 'tweet_count': 10},
                                            {'trend_name': '#Bar', 'tweet_count': 5}]}
        mocked_get.return_value = resp

        config = dict(CONFIG, woeids='1')
        catalog = select_all_streams(discover(), only=['trends_by_woeid'])
        client = XApiClient(config)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, config, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len(record_msgs), 2)

    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_config_loop_multi_paginates_per_id(self, mocked_get):
        mocked_get.side_effect = [
            mock.Mock(status_code=200, json=lambda: {'data': [{'id': 'u1'}], 'meta': {'next_token': 'abc'}}),
            mock.Mock(status_code=200, json=lambda: {'data': [{'id': 'u2'}], 'meta': {}}),
        ]
        config = dict(CONFIG, tweet_ids='1')
        catalog = select_all_streams(discover(), only=['post_liking_users'])
        client = XApiClient(config)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, config, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len(record_msgs), 2)
        self.assertEqual(mocked_get.call_args_list[1].kwargs['params'].get('pagination_token'), 'abc')


class TestSyncSearchStreams(unittest.TestCase):
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_search_skipped_when_required_query_not_configured(self, mocked_get):
        catalog = select_all_streams(discover(), only=['users_search'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        self.assertEqual(len([m for m in messages if m['type'] == 'RECORD']), 0)
        mocked_get.assert_not_called()

    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_search_runs_with_configured_query(self, mocked_get):
        mocked_get.return_value = mock.Mock(
            status_code=200, json=lambda: {'data': [{'id': 'u1', 'username': 'a'}]})
        config = dict(CONFIG, users_search_query='xdevelopers')
        catalog = select_all_streams(discover(), only=['users_search'])
        client = XApiClient(config)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, config, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len(record_msgs), 1)
        self.assertEqual(mocked_get.call_args.kwargs['params']['query'], 'xdevelopers')

    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_search_incremental_bookmark_keyed_by_query(self, mocked_get):
        mocked_get.return_value = mock.Mock(status_code=200, json=lambda: {
            'data': [{'id': '1', 'text': 'hi', 'created_at': '2023-05-01T00:00:00Z'}], 'meta': {}})
        config = dict(CONFIG, post_search_query='#opensource')
        catalog = select_all_streams(discover(), only=['post_search_recent'])
        client = XApiClient(config)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, config, catalog, state)

        bookmarks = state['bookmarks']['post_search_recent']
        self.assertEqual(list(bookmarks.values())[0], '2023-05-01T00:00:00Z')

    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_community_notes_uses_test_mode_default_false(self, mocked_get):
        mocked_get.return_value = mock.Mock(status_code=200, json=lambda: {'data': []})
        catalog = select_all_streams(discover(), only=['community_notes_search_written'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        # test_mode has a default (False) so the stream should NOT be skipped
        mocked_get.assert_called_once()
        self.assertEqual(mocked_get.call_args.kwargs['params']['test_mode'], False)


class TestSyncSpaceByIdGroup(unittest.TestCase):
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_space_by_id_and_children(self, mocked_get):
        def response_for(url, params=None):
            if url.endswith('/2/spaces/s1'):
                return mock.Mock(status_code=200, json=lambda: {'data': {'id': 's1', 'title': 'My Space'}})
            if url.endswith('/2/spaces/s1/tweets'):
                return mock.Mock(status_code=200, json=lambda: {'data': [{'id': 't1', 'text': 'hi', 'created_at': '2023-01-01T00:00:00Z'}], 'meta': {}})
            if url.endswith('/2/spaces/s1/buyers'):
                return mock.Mock(status_code=200, json=lambda: {'data': [{'id': 'b1', 'username': 'buyer'}], 'meta': {}})
            raise AssertionError(url)

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        config = dict(CONFIG, space_ids='s1')
        catalog = select_all_streams(discover(), only=['space_by_id', 'space_tweets', 'space_buyers'])
        client = XApiClient(config)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, config, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'space_by_id']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'space_tweets']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'space_buyers']), 1)


if __name__ == '__main__':
    unittest.main()
