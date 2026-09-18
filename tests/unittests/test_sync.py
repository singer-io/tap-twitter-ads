import io
import json
import unittest
from contextlib import redirect_stdout
from unittest import mock

from tap_twitter_ads.discover import discover as _discover
from tap_twitter_ads.client import XApiClient
from tap_twitter_ads.exceptions import XApiForbiddenError
from tap_twitter_ads.sync import sync, get_selected_streams
from tap_twitter_ads.streams import STREAMS


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


def make_all_accessible_client():
    """A fake client whose `.get()` always succeeds (never raises), so every
    stream's check_access() probe passes - mirrors test_discover.py's helper
    of the same name. `discover()` below always uses this, since the tests
    in this file only care about sync behavior given an already-selected
    catalog, not discovery's live access-check filtering."""
    client = mock.Mock()
    client.get.return_value = {'data': {'id': '123', 'username': 'someuser'}}
    return client


def discover():
    """Local wrapper preserving the no-arg call signature used throughout
    this file: discover.discover() now requires a real client/config (live
    access checks), so this builds the full, unfiltered catalog via a fake
    always-accessible client instead."""
    return _discover(make_all_accessible_client(), CONFIG)


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

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_child_streams_emit_schema_before_records(self, mocked_get, mocked_post):
        # Regression guard: sync_child_stream previously never called
        # write_schema, so RECORD messages for every users_me-child stream
        # (user_tweets, user_followers, ...) were emitted with no preceding
        # SCHEMA message - a Singer spec violation most targets reject.
        mock_app_token(mocked_post)
        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: self._responses(url, params)

        catalog = select_all_streams(discover(), only=['users_me', 'user_tweets', 'user_followers'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())

        schema_streams = {m['stream'] for m in messages if m['type'] == 'SCHEMA'}
        self.assertIn('user_tweets', schema_streams)
        self.assertIn('user_followers', schema_streams)

        # Every stream's SCHEMA message must come before its first RECORD.
        for stream_name in ('user_tweets', 'user_followers'):
            first_schema_idx = next(i for i, m in enumerate(messages)
                                     if m['type'] == 'SCHEMA' and m['stream'] == stream_name)
            first_record_idx = next(i for i, m in enumerate(messages)
                                     if m['type'] == 'RECORD' and m['stream'] == stream_name)
            self.assertLess(first_schema_idx, first_record_idx)


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
    def test_tweets_by_ids_batch_lookup_with_configured_ids(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)
        resp = mock.Mock()
        resp.status_code = 200
        resp.json.return_value = {'data': [{'id': '1', 'text': 'a'}, {'id': '2', 'text': 'b'}]}
        mocked_get.return_value = resp

        config = dict(CONFIG, tweet_ids='1,2')
        catalog = select_all_streams(discover(), only=['tweets_by_ids'])
        client = XApiClient(config)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, config, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD' and m['stream'] == 'tweets_by_ids']
        self.assertEqual(len(record_msgs), 2)
        self.assertEqual(mocked_get.call_args.kwargs['params']['ids'], '1,2')

    def test_tweets_by_ids_skipped_when_not_configured_and_no_tweets_exist(self):
        with mock.patch('tap_twitter_ads.client.requests.Session.post') as mocked_post, \
             mock.patch('tap_twitter_ads.client.requests.Session.get') as mocked_get:
            mock_app_token(mocked_post)
            mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: mock.Mock(
                status_code=200,
                json=lambda: {'data': {'id': 'u1'}} if url.endswith('/2/users/me')
                else {'data': [], 'meta': {}})

            catalog = select_all_streams(discover(), only=['tweets_by_ids'])
            client = XApiClient(CONFIG)
            state = {}

            buf = io.StringIO()
            with redirect_stdout(buf):
                sync(client, CONFIG, catalog, state)
            messages = parse_singer_output(buf.getvalue())
            record_msgs = [m for m in messages if m['type'] == 'RECORD']
            self.assertEqual(len(record_msgs), 0)

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_tweets_by_ids_defaults_to_users_own_tweet_ids(self, mocked_get, mocked_post):
        # Regression: tweet_ids/list_ids/space_ids/woeids/post_search_query are
        # all OPTIONAL self-defaulting fields now - the tap must produce data
        # for their dependent streams using only the 5 required config fields.
        mock_app_token(mocked_post)

        def response_for(url, params=None):
            if url.endswith('/2/users/me'):
                return mock.Mock(status_code=200, json=lambda: {'data': {'id': 'u1', 'username': 'me'}})
            if url.endswith('/2/users/u1/tweets'):
                return mock.Mock(status_code=200, json=lambda: {
                    'data': [{'id': '9', 'text': 'hi', 'created_at': '2023-01-01T00:00:00Z'}], 'meta': {}})
            if url.endswith('/2/tweets'):
                self.assertEqual(params['ids'], '9')
                return mock.Mock(status_code=200, json=lambda: {'data': [{'id': '9', 'text': 'hi'}]})
            raise AssertionError('Unexpected URL: {}'.format(url))

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        catalog = select_all_streams(discover(), only=['tweets_by_ids'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD' and m['stream'] == 'tweets_by_ids']
        self.assertEqual(len(record_msgs), 1)
        # user_tweets itself wasn't selected, so it must not have emitted a record
        self.assertEqual(len([m for m in messages if m['type'] == 'RECORD' and m['stream'] == 'user_tweets']), 0)

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

    @mock.patch('tap_twitter_ads.sync.LOGGER')
    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_list_children_do_not_silently_no_op_when_owned_lists_is_empty(self, mocked_get, mocked_post, mocked_logger):
        # list_ids now self-defaults from user_owned_lists, so a real (but
        # empty) fetch happens before the children are skipped - they must
        # still log an explicit SKIPPED warning, never silently no-op.
        mock_app_token(mocked_post)

        def response_for(url, params=None):
            if url.endswith('/2/users/me'):
                return mock.Mock(status_code=200, json=lambda: {'data': {'id': 'u1', 'username': 'me'}})
            if url.endswith('/2/users/u1/owned_lists'):
                return mock.Mock(status_code=200, json=lambda: {'data': [], 'meta': {}})
            raise AssertionError('Unexpected URL: {}'.format(url))

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        catalog = select_all_streams(discover(), only=['list_tweets', 'list_members'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)

        warning_calls = [str(call) for call in mocked_logger.warning.call_args_list]
        self.assertTrue(any('list_tweets' in c and 'SKIPPED' in c for c in warning_calls))
        self.assertTrue(any('list_members' in c and 'SKIPPED' in c for c in warning_calls))

    def test_all_remaining_config_ids_and_config_loop_streams_have_self_defaults(self):
        """Every `config_ids`/`config_ids_self_default`/`config_loop`/
        `config_loop_multi` stream remaining in the tap (after removing the
        streams with no derivable id source - see streams.py's module
        docstring) resolves without any extra config beyond the 5 required
        fields. This guards against silently reintroducing a stream that
        needs config with no self-default (which would need a SKIPPED-with-
        zero-API-calls regression test like the old media_by_keys/broadcast_
        by_id ones this replaced)."""
        no_default_source_types = ('config_ids', 'config_ids_self_default', 'config_loop', 'config_loop_multi')
        for name, stream in STREAMS.items():
            if stream.source_type in no_default_source_types:
                with self.subTest(stream=name):
                    self.assertIn(stream.source_key, (
                        'tweet_ids', 'space_ids', 'creator_ids', 'list_ids', 'woeids'),
                        '{} has source_key {!r} with no known self-default - add one or a '
                        'dedicated SKIPPED-with-zero-API-calls regression test'.format(
                            name, stream.source_key))


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
    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_post_search_recent_skipped_when_users_me_fails(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)
        mocked_get.return_value = mock.Mock(status_code=403, text='{"detail": "Forbidden"}',
                                             json=lambda: {'detail': 'Forbidden'})

        catalog = select_all_streams(discover(), only=['post_search_recent'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            with self.assertRaises(Exception):
                sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        self.assertEqual(len([m for m in messages if m['type'] == 'RECORD']), 0)

    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_search_runs_with_configured_query(self, mocked_get):
        mocked_get.return_value = mock.Mock(
            status_code=200, json=lambda: {'data': [{'id': '1', 'text': 'hi', 'created_at': '2023-01-01T00:00:00Z'}], 'meta': {}})
        config = dict(CONFIG, post_search_query='#opensource')
        catalog = select_all_streams(discover(), only=['post_search_recent'])
        client = XApiClient(config)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, config, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len(record_msgs), 1)
        self.assertEqual(mocked_get.call_args.kwargs['params']['query'], '#opensource')

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_post_search_recent_defaults_query_to_from_self_username(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)

        def response_for(url, params=None):
            if url.endswith('/2/users/me'):
                return mock.Mock(status_code=200, json=lambda: {'data': {'id': 'u1', 'username': 'xdevelopers'}})
            if url.endswith('/2/tweets/search/recent'):
                self.assertEqual(params['query'], 'from:xdevelopers')
                return mock.Mock(status_code=200, json=lambda: {
                    'data': [{'id': '1', 'text': 'hi', 'created_at': '2023-01-01T00:00:00Z'}], 'meta': {}})
            raise AssertionError('Unexpected URL: {}'.format(url))

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        catalog = select_all_streams(discover(), only=['post_search_recent'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD' and m['stream'] == 'post_search_recent']
        self.assertEqual(len(record_msgs), 1)

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


class TestSyncSelfDefaultIds(unittest.TestCase):
    """These streams need an id-list/query config field that (per the tap's
    5-required-fields design) is now OPTIONAL - each self-defaults from
    another already-authenticated stream's own data instead of requiring
    extra config."""

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_list_by_id_defaults_to_users_own_owned_lists(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)

        def response_for(url, params=None):
            if url.endswith('/2/users/me'):
                return mock.Mock(status_code=200, json=lambda: {'data': {'id': 'u1', 'username': 'me'}})
            if url.endswith('/2/users/u1/owned_lists'):
                return mock.Mock(status_code=200, json=lambda: {
                    'data': [{'id': '111', 'name': 'My List'}], 'meta': {}})
            if url.endswith('/2/lists/111'):
                return mock.Mock(status_code=200, json=lambda: {'data': {'id': '111', 'name': 'My List'}})
            if url.endswith('/2/lists/111/tweets'):
                return mock.Mock(status_code=200, json=lambda: {
                    'data': [{'id': 't1', 'text': 'hi', 'created_at': '2023-01-01T00:00:00Z'}], 'meta': {}})
            raise AssertionError('Unexpected URL: {}'.format(url))

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        # user_owned_lists is NOT selected - only list_by_id/list_tweets are.
        catalog = select_all_streams(discover(), only=['list_by_id', 'list_tweets'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'list_by_id']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'list_tweets']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'user_owned_lists']), 0)

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_space_by_id_defaults_to_users_own_creator_spaces(self, mocked_get, mocked_post):
        mock_app_token(mocked_post)

        def response_for(url, params=None):
            if url.endswith('/2/users/me'):
                return mock.Mock(status_code=200, json=lambda: {'data': {'id': 'u1', 'username': 'me'}})
            if url.endswith('/2/spaces/by/creator_ids'):
                self.assertEqual(params['user_ids'], 'u1')
                return mock.Mock(status_code=200, json=lambda: {'data': [{'id': 's1', 'title': 'My Space'}]})
            if url.endswith('/2/spaces/s1'):
                return mock.Mock(status_code=200, json=lambda: {'data': {'id': 's1', 'title': 'My Space'}})
            raise AssertionError('Unexpected URL: {}'.format(url))

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        # spaces_by_creator_ids is NOT selected - only space_by_id is.
        catalog = select_all_streams(discover(), only=['space_by_id'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'space_by_id']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'spaces_by_creator_ids']), 0)

    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_trends_by_woeid_defaults_to_worldwide(self, mocked_get):
        mocked_get.return_value = mock.Mock(
            status_code=200, json=lambda: {'data': [{'trend_name': '#Foo', 'tweet_count': 10}]})
        catalog = select_all_streams(discover(), only=['trends_by_woeid'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len(record_msgs), 1)
        self.assertTrue(mocked_get.call_args.args[0].endswith('/2/trends/by/woeid/1'))


class TestSyncHandlesIdSourceStreamExcludedFromCatalog(unittest.TestCase):
    """Ensure excluded implicit ID-source streams do not cause sync failures."""

    class _DiscoveryFakeClient:
        """Allow all discovery probes except user_tweets."""

        def get(self, path, params=None, auth='app'):
            if path == '/2/users/u1/tweets':
                raise XApiForbiddenError('HTTP-error-code: 403, Message: Forbidden')
            if path == '/2/users/me':
                return {'data': {'id': 'u1', 'username': 'xdevelopers'}}
            return {'data': {'id': '123', 'username': 'someuser'}}

    def _discover_with_user_tweets_excluded(self):
        return _discover(self._DiscoveryFakeClient(), CONFIG)

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_tweets_by_ids_selected_with_no_tweet_ids_and_user_tweets_excluded_does_not_crash(
            self, mocked_get, mocked_post):
        """Skip ID resolution when user_tweets is excluded from the catalog."""
        mock_app_token(mocked_post)

        def response_for(url, params=None):
            if url.endswith('/2/users/me'):
                return mock.Mock(
                    status_code=200,
                    json=lambda: {'data': {'id': 'u1', 'username': 'xdevelopers'}}
                )
            raise AssertionError(
                'Unexpected URL during sync (user_tweets was excluded from the '
                'catalog, so it must never be fetched): {}'.format(url)
            )

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        catalog = self._discover_with_user_tweets_excluded()
        self.assertIsNone(catalog.get_stream('user_tweets'),
                          'test setup invalid - user_tweets should have been excluded from the catalog')

        catalog = select_all_streams(catalog, only=['tweets_by_ids'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)

        messages = parse_singer_output(buf.getvalue())
        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len(record_msgs), 0)

    @mock.patch('tap_twitter_ads.sync.LOGGER')
    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_logs_a_skipped_warning_naming_the_excluded_stream(self, mocked_get, mocked_post, mocked_logger):
        mock_app_token(mocked_post)
        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: mock.Mock(
            status_code=200, json=lambda: {'data': {'id': 'u1', 'username': 'xdevelopers'}})

        catalog = select_all_streams(self._discover_with_user_tweets_excluded(), only=['tweets_by_ids'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)

        warning_calls = [str(call) for call in mocked_logger.warning.call_args_list]
        self.assertTrue(any('user_tweets' in c and 'SKIPPED' in c and 'excluded from catalog' in c
                            for c in warning_calls))

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_reproduces_exact_original_crash_scenario_from_sync_log(self, mocked_get, mocked_post):
        """Reproduce the original crash with a zero-record sibling stream."""
        mock_app_token(mocked_post)

        def response_for(url, params=None):
            if url.endswith('/2/users/me'):
                return mock.Mock(status_code=200, json=lambda: {'data': {'id': 'u1', 'username': 'xdevelopers'}})
            if url.endswith('/2/users/u1/bookmarks/folders'):
                return mock.Mock(status_code=200, json=lambda: {'data': [], 'meta': {}})
            raise AssertionError('Unexpected URL during sync (user_tweets was excluded from the '
                                 'catalog, so it must never be fetched): {}'.format(url))

        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: response_for(url, params)

        catalog = select_all_streams(self._discover_with_user_tweets_excluded(),
                                      only=['users_me', 'user_bookmark_folders', 'tweets_by_ids'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)
        messages = parse_singer_output(buf.getvalue())

        record_msgs = [m for m in messages if m['type'] == 'RECORD']
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'users_me']), 1)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'user_bookmark_folders']), 0)
        self.assertEqual(len([m for m in record_msgs if m['stream'] == 'tweets_by_ids']), 0)


class TestGetSelectedStreamsToleratesStaleCatalog(unittest.TestCase):
    def test_unknown_stream_in_catalog_is_skipped_with_a_warning_not_a_crash(self):
        # Regression: a catalog generated by an older tap version may still
        # reference a stream that was since removed from STREAMS (e.g. a
        # stream requiring config with no derivable self-default) - this must
        # be skipped cleanly, never raise a KeyError deep in sync()'s
        # dispatch loop.
        fake_selected = mock.Mock(stream='this_stream_was_removed')
        fake_catalog = mock.Mock()
        fake_catalog.get_selected_streams.return_value = [fake_selected]

        with mock.patch('tap_twitter_ads.sync.LOGGER') as mocked_logger:
            result = get_selected_streams(fake_catalog, {})

        self.assertEqual(result, [])
        warning_calls = [str(call) for call in mocked_logger.warning.call_args_list]
        self.assertTrue(any('this_stream_was_removed' in c for c in warning_calls))

    def test_mix_of_known_and_unknown_streams_keeps_only_known(self):
        fake_known = mock.Mock(stream='users_me')
        fake_unknown = mock.Mock(stream='this_stream_was_removed')
        fake_catalog = mock.Mock()
        fake_catalog.get_selected_streams.return_value = [fake_known, fake_unknown]

        result = get_selected_streams(fake_catalog, {})
        self.assertEqual(result, ['users_me'])


if __name__ == '__main__':
    unittest.main()
