import io
import json
import unittest
from contextlib import redirect_stdout
from unittest import mock

from tap_twitter_ads.discover import discover as _discover
from tap_twitter_ads.client import XApiClient
from tap_twitter_ads.sync import sync

# NOTE: unlike the old OAuth1 Ads tap (LineItems -> targeting_criteria, keyed
# by a fixed `account_id`), the OAuth2 tap's parent/child relationship is
# users_me -> user_tweets (etc.), and bookmarks are keyed by the parent
# record's real id (here, the authenticated user's id) rather than a fixed
# account_id. The independence being tested - a child stream's bookmark
# advances/holds independently of its parent and of other children - is the
# same property as the original test, just ported to the new stream shape.

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


class TestParentChildBookmarkIndependence(unittest.TestCase):
    """Verify that bookmarking for a child stream (user_tweets) is
    independent of its parent (users_me, which has no bookmark at all -
    it's FULL_TABLE) and of sibling children."""

    def _responses(self, url):
        resp = mock.Mock()
        resp.status_code = 200
        if url.endswith('/2/users/me'):
            resp.json.return_value = {'data': {'id': 'u1', 'username': 'xdevelopers'}}
        elif url.endswith('/2/users/u1/tweets'):
            resp.json.return_value = {'data': [], 'meta': {}}
        elif url.endswith('/2/users/u1/mentions'):
            resp.json.return_value = {'data': [
                {'id': '9', 'text': 'a mention', 'created_at': '2023-05-01T00:00:00Z'},
            ], 'meta': {}}
        else:
            raise AssertionError('Unexpected URL: {}'.format(url))
        return resp

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_no_bookmark_written_when_child_stream_has_no_data(self, mocked_get, mocked_post):
        """Verify that no new (incorrect) bookmark value appears for a child
        stream when its sync returns zero records - the bookmark should stay
        at start_date, not silently disappear or error."""
        mock_app_token(mocked_post)
        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: self._responses(url)

        catalog = select_all_streams(discover(), only=['users_me', 'user_tweets'])
        client = XApiClient(CONFIG)
        state = {}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)

        # user_tweets got 0 records, but the bookmark is still written
        # (always, unconditionally - see sync_child_stream) at start_date.
        self.assertEqual(state['bookmarks']['user_tweets']['u1'], '2023-01-01T00:00:00Z')
        # users_me (FULL_TABLE) never gets a bookmark entry at all.
        self.assertNotIn('users_me', state.get('bookmarks', {}))

    @mock.patch('tap_twitter_ads.client.requests.Session.post')
    @mock.patch('tap_twitter_ads.client.requests.Session.get')
    def test_child_stream_bookmark_advances_independent_of_sibling(self, mocked_get, mocked_post):
        """Verify that user_mentions' bookmark advances based on its own
        data, unaffected by its sibling user_tweets returning nothing."""
        mock_app_token(mocked_post)
        mocked_get.side_effect = lambda url, headers=None, params=None, timeout=None: self._responses(url)

        catalog = select_all_streams(discover(), only=['users_me', 'user_tweets', 'user_mentions'])
        client = XApiClient(CONFIG)
        state = {'bookmarks': {'user_mentions': {'u1': '2023-02-01T00:00:00Z'}}}

        buf = io.StringIO()
        with redirect_stdout(buf):
            sync(client, CONFIG, catalog, state)

        # user_mentions advanced to the newest record's created_at
        self.assertEqual(state['bookmarks']['user_mentions']['u1'], '2023-05-01T00:00:00Z')
        # user_tweets (0 records) stayed at start_date, independent of its sibling
        self.assertEqual(state['bookmarks']['user_tweets']['u1'], '2023-01-01T00:00:00Z')


if __name__ == '__main__':
    unittest.main()
