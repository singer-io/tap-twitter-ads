import unittest
from unittest import mock

from tap_twitter_ads.sync import get_bookmark, write_bookmark

# NOTE: unlike the old OAuth1 Ads tap (bookmarks keyed by a single fixed
# `account_id` for every stream, with a special-cased PUBLISHED/SCHEDULED
# sub-key for the `tweets` stream), OAuth2 streams key their bookmark by
# whatever id/value they were synced under: a parent record id for
# users_me-children (e.g. `user_tweets`), or the resolved query text for
# search streams (e.g. `post_search_recent`). There is no more special-cased
# stream name - `get_bookmark`/`write_bookmark` are fully generic.


class TestGetBookmark(unittest.TestCase):
    """Test the get_bookmark function for different state shapes."""

    stream = "user_tweets"
    default = "2018-01-28T00:00:00Z"
    parent_id = "123456"

    def test_empty_state_returns_default(self):
        """If state is empty, the default (start_date) is returned."""
        state = {}
        bookmark = get_bookmark(state, self.stream, self.parent_id, self.default)
        self.assertEqual(bookmark, self.default)

    def test_no_bookmark_for_this_stream_returns_default(self):
        """If the stream has no bookmark entry at all, the default is returned."""
        state = {'bookmarks': {'some_other_stream': {self.parent_id: '2017-01-28T00:00:00Z'}}}
        bookmark = get_bookmark(state, self.stream, self.parent_id, self.default)
        self.assertEqual(bookmark, self.default)

    def test_no_bookmark_for_this_parent_id_returns_default(self):
        """If the stream has bookmarks, but not for this parent_id, the default is returned."""
        state = {'bookmarks': {self.stream: {'some_other_id': '2017-01-28T00:00:00Z'}}}
        bookmark = get_bookmark(state, self.stream, self.parent_id, self.default)
        self.assertEqual(bookmark, self.default)

    def test_valid_bookmark_is_returned(self):
        """If a bookmark exists for this stream/parent_id, it is returned as-is."""
        state = {'bookmarks': {self.stream: {self.parent_id: "2017-01-28T00:00:00Z"}}}
        bookmark = get_bookmark(state, self.stream, self.parent_id, self.default)
        self.assertEqual(bookmark, "2017-01-28T00:00:00Z")

    def test_search_stream_bookmark_keyed_by_query_text(self):
        """Search streams (post_search_recent, etc.) key their bookmark by the
        resolved query text rather than a parent record id - get_bookmark is
        generic enough that this is just a different string key."""
        query_key = "opensource"
        state = {'bookmarks': {'post_search_recent': {query_key: "2022-05-01T00:00:00Z"}}}
        bookmark = get_bookmark(state, 'post_search_recent', query_key, self.default)
        self.assertEqual(bookmark, "2022-05-01T00:00:00Z")


class TestWriteBookmark(unittest.TestCase):
    """Test the write_bookmark function persists state and flushes it."""

    stream = "user_tweets"
    parent_id = "123456"

    @mock.patch('tap_twitter_ads.sync.singer.write_state')
    def test_write_bookmark_updates_state_and_flushes(self, mocked_write_state):
        state = {}
        write_bookmark(state, self.stream, self.parent_id, "2023-01-01T00:00:00Z")

        self.assertEqual(state['bookmarks'][self.stream][self.parent_id], "2023-01-01T00:00:00Z")
        mocked_write_state.assert_called_once_with(state)

    @mock.patch('tap_twitter_ads.sync.singer.write_state')
    def test_write_bookmark_does_not_clobber_other_parent_ids(self, mocked_write_state):
        state = {'bookmarks': {self.stream: {'other_id': '2020-01-01T00:00:00Z'}}}
        write_bookmark(state, self.stream, self.parent_id, "2023-01-01T00:00:00Z")

        self.assertEqual(state['bookmarks'][self.stream]['other_id'], '2020-01-01T00:00:00Z')
        self.assertEqual(state['bookmarks'][self.stream][self.parent_id], "2023-01-01T00:00:00Z")


if __name__ == '__main__':
    unittest.main()
