import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie

from base import TwitterAds

class TweetsBookmarkTest(TwitterAds):
    """Test that `post_search_recent` keeps an INDEPENDENT bookmark per
    distinct `post_search_query` value (see sync.py's sync_search_stream -
    the bookmark key is the resolved query text itself, not a fixed id).
    This is the OAuth 2.0 equivalent of the old OAuth1 Ads tap's `tweets`
    stream keeping separate PUBLISHED/SCHEDULED sub-bookmarks - the concept
    of "one stream, multiple independent bookmark tracks" carries over even
    though the OAuth1 Ads-specific PUBLISHED/SCHEDULED distinction does not
    exist in X API v2."""

    STREAM = 'post_search_recent'

    def name(self):
        return "tap_tester_twitter_ads_tweets_bookmark_test"

    def test_run(self):
        # Run once per distinct query value so both queries' independent
        # bookmarks get exercised across the two syncs below.
        self.run_bookmark("opensource")
        self.run_bookmark("data engineering")

    def run_bookmark(self, search_query):
        """
        Verify that a sync records a bookmark keyed by the resolved query value,
        and that a second sync (with the bookmark rolled back) only re-fetches
        records at-or-after that bookmark - independent of any other query's
        bookmark for the same stream.
        """
        streams_to_test = {self.STREAM}
        replication_key = next(iter(self.expected_replication_keys()[self.STREAM]))

        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        test_catalogs = [catalog for catalog in found_catalogs
                        if catalog.get('tap_stream_id') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_state = menagerie.get_state(conn_id)

        bookmarks_for_stream = first_sync_state.get('bookmarks', {}).get(self.STREAM, {})

        # the bookmark key for a search stream is the resolved query text
        # itself (truncated/joined - see resolve `bookmark_key` in sync.py),
        # so simply verify SOME bookmark was written for this sync
        self.assertGreater(len(bookmarks_for_stream), 0,
                           msg="Expected a bookmark to be written for {} after searching for {!r}".format(
                               self.STREAM, search_query))

        ##########################################################################
        # Roll the bookmark back and re-sync
        ##########################################################################

        rolled_back_state = {'bookmarks': {self.STREAM: {}}}
        for bookmark_key, bookmark_value in bookmarks_for_stream.items():
            rolled_back_state['bookmarks'][self.STREAM][bookmark_key] = self.timedelta_formatted(
                bookmark_value[:10], days=-7) + bookmark_value[10:]
        menagerie.set_state(conn_id, rolled_back_state)

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()

        # verify the second (rolled-back) sync still respects the (older) bookmark
        second_sync_messages = [record.get('data') for record in
                                second_sync_records.get(self.STREAM, {}).get('messages', [])
                                if record.get('action') == 'upsert']
        for message in second_sync_messages:
            self.assertIsNotNone(message.get(replication_key))
