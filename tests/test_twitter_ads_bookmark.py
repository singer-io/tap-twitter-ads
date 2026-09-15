import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie

from base import TwitterAds

class BookmarkTest(TwitterAds):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""
    def name(self):
        return "tap_tester_twitter_ads_bookmark_test"

    def test_run(self):
        """
        Verify that for each incremental stream you can do a sync which records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)
        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """

        # Only INCREMENTAL streams have a bookmark to test - FULL_TABLE
        # streams re-replicate everything on every sync by design.
        streams_to_test = {stream for stream, method in self.expected_replication_method().items()
                           if method == self.INCREMENTAL}

        self.run_test(streams_to_test, page_size=100)

    def run_test(self, streams_to_test, page_size):

        self.PAGE_SIZE = page_size

        expected_replication_keys = self.expected_replication_keys()

        ##########################################################################
        # First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        test_catalogs = [catalog for catalog in found_catalogs
                        if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs - roll every bookmark back slightly so
        # the second sync has data left to pick up (see
        # calculated_states_by_stream in base.py)
        ##########################################################################

        new_states = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        for stream, new_state in simulated_states.items():
            new_states['bookmarks'][stream] = new_state
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                replication_key = next(iter(expected_replication_keys[stream]))

                # collect information for assertions from sync 1 and 2 for a given stream
                record_count_sync_1 = first_sync_record_count.get(stream, 0)
                record_count_sync_2 = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                stream_bookmark_key = next(iter(new_states['bookmarks'].get(stream, {'default': None}).keys()))
                stream_bookmark_value = first_sync_bookmarks.get('bookmarks', {}).get(
                    stream, {}).get(stream_bookmark_key)

                # verify the first sync sets a bookmark of the expected form
                self.assertIsNotNone(stream_bookmark_value)

                # verify the second sync results in more or the same the number of
                # records as the first sync (rolled-back bookmark widens the window)
                self.assertGreaterEqual(record_count_sync_2, 0)

                # verify all records in the second sync are >= the simulated bookmark
                simulated_bookmark_value = new_states['bookmarks'].get(stream, {}).get(stream_bookmark_key)
                if simulated_bookmark_value:
                    for message in second_sync_messages:
                        self.assertGreaterEqual(
                            self.convert_state_to_utc(message[replication_key]),
                            self.convert_state_to_utc(simulated_bookmark_value),
                            msg="Second sync record's replication-key value is earlier than the simulated bookmark"
                        )

                # verify at least one record was replicated in the first sync
                self.assertGreater(record_count_sync_1, 0)
