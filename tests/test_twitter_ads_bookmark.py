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
        Verify that for each stream you can do a sync which records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)
        Verify that for full table stream, all data replicated in sync 1 is replicated again in sync 2.
        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """

        streams_to_test = self.expected_streams()

        # For following streams(except targeting_tv_markets and targeting_tv_shows), we are not able to generate any records.
        # targeting_tv_markets and targeting_tv_shows streams take more than 5 hour to complete the sync.
        #  So, skipping those streams from test case.
        streams_to_test = streams_to_test - {'cards_image_conversation', 'cards_video_conversation', 'cards_image_direct_message',
                                            'cards_video_direct_message', 'accounts_daily_report', 'campaigns_daily_report', 'accounts',
                                            'targeting_tv_markets', 'targeting_tv_shows'}

        # Invalid bookmark for tweets stream - https://jira.talendforge.org/browse/TDL-18465
        streams_to_test = streams_to_test - {'tweets'}

        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        new_states = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(
            first_sync_bookmarks)
        for stream, new_state in simulated_states.items():
            new_states['bookmarks'][stream] = new_state
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Test By Stream
        ##########################################################################


        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(
                                           stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                first_bookmark_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream).get(self.account_id)
                second_bookmark_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream).get(self.account_id)


                if expected_replication_method == self.INCREMENTAL:

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(
                        iter(expected_replication_keys[stream]))
                    first_bookmark_value_utc = self.convert_state_to_utc(
                        first_bookmark_value)
                    second_bookmark_value_utc = self.convert_state_to_utc(
                        second_bookmark_value)


                    simulated_bookmark_value = self.convert_state_to_utc(new_states['bookmarks'][stream][self.account_id])

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    # assumes no changes to data during test
                    self.assertEqual(second_bookmark_value, first_bookmark_value)

                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = self.convert_state_to_utc(record.get(replication_key))

                        self.assertLessEqual(
                            replication_key_value, first_bookmark_value_utc,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = self.convert_state_to_utc(record.get(replication_key))

                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                                                msg="Second sync records do not repect the previous bookmark.")

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value, second_bookmark_value_utc,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_value)
                    self.assertIsNone(second_bookmark_value)

                    # Verify the number of records in the second sync is the same as the first

                    # `targeting_criteria` is child streams of parent stream `line_items` and `line_items` is an incremental streams
                    # Child stream also behave like incremental streams but does not save it's own state. So, it don't
                    # have same no of record on second sync and first sync.
                    if stream in ['targeting_criteria']:
                        continue

                    self.assertEqual(second_sync_count, first_sync_count)

                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method)
                    )

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
