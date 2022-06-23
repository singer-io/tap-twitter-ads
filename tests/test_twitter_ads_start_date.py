import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie

from base import TwitterAds

class StartDateTest(TwitterAds):
    """
    Ensure both all expected streams respect the start date. Run tap in check mode, 
    run 1st sync with start date = 2019-03-01T00:00:00Z, run check mode and 2nd sync on a new connection with start date 2022-04-04T06:00:00Z.
    """
    
    def name(self):
        return "tap_tester_twitter_ads_start_date_test"

    start_date_1 = ""
    start_date_2 = ""

    def test_run(self):
        # Streams to verify start date tests
        streams_to_test = self.expected_streams()

        # For the following streams(except targeting_tv_markets and targeting_tv_shows), we are not able to generate any records.
        # targeting_tv_markets and targeting_tv_shows streams take more than 5 hour to complete the sync.
        #  So, skipping those streams from test case.
        streams_to_test = streams_to_test - {'cards_image_conversation', 'cards_video_conversation', 'cards_image_direct_message',
                                            'cards_video_direct_message', 'accounts_daily_report', 'campaigns_daily_report', 'accounts',
                                            'targeting_tv_markets', 'targeting_tv_shows'}

        # running start_date_test for `line_items` and `targeting_criteria` stream
        expected_stream_1 = {"line_items", "targeting_criteria"}
        self.run_start_date(expected_stream_1, new_start_date="2022-06-01T00:00:00Z")
        
        # running start_date_test for `targeting_events`
        expected_stream_2 = {'targeting_events'}
        self.run_start_date(expected_stream_2, new_start_date="2019-06-01T00:00:00Z")
        
        # running start_date_test for rest of the streams
        streams_to_test = streams_to_test - expected_stream_1 - expected_stream_2
        self.run_start_date(streams_to_test, new_start_date="2022-04-06T00:00:00Z")

    def run_start_date(self, streams_to_test, new_start_date):
        """
        Test that the start_date configuration is respected
        • verify that a sync with a later start date has at least one record synced
        and fewer records than the 1st sync with a previous start date
        • verify that each stream has fewer records than the earlier start date sync
        • verify all data from later start data has bookmark values >= start_date
        """

        expected_replication_methods = self.expected_replication_method()

        self.start_date_1 = self.get_properties().get('start_date')
        self.start_date_2 = new_start_date
        self.start_date = self.start_date_1

        ##########################################################################
        # First Sync
        ##########################################################################

        # instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        # run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('tap_stream_id') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(
            conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        # Update START DATE Between Syncs
        ##########################################################################
   
        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(
            self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        # Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(
            self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(
            conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)

                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream, {}).get('messages', [])
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream, {}).get('messages', [])
                                       if message.get('action') == 'upsert']

                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                if expected_replication_method == self.INCREMENTAL:

                    # collect information specific to incremental streams from syncs 1 & 2
                    expected_replication_key = next(
                        iter(self.expected_replication_keys().get(stream, [])))
                    replication_dates_1 = [row.get('data').get(expected_replication_key) for row in
                                        synced_records_1.get(stream, {'messages': []}).get('messages', [])
                                        if row.get('data')]
                    replication_dates_2 = [row.get('data').get(expected_replication_key) for row in
                                        synced_records_2.get(stream, {'messages': []}).get('messages', [])
                                        if row.get('data')]

                    # Verify replication key is greater or equal to start_date for sync 1
                    for replication_date in replication_dates_1:

                        self.assertGreaterEqual(
                            replication_date, self.start_date_1,
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(self.start_date_1) +
                                "Record date: {} ".format(replication_date)
                        )

                    # Verify replication key is greater or equal to start_date for sync 2
                    for replication_date in replication_dates_2:

                        self.assertGreaterEqual(
                            replication_date, self.start_date_2,
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(self.start_date_2) +
                                "Record date: {} ".format(replication_date)
                        )

                    # Verify the records replicated in sync 2 were also replicated in sync 1
                    self.assertTrue(
                        primary_keys_sync_2.issubset(primary_keys_sync_1))

                if self.expected_metadata()[stream][self.OBEYS_START_DATE]:
                    
                    # Verify the number of records replicated in sync 1 is greater than the number
                    # of records replicated in sync 2
                    self.assertGreater(record_count_sync_1,
                                       record_count_sync_2)
                else:
                    
                    # Verify that the 2nd sync with a later start date replicates the same number of
                    # records as the 1st sync.

                    self.assertEqual(record_count_sync_2, record_count_sync_1)

                    # Verify by primary key the same records are replicated in the 1st and 2nd syncs
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)