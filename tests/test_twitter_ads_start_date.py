from tap_tester import LOGGER
import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie

from base import TwitterAds

class StartDateTest(TwitterAds):
    """
    Ensure incremental streams respect the start date. Run tap in check mode,
    run 1st sync with start date = 2020-01-01T00:00:00Z, run check mode and
    2nd sync on a new connection with a later start date.
    """

    def name(self):
        return "tap_tester_twitter_ads_start_date_test"

    start_date_1 = ""
    start_date_2 = ""

    def test_run(self):
        # Only INCREMENTAL streams meaningfully "obey" start_date - FULL_TABLE
        # streams always replicate everything regardless of start_date (see
        # `expected_metadata()`'s `OBEYS_START_DATE` flag in base.py).
        streams_to_test = {stream for stream, obeys in
                           ((s, props.get(self.OBEYS_START_DATE)) for s, props in self.expected_metadata().items())
                           if obeys}

        self.run_start_date(streams_to_test, new_start_date="2024-01-01T00:00:00Z")

    def run_start_date(self, streams_to_test, new_start_date, page_size = 100):
        """
        Test that the start_date configuration is respected
        • verify that a sync with a later start date has at least one record synced
        and fewer records than the 1st sync with a previous start date
        • verify that each stream has fewer records than the earlier start date sync
        • verify all data from later start data has bookmark values >= start_date
        """

        expected_replication_methods = self.expected_replication_method()
        self.PAGE_SIZE = page_size

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

        LOGGER.info("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(
            self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        # Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(
            conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # run second sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                replication_key = list(self.expected_replication_keys()[stream])[0]

                # record counts
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)

                # verify that at least one record was replicated for each sync
                self.assertGreater(record_count_sync_1, 0)
                self.assertGreater(record_count_sync_2, 0)

                # verify that the second sync (later start_date) replicated
                # fewer-or-equal records than the first (earlier start_date)
                self.assertLessEqual(record_count_sync_2, record_count_sync_1)

                # verify all replication key values in the 2nd sync are >= the new start date
                target_mark_2 = synced_records_2.get(stream)
                target_value_2 = [row.get('data').get(replication_key) for row in
                                  target_mark_2.get('messages') if row.get('action') == 'upsert']
                for value in target_value_2:
                    self.assertGreaterEqual(
                        self.convert_state_to_utc(value), self.convert_state_to_utc(self.start_date_2),
                        msg="Record replicated with a replication-key value earlier than the new start_date"
                    )
