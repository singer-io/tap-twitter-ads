import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import TwitterAds

class AutomaticFieldsTest(TwitterAds):
    """
    Ensure running the tap with all streams selected and all fields deselected results in the replication of just the 
    primary keys and replication keys (automatic fields).
    """
    
    def name(self):
        return "tap_tester_twitter_ads_automatic_fields_test"

    def test_run(self):
        """
        • Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        • Verify that only the automatic fields are sent to the target.
        • Verify that all replicated records have unique primary key values.
        """

        streams_to_test = self.expected_streams()
        
        # For following streams(except targeting_tv_markets and targeting_tv_shows), we are not able to generate any records.
        # targeting_tv_markets and targeting_tv_shows streams take more than 5 hour to complete the sync.
        #  So, skipping those streams from test case.
        streams_to_test = streams_to_test - {'cards_image_conversation', 'cards_video_conversation', 'cards_image_direct_message',
                                            'cards_video_direct_message', 'accounts_daily_report', 'campaigns_daily_report', 
                                            'targeting_tv_markets', 'targeting_tv_shows'}

        # Set page_size to 1000 for following streams because these streams contain more than 40000 records.
        # So, page_size of 200 get a lot of time to get all records.
        self.run_test(streams_to_test={"targeting_locations", "targeting_conversations"}, page_size=1000)

        # For, some of the streams the maximum allowed page_size is 200. For, the greater value of page_size SDK throws the error.
        # So, revert back page_size to 200 for the rest of the streams.
        streams_to_test = streams_to_test - {"targeting_locations", "targeting_conversations"}
        self.run_test(streams_to_test, page_size=200)

    def run_test(self, streams_to_test, page_size):

        self.PAGE_SIZE = page_size

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('tap_stream_id') in streams_to_test]

        # Select all streams and no fields within streams
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = self.expected_automatic_fields().get(stream)
                expected_primary_keys = self.expected_primary_keys()[stream]

                # collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row['data'].keys())
                                        for row in data.get('messages', [])]
                primary_keys_list = [tuple(message.get('data', {}).get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in data.get('messages', [])
                                       if message.get('action') == 'upsert']
                unique_primary_keys_list = set(primary_keys_list)

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream min limit")

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                #Verify that all replicated records have unique primary key values.
                self.assertEqual(len(primary_keys_list),
                                    len(unique_primary_keys_list),
                                    msg="Replicated record does not have unique primary key values.")
