import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import TwitterAds
import json

class PaginationTest(TwitterAds):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    def name(self):
        return "tap_tester_twitter_ads_pagination_test"

    def test_run(self):
        """
        • Verify that for each stream you can get multiple pages of data.  
        This requires we ensure more than 1 page of data exists at all times for any given stream.
        • Verify by pks that the data replicated matches the data we expect.
        """
        expected_streams = self.expected_streams()

        # For following streams, we are not able to generate enough records. So, skipping those streams from test case.
        expected_streams = expected_streams - {'cards_image_conversation', 'cards_video_conversation', 'cards_image_direct_message',
                                            'cards_video_direct_message', 'accounts_daily_report', 'campaigns_daily_report',
                                            'promoted_accounts', 'cards_image_direct_message', 'account_media', 'targeting_platforms',
                                            'funding_instruments', 'promotable_users', 'accounts', 'tailored_audiences',
                                           'targeting_tv_markets', 'targeting_tv_shows'}

        # Skipping `content_catagories` as It does not follow pagination.
        expected_streams = expected_streams - {'content_categories'}

        # Reduce page_size to 2 due to less data.
        self.run_test(expected_streams=expected_streams - {"targeting_locations", "targeting_conversations"}, page_size=2)    
        
        # Set page_size to 1000 for following streams because these streams contain more than 40000 records.
        # So, page_size of 2 get a lot of time to get all records.
        self.run_test(expected_streams={"targeting_locations", "targeting_conversations"}, page_size=1000)

    def run_test(self, expected_streams, page_size):

        streams_to_test = expected_streams

        self.PAGE_SIZE = page_size

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
         
                # verify that we can paginate with all fields selected
                record_count_sync = record_count_by_stream.get(stream, 0)
                self.assertGreater(record_count_sync, page_size,
                                    msg="The number of records is not over the stream max limit")

                primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in expected_primary_keys])
                                        for message in synced_records.get(stream).get('messages')
                                        if message.get('action') == 'upsert']

                primary_keys_list_1 = primary_keys_list[:page_size]
                primary_keys_list_2 = primary_keys_list[page_size:2*page_size]

                primary_keys_page_1 = set(primary_keys_list_1)
                primary_keys_page_2 = set(primary_keys_list_2)

                # Verify by primary keys that data is unique for page
                self.assertTrue(
                    primary_keys_page_1.isdisjoint(primary_keys_page_2))
