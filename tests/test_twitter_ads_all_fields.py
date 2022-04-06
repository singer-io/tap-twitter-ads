import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import TwitterAds

class AllFieldsTest(TwitterAds):
    """Ensure running the tap with all streams and fields selected results in the replication of all fields."""

    def name(self):
        return "tap_tester_twitter_ads_all_fields_test"

    def test_run(self):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream. 
        • verify all fields for each stream are replicated
        """
     
        # Streams to verify all fields tests
        streams_to_test = self.expected_streams()

        # For following streams(except targeting_tv_markets and targeting_tv_shows), we are not able to generate any records.
        # targeting_tv_markets and targeting_tv_shows streams take more than 5 hour to complete the sync.
        #  So, skipping those streams from test case.
        streams_to_test = streams_to_test - {'cards_image_conversation', 'cards_video_conversation', 'cards_image_direct_message',
                                            'cards_video_direct_message', 'accounts_daily_report', 'campaigns_daily_report', 
                                            'targeting_tv_markets', 'targeting_tv_shows'}
        
        
        # Endpoints are swapped for content_categories and iab_categories streams - https://jira.talendforge.org/browse/TDL-18374        
        streams_to_test = streams_to_test - {'iab_categories', 'targeting_tv_markets'}

        # Invalid endpoint for targeting_events stream - https://jira.talendforge.org/browse/TDL-18463
        streams_to_test = streams_to_test - {'targeting_events'}

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        # grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(
                fields_from_field_level_md)

        self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)
    
        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields.get(
                    stream, set())

                # Verify that more than just the automatic fields are replicated for each stream.
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))

                messages = synced_records.get(stream)
                # collect actual values
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())
                    
                # As we can't generate following fields by twitter-ads APIs and UI, so removed it form expectation list.
                if stream == "tailored_audiences":
                    expected_all_keys = expected_all_keys - {'is_owner'}
                elif stream == "accounts":
                    expected_all_keys = expected_all_keys - {'salt'}
                elif stream == "account_media":
                    # https://github.com/twitterdev/twitter-python-ads-sdk/blob/master/twitter_ads/creative.py#L95-#L103
                    expected_all_keys = expected_all_keys - {'total_budget_amount_local_micro', 'end_time', 'entity_status', 'currency', 
                                                             'reasons_not_servable', 'name', 'funding_instrument_id', 'duration_in_days', 
                                                             'daily_budget_amount_local_micro', 'servable', 'start_time', 'frequency_cap', 
                                                             'standard_delivery'}
                elif stream == "cards_image_app_download" or stream == "cards_video_app_download":
                    expected_all_keys = expected_all_keys - {'google_play_app_id'}
                elif stream == "tweets":
                    expected_all_keys = expected_all_keys - {'text', 'retweeted_status', 'extended_entities', 'reply_count'
                                                            ,'quoted_status', 'filter_level', 'quote_count', 'is_quote_status'
                                                            ,'quote_status_id_str', 'quote_status_id', 'possibly_sensitive', 
                                                            'matching_rules'}
                elif stream == "media_creatives":
                    expected_all_keys = expected_all_keys - {'serving_status'}
                elif stream == "line_items":
                    # https://twittercommunity.com/t/ads-api-version-9/150316#line-itemad-group-detail-changes-4
                    # https://twittercommunity.com/t/ads-api-version-10/158787
                    expected_all_keys = expected_all_keys - {'charge_by', 'tracking_tags', 'bid_unit', 'optimization', 'bid_type',
                                                             'automatically_select_bid'}
                elif stream == "cards_poll":
                    expected_all_keys = expected_all_keys - {'video_url', 'video_poster_url', 'video_hls_url', 'video_poster_width', 
                                                             'video_height', 'video_poster_height', 'content_duration_seconds', 'video_width'}
                
                elif stream == "targeting_criteria":
                    # https://twittercommunity.com/t/ads-api-version-9/150316/3
                    expected_all_keys = expected_all_keys - {'tailored_audience_expansion', 'tailored_audience_type'}

                # verify all fields for each stream are replicated
                self.assertSetEqual(expected_all_keys, actual_all_keys)
