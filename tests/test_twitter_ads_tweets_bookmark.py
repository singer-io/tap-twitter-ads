import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
import dateutil.parser
from datetime import timedelta
from datetime import datetime as dt

from base import TwitterAds

class BookmarkTest(TwitterAds):
    """Test tap sets a separate bookmark for PUBLISHED and SCHEDULED tweets and 
    respects it for the next sync of a tweets stream"""
    
    def name(self):
        return "tap_tester_twitter_ads_bookmark_test"

    def test_run(self):
        
        # Run test with the bookmark of SCHEDULED subtype in the state.
        self.run_bookmark(True)
        
        # Run test without the bookmark of SCHEDULED subtype in the state.
        self.run_bookmark(False)

    def run_bookmark(self, add_schedule_bookmark):
        """
        Verify that for each stream you can do a sync that records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less than the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)
        PREREQUISITE
        For EACH stream that is incrementally replicated, there are multiple rows of data with
            different values for the replication key
        """
        
        streams_to_test = {'tweets'}
        expected_replication_keys = self.expected_replication_keys()

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

        new_states = {'bookmarks': {'tweets': {}}}
        
        datetime_format = "%a %b %d %H:%M:%S %z %Y"
        
        # state for tweet_type of PUBLISHED
        published_tweets_state_as_datetime = dateutil.parser.parse(first_sync_bookmarks['bookmarks']['tweets']['PUBLISHED'])
        calculated_published_state_as_datetime = published_tweets_state_as_datetime - timedelta(days=0, hours=0, minutes=1)
        
        # simulate state for PUBLISHED tweets
        calculated_published_state_formatted = dt.strftime(calculated_published_state_as_datetime, datetime_format)

        # state for tweet_type of SCHEDULED
        scheduled_tweets_state_as_datetime = dateutil.parser.parse(first_sync_bookmarks['bookmarks']['tweets']['SCHEDULED'])
        calculated_scheduled_state_as_datetime = scheduled_tweets_state_as_datetime - timedelta(days=0, hours=0, minutes=1)

        # simulate state for SCHEDULED tweets
        calculated_scheduled_state_formatted = dt.strftime(calculated_scheduled_state_as_datetime, datetime_format)

        new_states['bookmarks']['tweets']['PUBLISHED'] = calculated_published_state_formatted
        # Add bookmark value of SCHEDULED subtype in the state.
        if add_schedule_bookmark:
            new_states['bookmarks']['tweets']['SCHEDULED'] = calculated_scheduled_state_formatted

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

                # collect information for assertions from syncs 1 & 2 base on expected values
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(
                                           stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                # Get bookmark for PUBLISHED tweets
                first_published_bookmark_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream, {}).get('PUBLISHED')
                second_published_bookmark_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream, {}).get('PUBLISHED')
                
                # Get bookmark for SCHEDULED tweets
                first_scheduled_bookmark_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream, {}).get('SCHEDULED')
                second_scheduled_bookmark_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream, {}).get('SCHEDULED')

                # collect information specific to incremental streams from syncs 1 & 2
                replication_key = next(
                    iter(expected_replication_keys[stream]))
                first_published_bookmark_value_utc = self.convert_state_to_utc(
                    first_published_bookmark_value)
                second_published_bookmark_value_utc = self.convert_state_to_utc(
                    second_published_bookmark_value)

                first_scheduled_bookmark_value_utc = self.convert_state_to_utc(
                    first_scheduled_bookmark_value)
                second_scheduled_bookmark_value_utc = self.convert_state_to_utc(
                    second_scheduled_bookmark_value)
                
                simulated_published_bookmark_value = self.convert_state_to_utc(new_states['bookmarks'][stream]['PUBLISHED'])
                
                # Records will respect the start_date in case of bookmark for SCHEDULED subtype is not present in state
                simulated_scheduled_bookmark_value = self.convert_state_to_utc(new_states['bookmarks'][stream]['SCHEDULED'] if add_schedule_bookmark else self.get_properties()["start_date"])
                
                # Verify the first sync sets both bookmarks of the expected form
                self.assertIsNotNone(first_published_bookmark_value_utc)
                self.assertIsNotNone(second_published_bookmark_value_utc)

                # Verify the second sync sets both bookmarks of the expected form
                self.assertIsNotNone(first_scheduled_bookmark_value_utc)
                self.assertIsNotNone(second_scheduled_bookmark_value_utc)

                # Verify the second sync bookmark is Equal to the first sync bookmark
                # assumes no changes to data during test
                self.assertEqual(second_published_bookmark_value_utc, first_published_bookmark_value_utc)
                self.assertEqual(second_scheduled_bookmark_value_utc, first_scheduled_bookmark_value_utc)

                for record in first_sync_messages:

                    # Verify the first sync bookmark value is the max replication key value for a given stream
                    replication_key_value = record.get(replication_key)
                    if record.get('tweet_type') == "PUBLISHED":
                        # Verify the first sync bookmark value is the max replication key value for a PUBLISHED tweets
                        self.assertLessEqual(
                            replication_key_value, first_published_bookmark_value_utc,
                            msg="First sync bookmark for PUBLISHED tweets was set incorrectly, a record with a greater replication-key value was synced."
                        )
                    else:
                        # Verify the first sync bookmark value is the max replication key value for a SCHEDULED tweets
                        self.assertLessEqual(
                            replication_key_value, first_scheduled_bookmark_value_utc,
                            msg="First sync bookmark for SCHEDULED tweets was set incorrectly, a record with a greater replication-key value was synced."
                        )
                
                for record in second_sync_messages:
                    # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                    replication_key_value = record.get(replication_key)


                    # Verify the second sync bookmark value is the max replication key value for a given stream
                    if record.get('tweet_type') == "PUBLISHED":
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark

                        self.assertGreaterEqual(replication_key_value, simulated_published_bookmark_value,
                                                msg="Second sync records do not repect the previous bookmark for PUBLISHED tweets.")

                        self.assertLessEqual(
                            replication_key_value, second_published_bookmark_value_utc,
                            msg="Second sync bookmark for PUBLISHED tweets was set incorrectly, a record with a greater replication-key value was synced."
                        )
                    else:
                        # Verify the first sync bookmark value is the max replication key value for a SCHEDULED tweets
                        
                        self.assertGreaterEqual(replication_key_value, simulated_scheduled_bookmark_value,
                                                msg="Second sync records do not repect the previous bookmark for SCHEDULED tweets.")
                        
                        self.assertLessEqual(
                            replication_key_value, second_scheduled_bookmark_value_utc,
                            msg="Second sync bookmark for SCHEDULED tweets was set incorrectly, a record with a greater replication-key value was synced."
                        )



                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))