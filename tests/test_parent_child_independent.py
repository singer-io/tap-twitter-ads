import tap_tester.connections as connections
import tap_tester.runner as runner

from base import TwitterAds

class ParentChildIndependentTest(TwitterAds):
    """
        Test case to verify that tap is working fine if only first-level child streams are selected
    """  
  
    def name(self):
        return "tap_tester_twitter_ads_parent_child_test"

    def test_run(self):
        """
            Testing that tap is working fine if only child streams are selected
            - Verify that if only child streams are selected then only child streams are replicated.
        """

        # Test for the case of child is selected and parent is not selected
        child_streams = {'targeting_criteria'}
        
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in child_streams]
        # table and field selection
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        # run initial sync
        self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(child_streams, synced_stream_names)
