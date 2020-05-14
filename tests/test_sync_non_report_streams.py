"""
Test tap combined
"""

import unittest
from datetime import datetime, timedelta
import os
from .test_configuration import config
from tap_tester import menagerie
import tap_tester.runner as runner
import tap_tester.connections as connections


class TestSyncNonReportStreams(unittest.TestCase):
    """ Test the non-report streams """

    def name(self):
        return config['test_name']

    def tap_name(self):
        """The name of the tap"""
        return config['tap_name']

    def get_type(self):
        """the expected url route ending"""
        return config['type']

    def expected_check_streams(self):
        return set(config['streams'].keys())

    def expected_sync_streams(self):
        return set(config['streams'].keys())

    def expected_pks(self):
        return config['streams']

    def get_properties(self):
        """Configuration properties required for the tap."""
        properties_dict = {}
        props = config['properties']
        for prop in props:
            properties_dict[prop] = os.getenv(props[prop])

        return properties_dict

    def get_credentials(self):
        """Authentication information for the test account. Username is expected as a property."""
        credentials_dict = {}
        creds = config['credentials']
        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def setUp(self):
        missing_envs = []
        props = config['properties']
        creds = config['credentials']

        for prop in props:
            if os.getenv(props[prop]) == None:
                missing_envs.append(prop)
        for cred in creds:
            if os.getenv(creds[cred]) == None:
                missing_envs.append(cred)

        if len(missing_envs) != 0:
            raise Exception("set " + ", ".join(missing_envs))

    def test_run(self):

        conn_id = connections.ensure_connection(self, payload_hook=None)

        # Run the tap in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify the check's exit status
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify that there are catalogs found
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset(found_catalog_names)
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")
        #
        # # Select some catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for catalog in our_catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], [])

        # # Verify that all streams sync at least one row for initial sync
        # # This test is also verifying access token expiration handling. If test fails with
        # # authentication error, refresh token was not replaced after expiring.
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(),
                                                                   self.expected_pks())
        zero_count_streams = {k for k, v in record_count_by_stream.items() if v == 0}
        self.assertFalse(zero_count_streams,
                         msg="The following streams did not sync any rows {}".format(zero_count_streams))

        # Verify that bookmark values are correct after incremental sync
        bookmark_props = config['bookmark']
        current_state = menagerie.get_state(conn_id)
        # Get a datetime 5 days prior to now
        start_time = datetime.today() - timedelta(days=5)
        new_date = start_time.strftime("%Y-%m-%dT00:00:00Z")
        # Set the current state to 5 days ago
        current_state['bookmarks'][bookmark_props['bookmark_key']][bookmark_props['bookmark_site']]['web'] = new_date
        # Run the incremental sync
        runner.run_sync_mode(self, conn_id)
        current_state = menagerie.get_state(conn_id)
        test_bookmark = current_state.get('bookmarks', {}).get(bookmark_props['bookmark_key'], {}).get(
            bookmark_props['bookmark_site'], {}).get('web')
        test_bookmark_datetime = datetime.strptime(test_bookmark, '%Y-%m-%dT%H:%M:%S.%fZ')
        # Ensure that after the incremental sync the bookmark's date has progressed forward
        self.assertTrue(test_bookmark_datetime > start_time,
                        msg="The bookmark value does not match the expected result")
