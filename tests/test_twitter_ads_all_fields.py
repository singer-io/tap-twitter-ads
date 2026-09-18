import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import TwitterAds
from tap_twitter_ads.schema import get_schemas

# Fields that the X API v2 schema declares but that this test account/query
# combination cannot be relied on to ever populate (e.g. fields only present
# for verified/paid-tier accounts, or fields gated behind scopes this app's
# OAuth 2.0 grant doesn't include). Add entries here as they're discovered
# against a real test account, same convention as the old Ads-API suite -
# empty for now since no stream-specific gaps have been confirmed yet.
KNOWN_MISSING_FIELDS = {}


class AllFieldsTest(TwitterAds):
    """Test that with all fields selected for a stream we replicate data as expected"""

    def name(self):
        return "tap_tester_twitter_ads_all_fields_test"

    def test_run(self):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream
        • Verify all fields for each stream are replicated (except any known-missing ones)
        """
        streams_to_test = self.expected_streams()
        schemas, _ = get_schemas()

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields, select_all_fields=True)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                expected_all_keys = set(schemas[stream].get('properties', {}).keys())
                expected_missing_fields = KNOWN_MISSING_FIELDS.get(stream, set())

                data = synced_records.get(stream, {})
                actual_all_keys = set()
                for message in data.get('messages', []):
                    if message.get('action') == 'upsert':
                        actual_all_keys.update(message.get('data').keys())

                # Verify all fields for a stream were replicated (except any
                # known-missing fields for this test account/query combo)
                self.assertGreater(len(actual_all_keys), len(self.expected_automatic_fields().get(stream, set())),
                                   msg="A stream synced with all fields selected should have "
                                       "more than just the automatic fields, stream: {}".format(stream))
                self.assertSetEqual(
                    expected_all_keys - expected_missing_fields - actual_all_keys, set(),
                    msg="Field(s) present in schema but never returned for stream {}".format(stream)
                )

