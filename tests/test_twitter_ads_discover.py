"""Test tap discover mode and metadata."""
import re

from tap_tester import menagerie, connections
from base import TwitterAds

class DiscoverTest(TwitterAds):
    """Test tap discover mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_twitter_ads_discover_test"

    def test_run(self):
        """
        Testing that discover creates the appropriate catalog with valid metadata.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify there are no duplicate metadata entries
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication keys are given the inclusion of automatic.
        • verify that all other fields have inclusion of available metadata.
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self)

        # Verify that there are catalogs found
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                      msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify ensure the caatalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = self.expected_automatic_fields()[stream]
                expected_replication_method = self.expected_replication_method()[stream]

                # collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_primary_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])
                )
                actual_replication_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])
                )
                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                )

                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                ##########################################################################
                ### metadata assertions
                ##########################################################################

                # verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # verify there are no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(set(actual_fields)), msg = "duplicates in the fields retrieved")

                # verify replication key(s) match expectations
                self.assertEqual(expected_replication_keys, actual_replication_keys,
                                 msg="expected replication key {} but actual is {}".format(
                                     expected_replication_keys, actual_replication_keys))

                # verify primary key(s) match expectations
                self.assertSetEqual(expected_primary_keys, actual_primary_keys,
                                 msg="expected primary key {} but actual is {}".format(
                                     expected_primary_keys, actual_primary_keys))

                # verify the replication method matches our expectations
                self.assertEqual(expected_replication_method, actual_replication_method,
                                    msg="The actual replication method {} doesn't match the expected {}".format(
                                        actual_replication_method, expected_replication_method))

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if stream == "targeting_criteria" or expected_replication_keys:
                    # `targeting_criteria` is child stream of line_items stream which is incremental.
                    # We are writing a separate bookmark for the child stream in which we are storing 
                    # the bookmark based on the parent's replication key.
                    # But, we are not using any fields from the child record for it.
                    # That's why the `targeting_criteria` stream does not have replication_key but still it is incremental.
                    self.assertEqual(self.INCREMENTAL, actual_replication_method)
                else:
                    self.assertEqual(self.FULL_TABLE, actual_replication_method)

                # verify that primary keys and replication keys
                # are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # verify that all other fields have inclusion of available
                field_metadata = [item for item in metadata if item["breadcrumb"] != []]
                expected_available_field_metadata = [fmd for fmd in field_metadata
                                                     if fmd["breadcrumb"][1] not in expected_automatic_fields]
                for item in expected_available_field_metadata:
                    with self.subTest(field=item["breadcrumb"][1]):
                        self.assertEqual("available", item["metadata"]["inclusion"])
