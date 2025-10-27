"""Test parent-tap-stream-id metadata functionality."""
import unittest
from tap_tester import menagerie, connections
from base import TwitterAds


class ParentTapStreamIdTest(TwitterAds):
    """Test parent-tap-stream-id metadata is correctly set for child streams."""

    @staticmethod
    def name():
        return "tap_tester_twitter_ads_parent_tap_stream_id_test"

    def test_parent_tap_stream_id_metadata(self):
        """
        Test that parent-tap-stream-id metadata is correctly set for child streams.
        
        • Verify that child streams have parent-tap-stream-id metadata
        • Verify that parent-tap-stream-id points to the correct parent stream
        • Verify that parent streams do not have parent-tap-stream-id metadata
        • Verify that the parent stream exists in the catalog
        """
        expected_parent_child_relationships = self.expected_parent_streams()
        all_streams = self.expected_streams()
        
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        
        # Create a mapping of stream names to catalogs for easy lookup
        catalog_map = {catalog['stream_name']: catalog for catalog in found_catalogs}
        
        # Test each child stream
        for child_stream, expected_parent_stream in expected_parent_child_relationships.items():
            with self.subTest(child_stream=child_stream, parent_stream=expected_parent_stream):
                # Verify child stream exists in catalog
                self.assertIn(child_stream, catalog_map, 
                             f"Child stream '{child_stream}' not found in catalog")
                
                # Verify parent stream exists in catalog
                self.assertIn(expected_parent_stream, catalog_map,
                             f"Parent stream '{expected_parent_stream}' not found in catalog")
                
                # Get child stream metadata
                child_catalog = catalog_map[child_stream]
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, child_catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                
                # Get table-level metadata
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                self.assertEqual(len(stream_properties), 1, 
                               f"Expected exactly one table-level metadata entry for {child_stream}")
                
                # Verify parent-tap-stream-id is set correctly
                actual_parent_stream = stream_properties[0].get("metadata", {}).get("parent-tap-stream-id")
                self.assertEqual(expected_parent_stream, actual_parent_stream,
                               f"Child stream '{child_stream}' should have parent-tap-stream-id '{expected_parent_stream}' but got '{actual_parent_stream}'")
        
        # Test that parent streams do not have parent-tap-stream-id metadata
        parent_streams = set(expected_parent_child_relationships.values())
        for parent_stream in parent_streams:
            with self.subTest(parent_stream=parent_stream):
                # Get parent stream metadata
                parent_catalog = catalog_map[parent_stream]
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, parent_catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                
                # Get table-level metadata
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                self.assertEqual(len(stream_properties), 1,
                               f"Expected exactly one table-level metadata entry for {parent_stream}")
                
                # Verify parent-tap-stream-id is not set for parent streams
                actual_parent_stream = stream_properties[0].get("metadata", {}).get("parent-tap-stream-id")
                self.assertIsNone(actual_parent_stream,
                                f"Parent stream '{parent_stream}' should not have parent-tap-stream-id but got '{actual_parent_stream}'")
        
        # Test that non-child streams do not have parent-tap-stream-id metadata
        non_child_streams = all_streams - set(expected_parent_child_relationships.keys())
        for stream in non_child_streams:
            # Skip parent streams as they are already tested above
            if stream in parent_streams:
                continue
                
            with self.subTest(stream=stream):
                if stream in catalog_map:  # Some streams might not be in catalog due to configuration
                    catalog = catalog_map[stream]
                    schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                    metadata = schema_and_metadata["metadata"]
                    
                    # Get table-level metadata
                    stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                    if stream_properties:  # Some streams might not have table-level metadata
                        actual_parent_stream = stream_properties[0].get("metadata", {}).get("parent-tap-stream-id")
                        self.assertIsNone(actual_parent_stream,
                                        f"Non-child stream '{stream}' should not have parent-tap-stream-id but got '{actual_parent_stream}'")


if __name__ == '__main__':
    unittest.main()