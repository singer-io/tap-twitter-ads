import unittest
from unittest import mock
from tap_twitter_ads.schema import get_schemas
from tap_twitter_ads import streams
from singer import metadata


class TestParentTapStreamIdMetadata(unittest.TestCase):
    """Unit tests for parent-tap-stream-id metadata functionality."""

    def test_parent_tap_stream_id_added_to_child_streams(self):
        """Test that parent-tap-stream-id is added to metadata for child streams."""
        from tap_twitter_ads.streams import STREAMS
        
        # Mock reports (empty since we're testing stream metadata)
        reports = []

        # Get schemas and metadata
        schemas, field_metadata = get_schemas(reports)
        
        # Dynamically find all child streams (streams with parent_stream attribute)
        expected_child_streams = {}
        for stream_name, stream_class in STREAMS.items():
            if hasattr(stream_class, 'parent_stream'):
                expected_child_streams[stream_name] = stream_class.parent_stream
        
        # Test each child stream dynamically
        for child_stream, expected_parent in expected_child_streams.items():
            with self.subTest(child_stream=child_stream, expected_parent=expected_parent):
                # Verify child stream exists in metadata
                self.assertIn(child_stream, field_metadata, 
                             f"Child stream '{child_stream}' not found in field metadata")
                
                child_metadata = field_metadata[child_stream]
                
                # Convert to map for easier access
                mdata_map = metadata.to_map(child_metadata)
                
                # Verify parent-tap-stream-id is set correctly
                actual_parent_stream_id = metadata.get(mdata_map, (), 'parent-tap-stream-id')
                self.assertEqual(actual_parent_stream_id, expected_parent,
                               f"Child stream '{child_stream}' should have parent-tap-stream-id '{expected_parent}' "
                               f"but got '{actual_parent_stream_id}'")

    def test_parent_tap_stream_id_not_added_to_parent_streams(self):
        """Test that parent-tap-stream-id is not added to parent streams."""
        from tap_twitter_ads.streams import STREAMS
        
        # Mock reports (empty since we're testing stream metadata)
        reports = []
        
        # Get schemas and metadata
        schemas, field_metadata = get_schemas(reports)
        
        # Dynamically find all parent streams (streams that are referenced as parents)
        child_streams = {}
        for stream_name, stream_class in STREAMS.items():
            if hasattr(stream_class, 'parent_stream'):
                child_streams[stream_name] = stream_class.parent_stream
        
        parent_streams = set(child_streams.values())
        
        # Test each parent stream dynamically
        for parent_stream in parent_streams:
            with self.subTest(parent_stream=parent_stream):
                # Verify parent stream exists in metadata
                self.assertIn(parent_stream, field_metadata,
                             f"Parent stream '{parent_stream}' not found in field metadata")
                
                parent_metadata = field_metadata[parent_stream]
                
                # Convert to map for easier access
                mdata_map = metadata.to_map(parent_metadata)
                
                # Verify parent-tap-stream-id is not set for parent streams
                parent_stream_id = metadata.get(mdata_map, (), 'parent-tap-stream-id')
                self.assertIsNone(parent_stream_id,
                                 f"Parent stream '{parent_stream}' should not have parent-tap-stream-id "
                                 f"but got '{parent_stream_id}'")

    def test_stream_classes_have_correct_parent_stream_attribute(self):
        """Test that child stream classes have the correct parent_stream attribute."""
        from tap_twitter_ads.streams import STREAMS
        
        # Dynamically test all stream classes
        child_streams = {}
        parent_streams = set()
        regular_streams = set()
        
        for stream_name, stream_class in STREAMS.items():
            if hasattr(stream_class, 'parent_stream'):
                child_streams[stream_name] = stream_class.parent_stream
                parent_streams.add(stream_class.parent_stream)
            else:
                regular_streams.add(stream_name)
        
        # Test child stream classes have parent_stream attribute
        for stream_name, expected_parent in child_streams.items():
            with self.subTest(stream_name=stream_name, stream_type="child"):
                stream_class = STREAMS[stream_name]
                stream_instance = stream_class()
                
                self.assertTrue(hasattr(stream_instance, 'parent_stream'),
                              f"Child stream class '{stream_name}' should have parent_stream attribute")
                self.assertEqual(stream_instance.parent_stream, expected_parent,
                               f"Child stream '{stream_name}' should have parent_stream '{expected_parent}' "
                               f"but got '{stream_instance.parent_stream}'")
        
        # Test that parent streams don't have parent_stream attribute
        for stream_name in parent_streams:
            if stream_name in STREAMS:  # Only test if the parent stream is in STREAMS
                with self.subTest(stream_name=stream_name, stream_type="parent"):
                    stream_class = STREAMS[stream_name]
                    stream_instance = stream_class()
                    
                    self.assertFalse(hasattr(stream_instance, 'parent_stream'),
                                   f"Parent stream class '{stream_name}' should not have parent_stream attribute")
        
        # Test that regular (non-child, non-parent) streams don't have parent_stream attribute
        regular_non_parent_streams = regular_streams - parent_streams
        for stream_name in regular_non_parent_streams:
            with self.subTest(stream_name=stream_name, stream_type="regular"):
                stream_class = STREAMS[stream_name]
                stream_instance = stream_class()
                
                self.assertFalse(hasattr(stream_instance, 'parent_stream'),
                               f"Regular stream class '{stream_name}' should not have parent_stream attribute")
