import unittest
from unittest import mock
from tap_twitter_ads.schema import get_schemas
from tap_twitter_ads import streams
from singer import metadata


class TestParentTapStreamIdMetadata(unittest.TestCase):
    """Unit tests for parent-tap-stream-id metadata functionality."""

    def test_parent_tap_stream_id_added_to_child_streams(self):
        """Test that parent-tap-stream-id is added to metadata for child streams."""
        # Mock reports (empty since we're testing stream metadata)
        reports = []
        
        # Get schemas and metadata
        schemas, field_metadata = get_schemas(reports)
        
        # Test targeting_criteria (child of line_items)
        self.assertIn('targeting_criteria', field_metadata)
        targeting_criteria_metadata = field_metadata['targeting_criteria'] 
        
        # Convert to map for easier access
        mdata_map = metadata.to_map(targeting_criteria_metadata)
        
        # Verify parent-tap-stream-id is set
        parent_stream_id = metadata.get(mdata_map, (), 'parent-tap-stream-id')
        self.assertEqual(parent_stream_id, 'line_items')
        
        # Test targeting_tv_shows (child of targeting_tv_markets)
        self.assertIn('targeting_tv_shows', field_metadata)
        targeting_tv_shows_metadata = field_metadata['targeting_tv_shows']
        
        # Convert to map for easier access
        mdata_map = metadata.to_map(targeting_tv_shows_metadata)
        
        # Verify parent-tap-stream-id is set
        parent_stream_id = metadata.get(mdata_map, (), 'parent-tap-stream-id')
        self.assertEqual(parent_stream_id, 'targeting_tv_markets')

    def test_parent_tap_stream_id_not_added_to_parent_streams(self):
        """Test that parent-tap-stream-id is not added to parent streams."""
        # Mock reports (empty since we're testing stream metadata)
        reports = []
        
        # Get schemas and metadata
        schemas, field_metadata = get_schemas(reports)
        
        # Test line_items (parent stream)
        self.assertIn('line_items', field_metadata)
        line_items_metadata = field_metadata['line_items']
        
        # Convert to map for easier access
        mdata_map = metadata.to_map(line_items_metadata)
        
        # Verify parent-tap-stream-id is not set
        parent_stream_id = metadata.get(mdata_map, (), 'parent-tap-stream-id')
        self.assertIsNone(parent_stream_id)
        
        # Test targeting_tv_markets (parent stream)
        self.assertIn('targeting_tv_markets', field_metadata)
        targeting_tv_markets_metadata = field_metadata['targeting_tv_markets']
        
        # Convert to map for easier access
        mdata_map = metadata.to_map(targeting_tv_markets_metadata)
        
        # Verify parent-tap-stream-id is not set
        parent_stream_id = metadata.get(mdata_map, (), 'parent-tap-stream-id')
        self.assertIsNone(parent_stream_id)

    def test_parent_tap_stream_id_not_added_to_regular_streams(self):
        """Test that parent-tap-stream-id is not added to regular (non-child) streams.""" 
        # Mock reports (empty since we're testing stream metadata)
        reports = []
        
        # Get schemas and metadata
        schemas, field_metadata = get_schemas(reports)
        
        # Test a regular stream (accounts)
        self.assertIn('accounts', field_metadata)
        accounts_metadata = field_metadata['accounts']
        
        # Convert to map for easier access
        mdata_map = metadata.to_map(accounts_metadata)
        
        # Verify parent-tap-stream-id is not set
        parent_stream_id = metadata.get(mdata_map, (), 'parent-tap-stream-id')
        self.assertIsNone(parent_stream_id)

    def test_stream_classes_have_correct_parent_stream_attribute(self):
        """Test that child stream classes have the correct parent_stream attribute."""
        # Test TargetingCriteria
        targeting_criteria = streams.TargetingCriteria()
        self.assertTrue(hasattr(targeting_criteria, 'parent_stream'))
        self.assertEqual(targeting_criteria.parent_stream, 'line_items')
        
        # Test TargetingTVShows
        targeting_tv_shows = streams.TargetingTVShows()
        self.assertTrue(hasattr(targeting_tv_shows, 'parent_stream'))
        self.assertEqual(targeting_tv_shows.parent_stream, 'targeting_tv_markets')
        
        # Test that parent streams don't have parent_stream attribute
        line_items = streams.LineItems()
        self.assertFalse(hasattr(line_items, 'parent_stream'))
        
        targeting_tv_markets = streams.TargetingTvMarkets()
        self.assertFalse(hasattr(targeting_tv_markets, 'parent_stream'))


if __name__ == '__main__':
    unittest.main()