import unittest

from tap_twitter_ads.schema import get_schemas
from tap_twitter_ads.streams import STREAMS
from singer import metadata


class TestParentTapStreamIdMetadata(unittest.TestCase):
    """Unit tests for parent-tap-stream-id metadata functionality.

    NOTE: unlike the old OAuth1 Ads tap (a `parent_stream` class attribute),
    the OAuth2 tap exposes this via the `Stream.parent` @property, which is
    only non-None for `source_type == 'parent'` streams (see streams.py's
    docstring - this distinction was the root cause of a real bug where
    'config_loop' streams were wrongly treated as having a parent)."""

    def test_parent_tap_stream_id_added_to_child_streams(self):
        """Test that parent-tap-stream-id is added to metadata for child streams."""
        schemas, field_metadata = get_schemas()

        expected_child_streams = {name: stream.parent for name, stream in STREAMS.items() if stream.parent}
        self.assertGreater(len(expected_child_streams), 0, "no child streams found to test against")

        for child_stream, expected_parent in expected_child_streams.items():
            with self.subTest(child_stream=child_stream, expected_parent=expected_parent):
                self.assertIn(child_stream, field_metadata,
                             f"Child stream '{child_stream}' not found in field metadata")

                mdata_map = metadata.to_map(field_metadata[child_stream])
                actual_parent_stream_id = metadata.get(mdata_map, (), 'parent-tap-stream-id')
                self.assertEqual(actual_parent_stream_id, expected_parent,
                               f"Child stream '{child_stream}' should have parent-tap-stream-id "
                               f"'{expected_parent}' but got '{actual_parent_stream_id}'")

    def test_parent_tap_stream_id_not_added_to_non_child_streams(self):
        """Test that parent-tap-stream-id is not added to streams with no parent
        (this includes true top-level streams AND 'config_loop' streams whose
        `source_key` is a config field name, not a parent stream id - the
        exact distinction the `.parent` property fix guards against)."""
        schemas, field_metadata = get_schemas()

        non_child_streams = {name for name, stream in STREAMS.items() if not stream.parent}
        self.assertGreater(len(non_child_streams), 0, "no non-child streams found to test against")

        for stream_name in non_child_streams:
            with self.subTest(stream_name=stream_name):
                self.assertIn(stream_name, field_metadata,
                             f"Stream '{stream_name}' not found in field metadata")

                mdata_map = metadata.to_map(field_metadata[stream_name])
                parent_stream_id = metadata.get(mdata_map, (), 'parent-tap-stream-id')
                self.assertIsNone(parent_stream_id,
                                 f"Stream '{stream_name}' should not have parent-tap-stream-id "
                                 f"but got '{parent_stream_id}'")

    def test_config_loop_streams_source_key_is_not_mistaken_for_a_parent(self):
        """Regression guard: config_loop streams (e.g. broadcast_by_id) have a
        `source_key` that names a CONFIG FIELD ('broadcast_ids'), not a
        stream id - `.parent` must be None for these even though
        `source_key` is set, otherwise they'd be silently skipped by
        sync.py's dispatch loop (see streams.py's `.parent` docstring)."""
        config_loop_streams = {name for name, stream in STREAMS.items() if stream.source_type == 'config_loop'}
        self.assertGreater(len(config_loop_streams), 0, "no config_loop streams found to test against")

        for stream_name in config_loop_streams:
            with self.subTest(stream_name=stream_name):
                stream = STREAMS[stream_name]
                self.assertIsNotNone(stream.source_key, f"{stream_name} should have a source_key")
                self.assertIsNone(stream.parent,
                                 f"config_loop stream '{stream_name}' must not expose "
                                 f".parent even though source_key='{stream.source_key}' is set")


if __name__ == '__main__':
    unittest.main()
