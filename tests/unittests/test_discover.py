import unittest

from tap_twitter_ads.discover import discover
from tap_twitter_ads.streams import STREAMS


class TestDiscover(unittest.TestCase):
    def test_discover_returns_one_entry_per_stream(self):
        catalog = discover()
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertEqual(stream_ids, set(STREAMS.keys()))

    def test_every_stream_schema_loads_without_error(self):
        catalog = discover()
        for entry in catalog.streams:
            schema_dict = entry.schema.to_dict()
            self.assertIn('properties', schema_dict)
            self.assertTrue(len(schema_dict['properties']) > 0, entry.tap_stream_id)

    def test_key_properties_match_stream_definition(self):
        catalog = discover()
        by_id = {entry.tap_stream_id: entry for entry in catalog.streams}
        for name, stream in STREAMS.items():
            self.assertEqual(by_id[name].key_properties, stream.key_properties, name)

    def test_child_streams_have_parent_tap_stream_id_metadata(self):
        catalog = discover()
        by_id = {entry.tap_stream_id: entry for entry in catalog.streams}
        for name, stream in STREAMS.items():
            if not stream.parent:
                continue
            table_md = next(m['metadata'] for m in by_id[name].metadata if m['breadcrumb'] == ())
            self.assertEqual(table_md.get('parent-tap-stream-id'), stream.parent, name)

    def test_incremental_streams_have_replication_key_marked_automatic(self):
        catalog = discover()
        by_id = {entry.tap_stream_id: entry for entry in catalog.streams}
        for name, stream in STREAMS.items():
            if stream.replication_method != 'INCREMENTAL':
                continue
            field_md = next(
                m['metadata'] for m in by_id[name].metadata
                if m['breadcrumb'] == ('properties', stream.replication_key))
            self.assertEqual(field_md.get('inclusion'), 'automatic', name)


if __name__ == '__main__':
    unittest.main()
