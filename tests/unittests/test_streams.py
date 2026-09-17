import unittest

from tap_twitter_ads.streams import (
    STREAMS,
    Stream,
    SingletonStream,
    SingletonListStream,
    ParentStream,
    ConfigIdsStream,
    ConfigIdsSelfDefaultStream,
    ConfigLoopStream,
    ConfigLoopMultiStream,
    SearchStream,
    UsersMe,
    UserTweets,
    ListById,
    ListTweets,
    SpaceById,
    SpacesByCreatorIds,
)


class TestStreamClassHierarchy(unittest.TestCase):
    def test_every_stream_is_a_distinct_class_instance(self):
        # 46 stream classes registered, one instance each, no accidental sharing.
        classes = {type(s) for s in STREAMS.values()}
        self.assertEqual(len(classes), len(STREAMS))

    def test_category_base_classes_set_source_type(self):
        self.assertEqual(SingletonStream.source_type, 'singleton')
        self.assertEqual(SingletonListStream.source_type, 'singleton_list')
        self.assertEqual(ParentStream.source_type, 'parent')
        self.assertEqual(ConfigIdsStream.source_type, 'config_ids')
        self.assertEqual(ConfigIdsSelfDefaultStream.source_type, 'config_ids_self_default')
        self.assertEqual(ConfigLoopStream.source_type, 'config_loop')
        self.assertEqual(ConfigLoopMultiStream.source_type, 'config_loop_multi')
        self.assertEqual(SearchStream.source_type, 'search')

    def test_concrete_streams_inherit_correct_category(self):
        self.assertIsInstance(STREAMS['users_me'], SingletonStream)
        self.assertIsInstance(STREAMS['user_tweets'], ParentStream)
        self.assertIsInstance(STREAMS['list_by_id'], ConfigLoopStream)
        self.assertIsInstance(STREAMS['spaces_by_creator_ids'], ConfigIdsSelfDefaultStream)
        self.assertIsInstance(STREAMS['spaces_by_creator_ids'], ConfigIdsStream)  # inherits transitively

    def test_parent_property_only_true_for_parent_source_type(self):
        # Regression guard: a config_loop stream's source_key is a CONFIG FIELD
        # name, not a parent stream id, and must never be exposed as `.parent`
        # (doing so previously caused those streams to be silently skipped).
        self.assertEqual(UserTweets().parent, 'users_me')
        self.assertIsNone(ListById().parent)
        self.assertIsNone(SpaceById().parent)
        self.assertEqual(ListTweets().parent, 'list_by_id')

    def test_schema_file_always_equals_tap_stream_id(self):
        for name, stream in STREAMS.items():
            self.assertEqual(stream.schema_file, name)

    def test_registry_contains_no_abstract_category_base_classes(self):
        registered_classes = {type(s) for s in STREAMS.values()}
        abstract_bases = {Stream, SingletonStream, SingletonListStream, ParentStream, ConfigIdsStream,
                           ConfigIdsSelfDefaultStream, ConfigLoopStream, ConfigLoopMultiStream, SearchStream}
        self.assertEqual(registered_classes & abstract_bases, set())
        self.assertIsNone(Stream.tap_stream_id)


if __name__ == '__main__':
    unittest.main()
