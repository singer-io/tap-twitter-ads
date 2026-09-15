import unittest
import os
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
from datetime import datetime as dt
from datetime import timedelta
import dateutil.parser
import pytz


class TwitterAds(unittest.TestCase):
    """Base class for tap-tester integration tests against the OAuth 2.0 X
    API v2 tap. Streams/config/metadata below reflect tap_twitter_ads.streams
    (see that module's docstring for the authoritative config-field
    reference) - keep this in sync when streams.py changes."""

    start_date = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    BOOKMARK_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"
    OBEYS_START_DATE = "obey-start-date"
    PAGE_SIZE = 100
    # Resolved after the first `users_me` sync/lookup - used as the default
    # bookmark key for users_me-child streams (see calculated_states_by_stream).
    default_id = ""

    def tap_name(self):
        return "tap-twitter-ads"

    def get_type(self):
        return "platform.twitter-ads"

    def get_credentials(self):
        """OAuth 2.0 Authorization Code + PKCE credentials - unlike the old
        OAuth 1.0a Ads API, the refresh_token ROTATES on every use, so CI
        must persist whatever the tap writes back to config, not just re-read
        these fixed env vars on every run."""
        return {
            'client_id': os.getenv('TAP_TWITTER_ADS_CLIENT_ID'),
            'client_secret': os.getenv('TAP_TWITTER_ADS_CLIENT_SECRET'),
            'access_token': os.getenv('TAP_TWITTER_ADS_ACCESS_TOKEN'),
            'refresh_token': os.getenv('TAP_TWITTER_ADS_REFRESH_TOKEN'),
        }

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap. Optional
        config-driven streams (user_ids/usernames/tweet_ids/space_ids/
        list_ids/etc.) are sourced from env vars so CI can point them at
        real ids/queries the test account has access to - see README.md's
        "Configuration Reference" for which stream(s) each field activates."""

        return_value = {
            "start_date": "2020-01-01T00:00:00Z",
            "page_size": self.PAGE_SIZE,
            "user_ids": os.getenv("TAP_TWITTER_ADS_USER_IDS"),
            "usernames": os.getenv("TAP_TWITTER_ADS_USERNAMES"),
            "tweet_ids": os.getenv("TAP_TWITTER_ADS_TWEET_IDS"),
            "space_ids": os.getenv("TAP_TWITTER_ADS_SPACE_IDS"),
            "list_ids": os.getenv("TAP_TWITTER_ADS_LIST_IDS"),
            "media_keys": os.getenv("TAP_TWITTER_ADS_MEDIA_KEYS"),
            "woeids": os.getenv("TAP_TWITTER_ADS_WOEIDS", "1"),
            "users_search_query": os.getenv("TAP_TWITTER_ADS_USERS_SEARCH_QUERY"),
            "post_search_query": os.getenv("TAP_TWITTER_ADS_POST_SEARCH_QUERY"),
        }
        if original:
            return return_value

        # Reassign start date
        return_value["start_date"] = self.start_date
        return return_value

    def setUp(self):
        required_env = {
            "TAP_TWITTER_ADS_CLIENT_ID",
            "TAP_TWITTER_ADS_CLIENT_SECRET",
            "TAP_TWITTER_ADS_ACCESS_TOKEN",
            "TAP_TWITTER_ADS_REFRESH_TOKEN",
        }
        missing_envs = [v for v in required_env if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def expected_metadata(self):
        """The expected streams and metadata about the streams - mirrors
        tap_twitter_ads.streams.STREAMS exactly (key_properties,
        replication_method, replication_key, parent-tap-stream-id)."""
        no_replication_key = {self.OBEYS_START_DATE: False}
        incremental_created_at = {
            self.REPLICATION_KEYS: {"created_at"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.OBEYS_START_DATE: True,
        }

        def full_table(*primary_keys, parent_stream=None):
            metadata = {
                self.PRIMARY_KEYS: set(primary_keys),
                self.REPLICATION_METHOD: self.FULL_TABLE,
                **no_replication_key,
            }
            if parent_stream:
                metadata["parent_stream"] = parent_stream
            return metadata

        def incremental(*primary_keys, parent_stream=None):
            metadata = {self.PRIMARY_KEYS: set(primary_keys), **incremental_created_at}
            if parent_stream:
                metadata["parent_stream"] = parent_stream
            return metadata

        return {
            # ---- Authenticated-user streams (users_me + its children) ----
            "users_me": full_table("id"),
            "account": full_table("account_id"),
            "usage_tweets": full_table("project_id"),
            "usage_credits": full_table(),
            "personalized_trends": full_table("trend_name"),
            "user_reposts_of_me": full_table("id"),
            "bots": full_table("id"),
            "webhooks": full_table("id"),
            "user_tweets": incremental("id", parent_stream="users_me"),
            "user_mentions": incremental("id", parent_stream="users_me"),
            "user_home_timeline": incremental("id", parent_stream="users_me"),
            "user_liked_tweets": full_table("id", parent_stream="users_me"),
            "user_bookmarks": full_table("id", parent_stream="users_me"),
            "user_bookmark_folders": full_table("id", parent_stream="users_me"),
            "user_followers": full_table("id", parent_stream="users_me"),
            "user_following": full_table("id", parent_stream="users_me"),
            "user_blocking": full_table("id", parent_stream="users_me"),
            "user_muting": full_table("id", parent_stream="users_me"),
            "user_owned_lists": full_table("id", parent_stream="users_me"),
            "user_pinned_lists": full_table("id", parent_stream="users_me"),
            "user_list_memberships": full_table("id", parent_stream="users_me"),
            "user_followed_lists": full_table("id", parent_stream="users_me"),
            "user_affiliates": full_table("id", parent_stream="users_me"),
            "dm_events": full_table("id", parent_stream="users_me"),
            # ---- Config-driven batch lookups (config_ids) ----
            "users_by_ids": full_table("id"),
            "users_by_usernames": full_table("id"),
            "tweets_by_ids": full_table("id"),
            "spaces_by_ids": full_table("id"),
            "spaces_by_creator_ids": full_table("id"),
            "media_by_keys": full_table("media_key"),
            # ---- Config-driven loops (config_loop / config_loop_multi) ----
            "trends_by_woeid": full_table("trend_name"),
            "compliance_jobs": full_table("id"),
            "list_by_id": full_table("id"),
            "list_tweets": full_table("id", parent_stream="list_by_id"),
            "list_members": full_table("id", parent_stream="list_by_id"),
            "list_followers": full_table("id", parent_stream="list_by_id"),
            "space_by_id": full_table("id"),
            "space_tweets": full_table("id", parent_stream="space_by_id"),
            "space_buyers": full_table("id", parent_stream="space_by_id"),
            "broadcast_by_id": full_table("id"),
            "scheduled_broadcast_by_id": full_table("id"),
            "community_by_id": full_table("id"),
            "news_by_id": full_table("id"),
            # ---- Per-post engagement lookups (config_loop_multi over tweet_ids) ----
            "post_liking_users": full_table("id"),
            "post_quote_tweets": full_table("id"),
            "post_reposted_by": full_table("id"),
            "post_reposts": full_table("id"),
            # ---- Search / query-driven streams ----
            "users_search": full_table("id"),
            "post_search_recent": incremental("id"),
            "post_search_all": incremental("id"),
            "post_counts_recent": full_table("start"),
            "post_counts_all": full_table("start"),
            "communities_search": full_table("id"),
            "news_search": full_table("id"),
            "community_notes_search_written": full_table("id"),
            "community_notes_eligible_posts": full_table("id"),
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """ return a dictionary with the key of table name and value as a set of primary key fields """
        return {table: properties.get(self.PRIMARY_KEYS) or set()
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """return a dictionary with the key of table name and value as a set of replication key fields"""
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        """return a dictionary with the key of table name and value as a set of automatic key fields"""

        return {table: ((self.expected_primary_keys().get(table) or set()) |
                        (self.expected_replication_keys().get(table) or set()))
                for table in self.expected_metadata()}

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_parent_streams(self):
        """return a dictionary with the key of child stream name and value as the parent stream name"""
        return {stream: metadata.get('parent_stream')
            for stream, metadata in self.expected_metadata().items()
            if metadata.get('parent_stream')}

#########################
#   Helper Methods      #
#########################

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(self,
                                                              conn_id,
                                                              self.expected_streams(),
                                                              self.expected_primary_keys())

        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields


    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def calculated_states_by_stream(self, current_state):
        """Roll each stream's current bookmark back by a small timedelta so a
        second sync has new data to pick up. Unlike the old OAuth1 Ads tap
        (every stream bookmarked under one fixed `account_id` key), OAuth2
        streams key their bookmark by whatever id/value they were synced
        under (a parent record id for users_me-children, or the resolved
        query text for search streams) - so each stream's single bookmark
        key is discovered from state itself rather than assumed."""
        timedelta_by_stream = {stream: [0, 0, 1]  # {stream_name: [days, hours, minutes], ...}
                               for stream in self.expected_streams()}

        stream_to_calculated_state = {}
        for stream, bookmarks_by_key in current_state.get('bookmarks', {}).items():
            stream_to_calculated_state[stream] = {}
            for bookmark_key, bookmark_value in bookmarks_by_key.items():
                if not bookmark_value:
                    continue
                state_as_datetime = dateutil.parser.parse(bookmark_value)

                days, hours, minutes = timedelta_by_stream.get(stream, [0, 0, 1])
                calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

                calculated_state_formatted = dt.strftime(calculated_state_as_datetime, self.BOOKMARK_FORMAT)

                stream_to_calculated_state[stream][bookmark_key] = calculated_state_formatted

        return stream_to_calculated_state

    def convert_state_to_utc(self, date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return dt.strftime(date_object_utc, self.BOOKMARK_FORMAT)

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
                return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))
