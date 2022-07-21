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
    start_date = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    BOOKMARK_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"
    OBEYS_START_DATE = "obey-start-date"
    PAGE_SIZE = 1000
    account_id = ""

    def tap_name(self):
        return "tap-twitter-ads"

    def get_type(self):
        return "platform.twitter-ads"

    def get_credentials(self):
        return {
            'consumer_key': os.getenv('TAP_TWITTER_ADS_CONSUMER_KEY'),
            'consumer_secret': os.getenv('TAP_TWITTER_ADS_CONSUMER_SECRET'),
            'access_token': os.getenv('TAP_TWITTER_ADS_ACCESS_TOKEN'),
            'access_token_secret': os.getenv('TAP_TWITTER_ADS_ACCESS_TOKEN_SECRET')
        }

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""

        return_value = {
            "account_ids": os.getenv("TAP_TWITTER_ADS_ACCOUNT_IDS"),
            "attribution_window": os.getenv("TAP_TWITTER_ADS_ATTRIBUTION_WINDOW"),
            "with_deleted": os.getenv("TAP_TWITTER_ADS_WITH_DELETED"),
            "country_codes": os.getenv("TAP_TWITTER_ADS_COUNTRY_CODES"),
            "start_date": "2019-03-01T00:00:00Z",
            "page_size": self.PAGE_SIZE,
            "reports": [
                {
                    "name": "accounts_daily_report",
                    "entity": "ACCOUNT",
                    "segment": "NO_SEGMENT",
                    "granularity": "HOUR"
                },
                {
                    "name": "campaigns_daily_report",
                    "entity": "CAMPAIGN",
                    "segment": "NO_SEGMENT",
                    "granularity": "HOUR"
                }
            ]
        }
        self.account_id = os.getenv("TAP_TWITTER_ADS_ACCOUNT_IDS").split(" ")[0]
        if original:
            return return_value

        # Reassign start date
        return_value["start_date"] = self.start_date
        return return_value

    def setUp(self):
        required_env = {
            "TAP_TWITTER_ADS_CONSUMER_KEY",
            "TAP_TWITTER_ADS_CONSUMER_SECRET",
            "TAP_TWITTER_ADS_ACCESS_TOKEN",
            "TAP_TWITTER_ADS_ACCESS_TOKEN_SECRET",
            "TAP_TWITTER_ADS_ACCOUNT_IDS",
            "TAP_TWITTER_ADS_ATTRIBUTION_WINDOW",
            "TAP_TWITTER_ADS_WITH_DELETED",
            "TAP_TWITTER_ADS_COUNTRY_CODES"
        }
        missing_envs = [v for v in required_env if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        default_metadata = {
            self.REPLICATION_KEYS: {"updated_at"},
            self.PRIMARY_KEYS: {"id"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.OBEYS_START_DATE: True
        }

        targeting_endpoint_metadata = {
            self.PRIMARY_KEYS: {"targeting_value"},
            self.REPLICATION_METHOD: self.FULL_TABLE,
            self.OBEYS_START_DATE: False
        }
        report_metadata = {
            self.REPLICATION_KEYS: {"end_time"},
            self.PRIMARY_KEYS: {"__sdc_dimensions_hash_key"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.OBEYS_START_DATE: True
        }
        return {
            'accounts': default_metadata,
            'account_media': default_metadata,
            'tracking_tags': default_metadata,
            "advertiser_business_categories": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False
            },
            "campaigns": default_metadata,
            "cards": default_metadata,
            "cards_poll": default_metadata,
            "cards_image_conversation": default_metadata,
            "cards_video_conversation": default_metadata,
            "content_categories": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False
            },
            "funding_instruments": default_metadata,
            "iab_categories": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False
            },
            "line_items": default_metadata,
            "targeting_criteria": {
                # `targeting_criteria` is child stream of line_items stream which is incremental.
                # We are writing a separate bookmark for the child stream in which we are storing
                # the bookmark based on the parent's replication key.
                # But, we are not using any fields from the child record for it.
                # That's why the `targeting_criteria` stream does not have replication_key but still it is incremental.
                self.PRIMARY_KEYS: {"line_item_id", "id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.OBEYS_START_DATE: True
            },
            "media_creatives": default_metadata,
            "preroll_call_to_actions": default_metadata,
            "promoted_accounts": default_metadata,
            "promoted_tweets": default_metadata,
            "promotable_users": default_metadata,
            "scheduled_promoted_tweets": default_metadata,
            "tailored_audiences": default_metadata,
            "targeting_app_store_categories": targeting_endpoint_metadata,
            "targeting_conversations": targeting_endpoint_metadata,
            "targeting_devices": targeting_endpoint_metadata,
            "targeting_events": {
                self.PRIMARY_KEYS: {"targeting_value"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: True
            },
            "targeting_interests": targeting_endpoint_metadata,
            "targeting_languages": targeting_endpoint_metadata,
            "targeting_locations": targeting_endpoint_metadata,
            "targeting_network_operators": targeting_endpoint_metadata,
            "targeting_platform_versions": targeting_endpoint_metadata,
            "targeting_platforms": targeting_endpoint_metadata,
            "targeting_tv_markets":  {
                self.PRIMARY_KEYS: {"locale"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False
            },
            "targeting_tv_shows": targeting_endpoint_metadata,
            "tweets": {
                self.REPLICATION_KEYS: {"created_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.OBEYS_START_DATE: True
            },
            "accounts_daily_report": report_metadata,
            "campaigns_daily_report": report_metadata,
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
        timedelta_by_stream = {stream: [0,0,1]  # {stream_name: [days, hours, minutes], ...}
                               for stream in self.expected_streams()}

        stream_to_calculated_state = {stream: {self.account_id: ""} for stream in current_state['bookmarks'].keys()}
        for stream, state in current_state['bookmarks'].items():
            state_as_datetime = dateutil.parser.parse(state[self.account_id])

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

            calculated_state_formatted = dt.strftime(calculated_state_as_datetime, self.BOOKMARK_FORMAT)

            stream_to_calculated_state[stream][self.account_id] = calculated_state_formatted

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