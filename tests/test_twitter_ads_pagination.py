import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import TwitterAds
from tap_twitter_ads.streams import STREAMS


class PaginationTest(TwitterAds):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    def name(self):
        return "tap_tester_twitter_ads_pagination_test"

    def test_run(self):
        """
        • Verify that for each paginated stream you can get multiple pages of data.
          This requires we ensure more than 1 page of data exists at all times for
          any given stream.
        • Verify by pks that the data replicated matches the data we expect.
        """
        # Only streams with `stream.paginated = True` (see streams.py) can
        # ever return more than one page - singleton/config_ids/config_loop
        # streams return at most one page by design, so testing pagination
        # on them would be meaningless. Derived directly from STREAMS instead
        # of a hardcoded stream-name list so this stays correct automatically
        # as streams.py evolves.
        expected_streams = {name for name in self.expected_streams() if STREAMS[name].paginated}

        # Small page_size so a modest amount of test-account data still spans
        # multiple pages (X API v2 caps `max_results` at 100 for most endpoints).
        self.run_test(expected_streams=expected_streams, page_size=2)

    def run_test(self, expected_streams, page_size):

        streams_to_test = expected_streams

        self.PAGE_SIZE = page_size

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]

                # verify records are more than page size so multiple pages are exercised
                sync_records = synced_records.get(stream)
                record_count_sync = sync_records.get('record_count')
                self.assertGreater(
                    record_count_sync, self.PAGE_SIZE,
                    msg="The number of records is not over the stream max limit, "
                        "so pagination was not fully tested for stream {}".format(stream))

                # verify all records in the target are unique by primary key
                records_pks_set = {
                    tuple(message.get('data').get(primary_key) for primary_key in expected_primary_keys)
                    for message in sync_records.get('messages')
                }
                records_pks_list = [
                    tuple(message.get('data').get(primary_key) for primary_key in expected_primary_keys)
                    for message in sync_records.get('messages')
                ]
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg="We have duplicate records for {}".format(stream))
