import unittest
from unittest.mock import Mock, patch
from tap_twitter_ads.sync import sync_endpoint, get_bookmark

START_DATE = "2022-01-28T00:00:00Z"
STREAM_NAME = "line_items"
ENDPOINT_CONFIG = {
            'path': '{account_id}/line_items',
            'data_key': 'data',
            'key_properties': ['id'],
            'replication_method': 'INCREMENTAL',
            'replication_keys': ['updated_at'],
            'params': {
                'sort_by': ['updated_at-desc'],
                'with_deleted': '{with_deleted}',
                'count': 1000,
                'cursor': None
            },
            'children': {
                'targeting_criteria': {
                    'path': 'accounts/{account_id}/targeting_criteria',
                    'data_key': 'data',
                    'key_properties': ['line_item_id', 'id'],
                    'replication_method': 'FULL_TABLE',
                    'parent_ids_limit': 1,
                    'params': {
                        'line_item_ids': '{parent_ids}',
                        'with_deleted': '{with_deleted}',
                        'count': 1000,
                        'cursor': None
                    }
                }
            }
        }
ACCOUNT_ID = 'dummy_account_id'


@patch('singer.metadata.to_map')
@patch('singer.Transformer.transform')
@patch('singer.write_schema')
@patch('singer.messages.write_record')
@patch('twitter_ads.http.Request.perform')
class TestParentChildBookmarkingWithNoData(unittest.TestCase):
    """
    Test bookmarking for child stream. Verify that bookmark mechanism of child stream is independent of parent stream.
    """
    def test_child_bookmark_update_with_no_data(self, mock_request, mock_write_record, mock_write_schema,
                                                          mock_transform, mock_metadata):
        """
        Verify that tap does not write bookmark for stream if there is no data available in historic sync.
        """
        mock_client = Mock() # Mock the twitter_ads sdk client object
        mock_catalog = Mock() # Mock catalog

        mock_line_items = []
        mock_response = Mock()
        mock_response.body = {'data': mock_line_items}
        mock_response.headers = []

        mock_request.return_value = mock_response # Mock twitter_ads.http.Request.perform with proper response
        state = {"bookmarks": {}}

        # Call sync_endpoint to verify bookmark for child stream
        counter = sync_endpoint(mock_client, mock_catalog, state, START_DATE, STREAM_NAME, 
                           ENDPOINT_CONFIG, {}, ACCOUNT_ID, child_streams=['targeting_criteria'])

        # Get bookmark of child stream after sync
        bookmark = get_bookmark(state, 'targeting_criteria', START_DATE)

        # Verify that No bookark is written for child stream
        self.assertIsNone(bookmark)

        # Verify that Request.perform called only once for parent.
        self.assertEqual(mock_request.call_count, 1)

    def test_child_bookmark_update(self, mock_request, mock_write_record, mock_write_schema, mock_transform, mock_metadata):
        """
        Verify that tap write bookmark for child stream independent of parent stream.
        """
        mock_client = Mock() # Mock the twitter_ads sdk client object
        mock_catalog = Mock() # Mock catalog

        mock_line_items = [
            {'id': 1484405085639962727,'updated_at': '2022-03-09T04:59:57Z', 'deleted': False},
            {'id': 1484405085639962627,'updated_at': '2022-03-07T04:59:57Z', 'deleted': False}]
        mock_response = Mock()
        mock_response.body = {'data': mock_line_items}
        mock_response.headers = []

        mock_request.return_value = mock_response # Mock twitter_ads.http.Request.perform with proper response
        state = {"bookmarks": {"line_items": "2022-03-08T04:59:57+0000"}}

        # Call sync_endpoint to verify bookmark for child stream
        counter = sync_endpoint(mock_client, mock_catalog, state, START_DATE, STREAM_NAME, 
                           ENDPOINT_CONFIG, {}, ACCOUNT_ID, child_streams=['targeting_criteria'])

        # Get bookmark of child stream after sync
        bookmark = get_bookmark(state, 'targeting_criteria', START_DATE)

        # Verify that replication key value written as bookmark for child stream which is available in 1st record.
        # Because sdk return all records sorted by replication key.
        self.assertEqual(bookmark, '2022-03-09T04:59:57+0000')

        # Verify that Request.perform called 3 times, 1 time for parent and 2 times for child call.
        self.assertEqual(mock_request.call_count, 3)

    def test_child_bookmark_update_with_state(self, mock_request, mock_write_record, mock_write_schema, mock_transform, mock_metadata):
        """
        Verify that tap does not update bookmark for child stream if no new record found for stream
        """
        mock_client = Mock() # Mock the twitter_ads sdk client object
        mock_catalog = Mock() # Mock catalog


        mock_line_items = [
            {'id': 1484405085639962727,'updated_at': '2022-03-09T04:59:57Z', 'deleted': False},
            {'id': 1484405085639962627,'updated_at': '2022-03-07T04:59:57Z', 'deleted': False}]
        mock_response = Mock()
        mock_response.body = {'data': mock_line_items}
        mock_response.headers = []

        mock_request.return_value = mock_response # Mock twitter_ads.http.Request.perform with proper response
        state = {"bookmarks": {"line_items": "2022-03-09T04:59:57+0000", "targeting_criteria": "2022-03-09T04:59:57+0000"}}

        # Call sync_endpoint to verify bookmark for child stream
        counter = sync_endpoint(mock_client, mock_catalog, state, START_DATE, STREAM_NAME, 
                           ENDPOINT_CONFIG, {}, ACCOUNT_ID, child_streams=['targeting_criteria'])

        # Get bookmark of child stream after sync
        bookmark = get_bookmark(state, 'targeting_criteria', START_DATE)

        self.assertEqual(bookmark, '2022-03-09T04:59:57+0000')

        # Verify that Request.perform called 2 times, 1 time for parent and 1 time for child call.
        self.assertEqual(mock_request.call_count, 2)
