import unittest
from unittest import mock
from unittest.mock import Mock, patch
from tap_twitter_ads.streams import LineItems
from tap_twitter_ads.sync import sync

START_DATE = "2022-01-28T00:00:00Z"
STREAM_NAME = "line_items"
ACCOUNT_ID = 'dummy_account_id'


@patch('singer.metadata.to_map')
@patch('singer.Transformer.transform')
@patch('singer.write_schema')
@patch('singer.messages.write_record')
@patch('twitter_ads.http.Request.perform')
class TestParentChildBookmarkingWithNoData(unittest.TestCase):
    """
    Test bookmarking for child stream. Verify that the bookmark mechanism of the child stream is independent of the parent stream.
    """
    def test_child_bookmark_update_with_no_data(self, mock_request, mock_write_record, mock_write_schema,
                                                          mock_transform, mock_metadata):
        """
        Verify that tap does not write a bookmark for the stream if there is no data available in historic sync.
        """
        mock_client = Mock() # Mock the twitter_ads sdk client object
        mock_catalog = Mock() # Mock catalog

        test_stream = LineItems()
        mock_line_items = []
        mock_response = Mock()
        mock_response.body = {'data': mock_line_items}
        mock_response.headers = []

        mock_request.return_value = mock_response # Mock twitter_ads.http.Request.perform with proper response
        state = {"bookmarks": {}}

        # Call sync_endpoint to verify bookmark for child stream
        test_stream.sync_endpoint(mock_client, mock_catalog, state, START_DATE, STREAM_NAME, 
                    LineItems, {}, ACCOUNT_ID, child_streams=['targeting_criteria'], selected_streams=['line_items', 'targeting_criteria'])

        # Verify that No bookmark is written for child stream
        self.assertIsNone(state.get("bookmarks").get(STREAM_NAME))

        # Verify that Request.perform called only once for parent.
        self.assertEqual(mock_request.call_count, 1)

    @mock.patch("tap_twitter_ads.streams.TwitterAds.write_bookmark")
    def test_child_bookmark_update(self, mock_write_bookmark, mock_request, mock_write_record, mock_write_schema, mock_transform, mock_metadata):
        """
        Verify that tap write bookmark for child stream independent of parent stream.
        """
        mock_client = Mock() # Mock the twitter_ads sdk client object
        mock_catalog = Mock() # Mock catalog
        test_stream = LineItems()

        mock_line_items = [
            {'id': 1484405085639962727,'updated_at': '2022-03-09T04:59:57Z', 'deleted': False},
            {'id': 1484405085639962627,'updated_at': '2022-03-07T04:59:57Z', 'deleted': False}]
        mock_response = Mock()
        mock_response.body = {'data': mock_line_items}
        mock_response.headers = []

        mock_request.return_value = mock_response # Mock twitter_ads.http.Request.perform with proper response
        state = {"bookmarks": {"line_items": "2022-03-08T04:59:57+0000"}}

        # Call sync_endpoint to verify bookmark for child stream
        test_stream.sync_endpoint(mock_client, mock_catalog, state, START_DATE, 'targeting_criteria', 
                    LineItems, {}, ACCOUNT_ID, child_streams=['targeting_criteria'], selected_streams=['line_items', 'targeting_criteria'])

        # assert that write_bookmark is called with current state and max_bookmark_value
        mock_write_bookmark.assert_called_with(state, "targeting_criteria", "2022-03-09T04:59:57+0000")
    
        # Verify that Request.perform called 2 times, 1 time for parent and 1 times for child call.
        self.assertEqual(mock_request.call_count, 2)

    def test_child_bookmark_update_with_state(self, mock_request, mock_write_record, mock_write_schema, mock_transform, mock_metadata):
        """
        Verify that tap does not update bookmark for child stream if no new record found for parent stream
        """
        mock_client = Mock() # Mock the twitter_ads sdk client object
        mock_catalog = Mock() # Mock catalog
        test_stream = LineItems()
        test_stream.get_selected_fields = mock.Mock()
        test_stream.get_selected_fields.return_value = ['targeting_criteria']

        mock_line_items = [
            {'id': 1484405085639962727,'updated_at': '2022-03-09T04:59:57Z', 'deleted': False},
            {'id': 1484405085639962627,'updated_at': '2022-03-07T04:59:57Z', 'deleted': False}]
        mock_response = Mock()
        mock_response.body = {'data': mock_line_items}
        mock_response.headers = []

        mock_request.return_value = mock_response # Mock twitter_ads.http.Request.perform with proper response
        state = {"bookmarks": {"line_items": "2022-03-09T04:59:57+0000", "targeting_criteria": "2022-03-09T04:59:57+0000"}}

        # Call sync_endpoint to verify bookmark for child stream
        test_stream.sync_endpoint(mock_client, mock_catalog, state, START_DATE, STREAM_NAME, 
                    LineItems, {}, ACCOUNT_ID, child_streams=['targeting_criteria'], 
                    selected_streams=['line_items', 'targeting_criteria'])

        # Get bookmark of child stream after sync
        bookmark = test_stream.get_bookmark(state, 'targeting_criteria', START_DATE)

        self.assertEqual(bookmark, '2022-03-09T04:59:57+0000')

        # Verify that Request.perform called 2 times, 1 time for parent and 1 time for child call.
        self.assertEqual(mock_request.call_count, 2)
