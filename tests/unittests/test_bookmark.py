import unittest
from tap_twitter_ads.streams import TwitterAds

class TestBookmark(unittest.TestCase):
    """
    Test the get_bookmark method for different values
    """
    stream = "dummy_stream"
    default = "2018-01-28T00:00:00Z"
    account_id = "account_id"
    twitter_ads_object = TwitterAds()
    
    def test_empty_state(self):
        """ Test that if an empty state is passed in the get_bookmark then it returns the default value"""
        state = {}
        
        bookmark = self.twitter_ads_object.get_bookmark(state, self.stream, self.default, self.account_id)
        
        self.assertEqual(bookmark, self.default)

    def test_empty_bookmark_for_specific_stream(self):
        """ Test that if the bookmark for particular stream is not found in the state then it returns the default value """

        state = {'bookmark': {'stream_1': '2017-01-28T00:00:00Z'}}
        
        bookmark = self.twitter_ads_object.get_bookmark(state, self.stream, self.default, self.account_id)
        
        self.assertEqual(bookmark, self.default)

    def test_valid_bookmark(self):
        """ Test that if the valid bookmark is available in the state then it returns the bookmark value """

        state = {'bookmarks': {self.stream: {self.account_id: "2017-01-28T00:00:00Z"}}}
        
        bookmark = self.twitter_ads_object.get_bookmark(state, self.stream, self.default, self.account_id)
        
        self.assertEqual(bookmark, "2017-01-28T00:00:00Z")

    def test_null_bookmark(self):
        """ Test that if the bookmark value is None in the state then it returns None"""

        state = {'bookmarks': {self.stream: {self.account_id: None}}}
        
        bookmark = self.twitter_ads_object.get_bookmark(state, self.stream, self.default, self.account_id)
        
        self.assertEqual(bookmark, None)