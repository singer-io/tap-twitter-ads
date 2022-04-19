import unittest
from tap_twitter_ads.streams import TwitterAds

class TestBookmark(unittest.TestCase):
    """
    Test the get_bookmark method for different values
    """
    stream = "dummy_stream"
    default = "2018-01-28T00:00:00Z"
    account_id = "account_1"
    twitter_ads_object = TwitterAds()
    
    def test_empty_bookmark(self):
        """ Test that if an empty state passed in get_bookmark then it returns the default value"""
        state = {}
        
        bookmark = self.twitter_ads_object.get_bookmark(state, self.stream, self.default, self.account_id)
        
        self.assertEqual(bookmark, self.default)

    def test_invalid_bookmark(self):
        """ Test that if the `bookmarks` key does not found in the state then it returns the default value """

        state = {'bookmark_1': {}}
        
        bookmark = self.twitter_ads_object.get_bookmark(state, self.stream, self.default, self.account_id)
        
        self.assertEqual(bookmark, self.default)

    def test_account_1_valid_bookmark(self):
        """ Test that if the valid bookmark is available in the state then it returns the bookmark value """

        state = {'bookmarks': {self.stream: {self.account_id: "2017-01-28T00:00:00Z"}}}
        
        bookmark = self.twitter_ads_object.get_bookmark(state, self.stream, self.default, self.account_id)
        
        self.assertEqual(bookmark, "2017-01-28T00:00:00Z")

    def test_account_1_null_bookmark(self):
        """ Test that if the bookmark value is None in the state then it returns None"""

        state = {'bookmarks': {self.stream: {self.account_id: None}}}
        
        bookmark = self.twitter_ads_object.get_bookmark(state, self.stream, self.default, self.account_id)
        
        self.assertEqual(bookmark, None)