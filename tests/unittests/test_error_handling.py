import unittest
import tap_twitter_ads
import tap_twitter_ads.client as client
from unittest import mock
from twitter_ads.client import Client
# from tap_twitter_ads.sync import get_resource, post_resource
from tap_twitter_ads.streams import TwitterAds



class Mockresponse(Exception):
    def __init__(self, status_code, details=[]):
        self.code = status_code
        self.details = details

@mock.patch("tap_twitter_ads.streams.Request")
class TestExceptionHandling(unittest.TestCase):
    """
        Test cases to verify error is raised with proper message  for get_resource method.
    """
    
    client_obj = Client(
        consumer_key='test',
        consumer_secret='test',
        access_token='test',
        access_token_secret='test'
    )
    
    test_stream = TwitterAds()

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(400))
    def test_400_error_custom_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 400 error message from response
        """
        
        with self.assertRaises(client.TwitterAdsBadRequestError) as e:
            self.test_stream.get_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), "HTTP-error-code: 400, Message: The request is missing or has a bad parameter.")
    
    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(400, [{"message":"This mesaage from response 400"}]))
    def test_400_error_response_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 400 error message from response
        """

        with self.assertRaises(client.TwitterAdsBadRequestError) as e:
            self.test_stream.get_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), 'HTTP-error-code: 400, Message: This mesaage from response 400')
        
    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(401))   
    def test_401_error_custom_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 401 error message from response
        """

        with self.assertRaises(client.TwitterAdsUnauthorizedError) as e:
            self.test_stream.get_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), "HTTP-error-code: 401, Message: Unauthorized access for the URL.")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(401, [{"message":"This mesaage from response 401"}]))   
    def test_401_error_response_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 401 error message from response
        """

        with self.assertRaises(client.TwitterAdsUnauthorizedError) as e:
            self.test_stream.get_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), "HTTP-error-code: 401, Message: This mesaage from response 401")
 
    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(403))
    def test_403_error_custom_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 403 error message from response
        """

        with self.assertRaises(client.TwitterAdsForbiddenError) as e:
            self.test_stream.get_resource("test", self.client_obj, "path")
            
        self.assertEqual(str(e.exception), "HTTP-error-code: 403, Message: User does not have permission to access the resource.")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(403, [{"message":"This mesaage from response 403"}]))
    def test_403_error_response_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 403 error message from response
        """

        with self.assertRaises(client.TwitterAdsForbiddenError) as e:
            self.test_stream.get_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), "HTTP-error-code: 403, Message: This mesaage from response 403")
    
    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(404))
    def test_404_error_custom_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 404 error message from response
        """

        with self.assertRaises(client.TwitterAdsNotFoundError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")
            
        self.assertEqual(str(e.exception), "HTTP-error-code: 404, Message: The resource you have specified cannot be found.")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(404, [{"message":"This mesaage from response 404"}]))
    def test_404_error_response_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 404 error message from response
        """

        with self.assertRaises(client.TwitterAdsNotFoundError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")
        
        self.assertEqual(str(e.exception), "HTTP-error-code: 404, Message: This mesaage from response 404")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(405))
    def test_405_error_custom_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 405 error message from response
        """
        
        with self.assertRaises(client.TwitterAdsMethodNotFoundError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 405, Message: The provided HTTP method is not supported by the URL.")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(405, [{"message":"This mesaage from response 405"}]))
    def test_405_error_response_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 405 error message from response
        """

        with self.assertRaises(client.TwitterAdsMethodNotFoundError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 405, Message: This mesaage from response 405")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(408))
    def test_408_error_custom_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 408 error message from response
        """

        with self.assertRaises(client.TwitterAdsRequestCancelledError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 408, Message: Request is cancelled.")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(408, [{"message":"This mesaage from response 408"}]))
    def test_408_error_response_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 408 error message from response
        """

        with self.assertRaises(client.TwitterAdsRequestCancelledError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 408, Message: This mesaage from response 408")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(429))
    def test_429_error_custom_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 429 error message from response
        """

        with self.assertRaises(client.TwitterAdsClient429Error) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 429, Message: API rate limit exceeded, please retry after some time.")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(429, [{"message":"This mesaage from response 429"}]))
    def test_429_error_response_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 429 error message from response
        """
        
        with self.assertRaises(client.TwitterAdsClient429Error) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 429, Message: This mesaage from response 429")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(500))
    def test_500_error_custom_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 500 error message from response
        """
        
        with self.assertRaises(client.TwitterAdsInternalServerError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 500, Message: Internal error.")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(500, [{"message":"This mesaage from response 500"}]))
    def test_500_error_response_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 500 error message from response
        """
        
        with self.assertRaises(client.TwitterAdsInternalServerError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 500, Message: This mesaage from response 500")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(503))
    def test_503_error_custom_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 503 error message from response
        """

        with self.assertRaises(client.TwitterAdsServiceUnavailableError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 503, Message: Service is unavailable.")

    @mock.patch("tap_twitter_ads.streams.Cursor", side_effect=Mockresponse(503, [{"message":"This mesaage from response 503"}]))
    def test_503_error_response_message(self, mocked_cursor, mocked_request):
        """
            Test case to verify 503 error message from response
        """

        with self.assertRaises(client.TwitterAdsServiceUnavailableError) as e:
            self.test_stream.get_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 503, Message: This mesaage from response 503")
        
class TestExceptionHandlingForPost(unittest.TestCase):
    """
        Test cases to verify error is raised with proper message  for post_resource method.
    """
    
    client_obj = Client(
        consumer_key='test',
        consumer_secret='test',
        access_token='test',
        access_token_secret='test'
    )
    
    test_stream = TwitterAds()

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(400))
    def test_400_error_post_custom_message(self, mocked_request):
        """
            Test case to verify 400 error message from response
        """
        
        with self.assertRaises(client.TwitterAdsBadRequestError) as e:
            self.test_stream.post_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), "HTTP-error-code: 400, Message: The request is missing or has a bad parameter.")
    
    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(400, [{"message":"This mesaage from response 400"}]))
    def test_400_error_post_error_message(self, mocked_request):
        """
            Test case to verify 400 error message from response
        """
        
        with self.assertRaises(client.TwitterAdsBadRequestError) as e:
            self.test_stream.post_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), 'HTTP-error-code: 400, Message: This mesaage from response 400')
        
    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(401))   
    def test_401_error_custom_message(self, mocked_request):
        """
            Test case to verify 401 error message from response
        """

        with self.assertRaises(client.TwitterAdsUnauthorizedError) as e:
            self.test_stream.post_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), "HTTP-error-code: 401, Message: Unauthorized access for the URL.")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(401, [{"message":"This mesaage from response 401"}]))   
    def test_401_error_response_message(self, mocked_request):
        """
            Test case to verify 401 error message from response
        """

        with self.assertRaises(client.TwitterAdsUnauthorizedError) as e:
            self.test_stream.post_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), "HTTP-error-code: 401, Message: This mesaage from response 401")
 
    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(403))
    def test_403_error_custom_message(self, mocked_request):
        """
            Test case to verify 403 error message from response
        """

        with self.assertRaises(client.TwitterAdsForbiddenError) as e:
            self.test_stream.post_resource("test", self.client_obj, "path")
            
        self.assertEqual(str(e.exception), "HTTP-error-code: 403, Message: User does not have permission to access the resource.")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(403, [{"message":"This mesaage from response 403"}]))
    def test_403_error_response_message(self, mocked_request):
        """
            Test case to verify 403 error message from response
        """

        with self.assertRaises(client.TwitterAdsForbiddenError) as e:
            self.test_stream.post_resource("test", self.client_obj, "path")

        self.assertEqual(str(e.exception), "HTTP-error-code: 403, Message: This mesaage from response 403")
        
    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(404))
    def test_404_error_custom_message(self, mocked_request):
        """
            Test case to verify 404 error message from response
        """

        with self.assertRaises(client.TwitterAdsNotFoundError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")
            
        self.assertEqual(str(e.exception), "HTTP-error-code: 404, Message: The resource you have specified cannot be found.")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(404, [{"message":"This mesaage from response 404"}]))
    def test_404_error_response_message(self, mocked_request):
        """
            Test case to verify 404 error message from response
        """

        with self.assertRaises(client.TwitterAdsNotFoundError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")
        
        self.assertEqual(str(e.exception), "HTTP-error-code: 404, Message: This mesaage from response 404")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(405))
    def test_405_error_custom_message(self, mocked_request):
        """
            Test case to verify 405 error message from response
        """

        with self.assertRaises(client.TwitterAdsMethodNotFoundError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 405, Message: The provided HTTP method is not supported by the URL.")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(405, [{"message":"This mesaage from response 405"}]))
    def test_405_error_response_message(self, mocked_request):
        """
            Test case to verify 405 error message from response
        """

        with self.assertRaises(client.TwitterAdsMethodNotFoundError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 405, Message: This mesaage from response 405")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(408))
    def test_408_error_custom_message(self, mocked_request):
        """
            Test case to verify 408 error message from response
        """

        with self.assertRaises(client.TwitterAdsRequestCancelledError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 408, Message: Request is cancelled.")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(408, [{"message":"This mesaage from response 408"}]))
    def test_408_error_response_message(self, mocked_request):
        """
            Test case to verify 408 error message from response
        """

        with self.assertRaises(client.TwitterAdsRequestCancelledError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 408, Message: This mesaage from response 408")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(429))
    def test_429_error_custom_message(self, mocked_request):
        """
            Test case to verify 429 error message from response
        """

        with self.assertRaises(client.TwitterAdsClient429Error) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 429, Message: API rate limit exceeded, please retry after some time.")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(429, [{"message":"This mesaage from response 429"}]))
    def test_429_error_response_message(self, mocked_request):
        """
            Test case to verify 429 error message from response
        """

        with self.assertRaises(client.TwitterAdsClient429Error) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 429, Message: This mesaage from response 429")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(500))
    def test_500_error_custom_message(self, mocked_request):
        """
            Test case to verify 500 error message from response
        """

        with self.assertRaises(client.TwitterAdsInternalServerError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 500, Message: Internal error.")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(500, [{"message":"This mesaage from response 500"}]))
    def test_500_error_response_message(self, mocked_request):
        """
            Test case to verify 500 error message from response
        """

        with self.assertRaises(client.TwitterAdsInternalServerError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 500, Message: This mesaage from response 500")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(503))
    def test_503_error_custom_message(self, mocked_request):
        """
            Test case to verify 503 error message from response
        """

        with self.assertRaises(client.TwitterAdsServiceUnavailableError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 503, Message: Service is unavailable.")

    @mock.patch("tap_twitter_ads.streams.Request", side_effect=Mockresponse(503, [{"message":"This mesaage from response 503"}]))
    def test_503_error_response_message(self, mocked_request):
        """
            Test case to verify 503 error message from response
        """

        with self.assertRaises(client.TwitterAdsServiceUnavailableError) as e:
            self.test_stream.post_resource("test", self.client_obj, "obj")

        self.assertEqual(str(e.exception), "HTTP-error-code: 503, Message: This mesaage from response 503")
