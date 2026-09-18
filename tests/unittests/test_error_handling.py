import unittest
from unittest import mock

import requests

from tap_twitter_ads.exceptions import (
    raise_for_error_v2,
    XApiBadRequestError,
    XApiAuthenticationError,
    XApiForbiddenError,
    XApiNotFoundError,
    XApiRateLimitError,
    XApiServerError,
    XApiClientError,
)


def mock_response(status_code, json_body=None, text=''):
    resp = mock.Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    if json_body is None:
        resp.json.side_effect = ValueError('no body')
    else:
        resp.json.return_value = json_body
    return resp


class TestExceptionHandling(unittest.TestCase):
    """
    Test cases to verify raise_for_error_v2 maps every X API v2 HTTP status
    code to the correct exception class with a properly formatted message,
    for both the "title"/"detail" error shape and the "errors": [...] shape.
    """

    def test_400_error_with_detail(self):
        response = mock_response(400, {'title': 'Bad Request', 'detail': 'The query parameter is invalid.'})
        with self.assertRaises(XApiBadRequestError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 400, Message: The query parameter is invalid.")

    def test_400_error_with_errors_list(self):
        response = mock_response(400, {'errors': [{'message': 'This message from response 400'}]})
        with self.assertRaises(XApiBadRequestError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 400, Message: This message from response 400")

    def test_401_error_custom_message(self):
        response = mock_response(401, {'title': 'Unauthorized'})
        with self.assertRaises(XApiAuthenticationError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 401, Message: Unauthorized")

    def test_403_error_custom_message(self):
        response = mock_response(403, {'title': 'Forbidden'})
        with self.assertRaises(XApiForbiddenError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 403, Message: Forbidden")

    def test_404_error_custom_message(self):
        response = mock_response(404, {'title': 'Not Found'})
        with self.assertRaises(XApiNotFoundError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 404, Message: Not Found")

    def test_429_error_custom_message(self):
        response = mock_response(429, {'title': 'Too Many Requests'})
        with self.assertRaises(XApiRateLimitError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 429, Message: Too Many Requests")

    def test_500_error_maps_to_server_error(self):
        response = mock_response(500, {'title': 'Internal Server Error'})
        with self.assertRaises(XApiServerError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 500, Message: Internal Server Error")

    def test_503_error_maps_to_server_error(self):
        """Any 5xx not explicitly mapped falls back to XApiServerError (retryable)."""
        response = mock_response(503, {'title': 'Service Unavailable'})
        with self.assertRaises(XApiServerError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 503, Message: Service Unavailable")

    def test_unmapped_4xx_falls_back_to_client_error(self):
        """A 4xx code with no explicit mapping (e.g. 422) falls back to the
        generic non-retryable XApiClientError."""
        response = mock_response(422, {'title': 'Unprocessable Entity'})
        with self.assertRaises(XApiClientError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 422, Message: Unprocessable Entity")

    def test_no_json_body_falls_back_to_response_text(self):
        response = mock_response(400, json_body=None, text='plain text error body')
        with self.assertRaises(XApiBadRequestError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 400, Message: plain text error body")

    def test_no_body_and_no_text_uses_unknown_error(self):
        response = mock_response(404, json_body=None, text='')
        with self.assertRaises(XApiNotFoundError) as e:
            raise_for_error_v2(response)
        self.assertEqual(str(e.exception), "HTTP-error-code: 404, Message: Unknown Error")


if __name__ == '__main__':
    unittest.main()
