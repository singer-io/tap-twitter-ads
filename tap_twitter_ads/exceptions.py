"""
X API v2 (OAuth 2.0, api.x.com/2) exceptions - used by client.py.
"""
from singer import get_logger

LOGGER = get_logger()


class XApiClientError(Exception):
    """Base class for all non-retriable X API v2 errors (4xx except 429)."""
    pass


class XApiBackoffError(Exception):
    """Base class for X API v2 errors that should trigger a backoff/retry."""
    pass


class XApiAuthenticationError(XApiClientError):
    """401 - invalid/expired access token or invalid_grant on token refresh."""
    pass


class XApiForbiddenError(XApiClientError):
    """403 - valid auth, but insufficient OAuth 2.0 scope/permission."""
    pass


class XApiNotFoundError(XApiClientError):
    """404 - the requested resource/id does not exist (or the app lacks access to it)."""
    pass


class XApiBadRequestError(XApiClientError):
    """400 - malformed request (bad/missing query params, invalid id format, etc)."""
    pass


class XApiRateLimitError(XApiBackoffError):
    """429 - per-endpoint rate limit exceeded."""
    pass


class XApiServerError(XApiBackoffError):
    """5xx - transient server-side error."""
    pass


X_API_ERROR_CODE_EXCEPTION_MAPPING = {
    400: XApiBadRequestError,
    401: XApiAuthenticationError,
    403: XApiForbiddenError,
    404: XApiNotFoundError,
    429: XApiRateLimitError,
}


def raise_for_error_v2(response):
    """Raise a mapped XApi*Error for a non-200 `requests.Response` from api.x.com.

    X API v2 error bodies look like either:
      {"title": "...", "detail": "...", "status": 401, "type": "..."}
      {"errors": [{"message": "...", ...}]}
    """
    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    detail = body.get('detail') or body.get('title')
    if not detail and body.get('errors'):
        detail = '; '.join(e.get('message', str(e)) for e in body['errors'])
    detail = detail or response.text[:300] or 'Unknown Error'

    message = 'HTTP-error-code: {}, Message: {}'.format(status_code, detail)

    exception_class = X_API_ERROR_CODE_EXCEPTION_MAPPING.get(
        status_code, XApiServerError if status_code >= 500 else XApiClientError)
    raise exception_class(message) from None
