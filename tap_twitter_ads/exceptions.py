class TwitterAdsClientError(Exception):
    pass

class TwitterAdsBackoffError(Exception):
    """Base class for errors that should trigger a backoff/retry."""
    pass

class TwitterAdsBadRequestError(TwitterAdsClientError):
    pass

class TwitterAdsUnauthorizedError(TwitterAdsClientError):
    pass

class TwitterAdsForbiddenError(TwitterAdsClientError):
    pass

class TwitterAdsNotFoundError(TwitterAdsClientError):
    pass

class TwitterAdsMethodNotFoundError(TwitterAdsClientError):
    pass

class TwitterAdsUnprocessableEntityError(TwitterAdsClientError):
    pass

class TwitterAdsClient429Error(TwitterAdsBackoffError):
    pass

class TwitterAdsRequestCancelledError(TwitterAdsClientError):
    pass

class TwitterAdsInternalServerError(TwitterAdsBackoffError):
    pass

class TwitterAdsBadGatewayError(TwitterAdsBackoffError):
    pass

class TwitterAdsServiceUnavailableError(TwitterAdsBackoffError):
    pass
