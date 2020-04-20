from decimal import Decimal
import time
import json
import zlib
import backoff
import requests
from requests.exceptions import ConnectionError
from requests_oauthlib import OAuth1

from singer import metrics
import singer

LOGGER = singer.get_logger()

ADS_API_VERSION = '6'
ADS_API_URL = 'https://ads-api.twitter.com'
DEFAULT_CONNECTION_TIMEOUT = 5
DEFAULT_REST_TIMEOUT = 5



class Server5xxError(Exception):
    pass


class Server42xRateLimitError(Exception):
    pass


class TwitterError(Exception):
    pass


class TwitterBadRequestError(TwitterError):
    pass


class TwitterUnauthorizedError(TwitterError):
    pass


class TwitterNotFoundError(TwitterError):
    pass


class TwitterNotAcceptableError(TwitterError):
    pass


class TwitterResourceGoneError(TwitterError):
    pass


class TwitterEnhanceYourCalmError(TwitterError):
    pass


class TwitterUnprocessableEntityError(TwitterError):
    pass


class TwitterConflictError(TwitterError):
    pass


class TwitterForbiddenError(TwitterError):
    pass


class TwitterInternalServiceError(TwitterError):
    pass


# Reference: https://developer.twitter.com/en/docs/basics/response-codes
ERROR_CODE_EXCEPTION_MAPPING = {
    400: TwitterBadRequestError,
    401: TwitterUnauthorizedError,
    403: TwitterForbiddenError,
    404: TwitterNotFoundError,
    406: TwitterNotAcceptableError,
    410: TwitterResourceGoneError,
    420: TwitterEnhanceYourCalmError,
    422: TwitterUnprocessableEntityError,
    500: TwitterInternalServiceError}


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, TwitterError)


# Example error message:
# {"errors":[{"message":"Sorry, that page does not exist","code":34}]}
# { "errors": [ { "code": 88, "message": "Rate limit exceeded" } ] }
def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as err:
        LOGGER.error('{}'.format(err))
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Twitter has neither sent
                # us a 2xx response nor a response content.
                return
            status_code = response.status_code
            response_json = response.json()
            if 'errors' in response_json:
                ex = get_exception_for_error_code(status_code)
                i = 0
                error_combined = 'Error status: {}'.format(status_code)
                for error in response_json['errors']:
                    LOGGER.error('{}'.format(error))
                    message = response_json['errors'][i].get('message')
                    error_code = response_json['errors'][i].get('code')
                    error_message = '{}: {}'.format(error_code, message)
                    error_combined = '{}; {}'.format(error_combined, error_message)
                    i = i + 1
                # Provide warning-only for 'INVALID_ACCOUNT_SERVICE_LEVEL' unavailable enpoints
                if status_code == 400 and 'INVALID_ACCOUNT_SERVICE_LEVEL' in error_combined:
                    LOGGER.warning('{}'.format(error_combined))
                    return error_combined
                else:
                    raise ex(error_combined)
            else:
                raise TwitterError(response_json)
        except (ValueError, TypeError) as err2:
            raise TwitterError(err2)


class TwitterClient(object):
    def __init__(self,
                 consumer_key,
                 consumer_secret,
                 access_token,
                 access_token_secret,
                 user_agent=None):
        self.__consumer_key = consumer_key
        self.__consumer_secret = consumer_secret
        self.__access_token = access_token
        self.__access_token_secret = access_token_secret
        self.__user_agent = user_agent
        self.__verified = False
        self.__session = requests.Session()
        self.base_url = '{}/{}'.format(ADS_API_URL, ADS_API_VERSION)

        if not all([self.__consumer_key,
                    self.__consumer_secret,
                    self.__access_token,
                    self.__access_token_secret]):
            raise Exception('Missing authentication parameter')

        self.__auth_header = OAuth1(
            client_key=self.__consumer_key,
            client_secret=self.__consumer_secret,
            resource_owner_key=self.__access_token,
            resource_owner_secret=self.__access_token_secret,
            signature_type='auth_header')

    def __enter__(self):
        self.__verified = self.check_access()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()


    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def check_access(self):
        headers = {}
        # Endpoint: simple API call to return a single record (org settings) to test access
        url = '{}/accounts&count=1'.format(self.base_url)
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/json'

        response = self.__session.get(
            url=url,
            headers=headers,
            auth=self.__auth_header)
        if response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            raise_for_error(response)
        else:
            LOGGER.info('Access Granted')
            return True


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server42xRateLimitError),
                          max_tries=5,
                          factor=2)
    def request(self, method, url=None, path=None, data=None, params=None, **kwargs):

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Accept'] = 'application/json'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(
                method,
                url,
                auth=self.__auth_header,
                data=data,
                params=params,
                timeout=(DEFAULT_CONNECTION_TIMEOUT, DEFAULT_REST_TIMEOUT),
                **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        # Rate Limit reference: https://developer.twitter.com/en/docs/basics/rate-limiting
        # LOGGER.info('headers = {}'.format(response.headers))
        rate_limit = int(response.headers.get('x-rate-limit-limit'))
        rate_limit_remaining = int(response.headers.get('x-rate-limit-remaining'))
        rate_limit_reset = int(response.headers.get('x-rate-limit-reset'))
        rate_limit_percent_remaining = 100 * (Decimal(rate_limit_remaining) / Decimal(rate_limit))

        # Wait for reset if remaining calls are less than 5%
        if rate_limit_percent_remaining < 5:
            LOGGER.warning('Rate Limit Warning: {}; remaining calls: {}; remaining %: {}% '.format(
                rate_limit, rate_limit_remaining, int(rate_limit_percent_remaining)))
            wait_time = rate_limit_reset - int(time.time())
            LOGGER.warning('Waiting for {} seconds.'.format(wait_time))
            time.sleep(int(wait_time))

        if response.status_code in (420, 429):
            raise Server42xRateLimitError()

        elif response.status_code >= 500:
            raise Server5xxError()

        elif response.status_code == 400:
            error_combined = raise_for_error(response)
            if 'INVALID_ACCOUNT_SERVICE_LEVEL' in error_combined:
                return None

        elif response.status_code != 200:
            error_combined = raise_for_error(response)

        return response.json()


    def get(self, url=None, path=None, params=None, **kwargs):
        return self.request('GET', url=url, path=path, params=params, **kwargs)


    def post(self, url=None, path=None, data=None, params=None, **kwargs):
        return self.request('POST', url=url, path=path, data=data, params=params, **kwargs)


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server42xRateLimitError),
                          max_tries=7,
                          factor=3)
    def get_gzip_json(self, url, endpoint):
        resp = None
        with metrics.http_request_timer(endpoint) as timer:
            resp = self.__session.request(method='GET',
                                          url=url,
                                          timeout=60)
            timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return self.unzip(resp.content)

    @classmethod
    def unzip(cls, blob):
        extracted = zlib.decompress(blob, 16+zlib.MAX_WBITS)
        decoded = extracted.decode('utf-8')
        return json.loads(decoded)
