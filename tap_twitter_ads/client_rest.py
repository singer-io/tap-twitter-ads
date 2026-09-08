from decimal import Decimal
import time
import json
import zlib
import backoff
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, Timeout

from singer import metrics
import singer

LOGGER = singer.get_logger()

ADS_API_VERSION = '11'
ADS_API_URL = 'https://ads-api.twitter.com'
# X (Twitter) OAuth 2.0 token endpoint, used to exchange a refresh_token for a
# new access_token/refresh_token pair.
# Reference: https://developer.x.com/en/docs/authentication/oauth-2-0/user-access-token
OAUTH2_TOKEN_URL = 'https://api.x.com/2/oauth2/token'
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


class TwitterOauth2Error(TwitterError):
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
                # Provide warning-only for 'INVALID_ACCOUNT_SERVICE_LEVEL' unavailable endpoints
                if status_code == 400 and 'INVALID_ACCOUNT_SERVICE_LEVEL' in error_combined:
                    LOGGER.warning('{}'.format(error_combined))
                    return error_combined
                else:
                    raise ex(error_combined)
            else:
                raise TwitterError(response_json)
        except (ValueError, TypeError) as err2:
            raise TwitterError(err2)


def refresh_access_token(client_id, client_secret, refresh_token):
    """
    Exchange a refresh_token for a new (access_token, refresh_token) pair.
    X rotates refresh tokens on every use, so the caller must persist both
    returned values, not just the access_token.
    """
    LOGGER.info('Refreshing OAuth 2.0 access token')
    response = requests.post(
        OAUTH2_TOKEN_URL,
        data={
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': client_id
        },
        auth=HTTPBasicAuth(client_id, client_secret),
        timeout=(DEFAULT_CONNECTION_TIMEOUT, DEFAULT_REST_TIMEOUT))

    if response.status_code != 200:
        raise TwitterOauth2Error(
            'Failed to refresh OAuth 2.0 access token: {} {}'.format(
                response.status_code, response.text))

    token_response = response.json()
    new_access_token = token_response.get('access_token')
    new_refresh_token = token_response.get('refresh_token', refresh_token)

    if not new_access_token:
        raise TwitterOauth2Error('Refresh token response did not contain an access_token')

    LOGGER.info('OAuth 2.0 access token refreshed successfully')
    return new_access_token, new_refresh_token


def persist_refreshed_tokens(config, config_path, access_token, refresh_token):
    """Persist rotated access_token/refresh_token back to the config dict and file (if available)."""
    if config is not None:
        config['access_token'] = access_token
        config['refresh_token'] = refresh_token

    if not config_path:
        return

    try:
        with open(config_path, 'w', encoding='utf-8') as config_file:
            json.dump(config, config_file, indent=2)
    except OSError as err:
        LOGGER.warning('Unable to persist refreshed OAuth 2.0 tokens to config file: {}'.format(err))


class TwitterClient(object):
    def __init__(self,
                 client_id,
                 client_secret,
                 access_token,
                 refresh_token,
                 config=None,
                 config_path=None,
                 user_agent=None):
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__access_token = access_token
        self.__refresh_token = refresh_token
        self.__config = config
        self.__config_path = config_path
        self.__user_agent = user_agent
        self.__verified = False
        self.__session = requests.Session()
        self.base_url = '{}/{}'.format(ADS_API_URL, ADS_API_VERSION)

        if not all([self.__client_id,
                    self.__client_secret,
                    self.__access_token,
                    self.__refresh_token]):
            raise Exception('Missing authentication parameter')

    def __enter__(self):
        self.__verified = self.check_access()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    def __auth_header(self):
        return {'Authorization': 'Bearer {}'.format(self.__access_token)}

    def __refresh(self):
        new_access_token, new_refresh_token = refresh_access_token(
            self.__client_id, self.__client_secret, self.__refresh_token)
        self.__access_token = new_access_token
        self.__refresh_token = new_refresh_token
        persist_refreshed_tokens(self.__config, self.__config_path, new_access_token, new_refresh_token)

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server42xRateLimitError),
                          max_tries=5,
                          factor=2)
    def check_access(self):
        # Endpoint: simple API call to return a single record (org settings) to test access
        url = '{}/accounts&count=1'.format(self.base_url)
        headers = self.__auth_header()
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/json'

        response = self.__session.get(url=url, headers=headers)
        if response.status_code == 401:
            self.__refresh()
            headers.update(self.__auth_header())
            response = self.__session.get(url=url, headers=headers)

        if response.status_code in (420, 429):
            raise Server42xRateLimitError()
        elif 500 <= response.status_code < 600:
            raise Server5xxError()
        elif response.status_code != 200:
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

        kwargs['headers'].update(self.__auth_header())

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(
                method,
                url,
                data=data,
                params=params,
                timeout=(DEFAULT_CONNECTION_TIMEOUT, DEFAULT_REST_TIMEOUT),
                **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        # Expired access_token: refresh and retry once
        if response.status_code == 401:
            self.__refresh()
            kwargs['headers'].update(self.__auth_header())
            with metrics.http_request_timer(endpoint) as timer:
                response = self.__session.request(
                    method,
                    url,
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

        elif 500 <= response.status_code < 600:
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


def patch_twitter_ads_sdk_auth(config, config_path=None):
    """
    The vendored `twitter-ads` SDK (twitter_ads.http.Request) only knows how
    to sign requests with OAuth 1.0a (requests_oauthlib.OAuth1Session). X Ads
    API now also accepts OAuth 2.0 Bearer tokens, so this monkey-patches the
    SDK's private request method to authenticate with
    `Authorization: Bearer <access_token>` instead, and to transparently
    refresh + persist a new access_token/refresh_token pair on a 401 response.

    NOTE: Since `twitter_ads.client.Client` has no OAuth 2.0 fields, the tap
    constructs it re-using its OAuth 1.0a attribute slots to carry OAuth 2.0
    credentials instead:
        consumer_key/consumer_secret -> client_id/client_secret (refresh creds)
        access_token                -> OAuth 2.0 Bearer access_token
        access_token_secret         -> OAuth 2.0 refresh_token
    """
    # Imported lazily so this module has no hard dependency on twitter-ads.
    from twitter_ads.http import Request, Response  # pylint: disable=import-outside-toplevel

    def _oauth2_bearer_request(self):
        client = self.client
        headers = {'user-agent': self._Request__user_agent()}
        if 'headers' in self.options:
            headers.update(self.options['headers'].copy())
        if 'x-as-user' in client.options:
            headers['x-as-user'] = client.options.get('x-as-user')
        for key, val in client.headers.items():
            headers[key] = val

        params = self.options.get('params', None)
        data = self.options.get('body', None)
        files = self.options.get('files', None)
        stream = self.options.get('stream', False)

        handle_rate_limit = client.options.get('handle_rate_limit', False)
        retry_max = client.options.get('retry_max', 0)
        retry_delay = client.options.get('retry_delay', 1500)
        retry_on_status = client.options.get('retry_on_status', [500, 503])
        retry_on_timeouts = client.options.get('retry_on_timeouts', False)
        timeout = client.options.get('timeout', None)

        session = requests.Session()
        method = getattr(session, self._method)
        url = self._Request__domain() + self._resource

        retry_count = 0
        retry_after = None
        refreshed = False
        response = None
        while retry_count <= retry_max:
            headers['Authorization'] = 'Bearer {}'.format(client.access_token)
            try:
                response = method(url, headers=headers, data=data, params=params,
                                   files=files, stream=stream, timeout=timeout)
            except Timeout as e:
                if retry_on_timeouts:
                    if retry_count == retry_max:
                        raise Exception(e)
                    LOGGER.warning('Timeout occurred: resume in %s seconds',
                                   int(retry_delay) / 1000)
                    time.sleep(int(retry_delay) / 1000)
                    retry_count += 1
                    continue
                raise Exception(e)

            # do not retry on 2XX status code
            if 200 <= response.status_code < 300:
                break

            # Expired access_token: refresh once via refresh_token and retry immediately
            if response.status_code == 401 and not refreshed:
                LOGGER.warning('Received 401 from X Ads API, refreshing OAuth 2.0 access token')
                new_access_token, new_refresh_token = refresh_access_token(
                    client.consumer_key, client.consumer_secret, client.access_token_secret)
                # pylint: disable=protected-access
                client._access_token = new_access_token
                client._access_token_secret = new_refresh_token
                persist_refreshed_tokens(config, config_path, new_access_token, new_refresh_token)
                refreshed = True
                continue

            if handle_rate_limit and retry_after is None:
                rate_limit_reset = response.headers.get('x-account-rate-limit-reset') \
                    or response.headers.get('x-rate-limit-reset')

                if response.status_code == 429:
                    retry_after = int(rate_limit_reset) - int(time.time())
                    LOGGER.warning('Request reached Rate Limit: resume in %d seconds', retry_after)
                    time.sleep(retry_after + 5)
                    continue

            if retry_max > 0:
                if response.status_code not in retry_on_status:
                    break
                time.sleep(int(retry_delay) / 1000)

            retry_count += 1

        raw_response_body = response.raw.read() if stream else response.text
        return Response(response.status_code, response.headers,
                        body=response.raw, raw_body=raw_response_body)

    # pylint: disable=protected-access
    Request._Request__oauth_request = _oauth2_bearer_request
