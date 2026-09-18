"""
OAuth 2.0 REST client for X API v2 (api.x.com/2).

Auth modes:
  - User context (Authorization Code + PKCE - `access_token`/`refresh_token`)
    - the ONLY auth mode verified to work with this app's `client_id`/
    `client_secret` (confirmed via live testing: X API v2 rejected every
    request made with a `client_credentials`-grant token as "Unknown" auth,
    even for endpoints whose docs list `OAuth 2.0 Application-Only` as
    supported - see below). Used by default for almost every stream.
    X API v2 ROTATES `refresh_token` on every use - the previous one is
    invalidated instantly - so this client persists rotated tokens back to
    the config file immediately (synchronously, inside
    `refresh_access_token()`) and only refreshes when the current
    access_token is actually rejected (401).
  - App-only Bearer Token - X API v2 expects a genuine "OAuth 2.0
    Application-Only" token for a handful of endpoints (`compliance_jobs`,
    `usage_tweets`) that do NOT support user context at all. The classic way
    to obtain one is a `client_credentials` grant using a project's
    *classic* API Key/Secret (Twitter API v1.1-style consumer key/secret,
    issued separately from the OAuth 2.0 Client ID/Secret used for the
    Authorization Code flow) - this app only has the latter, and minting a
    token via `client_credentials` with it returns HTTP 200 but the
    resulting token is NOT accepted by these endpoints (403 "Authenticating
    with Unknown is forbidden"). Rather than rely on that non-functional
    token, this client only uses a *statically configured* `bearer_token`
    (an optional config field - paste a genuine App-only Bearer Token from
    the developer portal, if you have one) for `auth='app'` streams; without
    it those streams fail clearly rather than silently.
"""
import json
import time

import backoff
import requests
import singer

from tap_twitter_ads.exceptions import (
    XApiAuthenticationError,
    XApiBackoffError,
    XApiClientError,
    XApiRateLimitError,
    XApiServerError,
    raise_for_error_v2,
)

LOGGER = singer.get_logger()

TOKEN_URL = 'https://api.x.com/2/oauth2/token'
API_BASE_URL = 'https://api.x.com'
DEFAULT_REQUEST_TIMEOUT = 300  # seconds
APP_TOKEN_SCOPE = 'users.read tweet.read'  # app-only tokens don't gate on scope per-endpoint


class XApiClient:
    """Thin `requests`-based REST client for `api.x.com/2`. See the module
    docstring for the two auth modes (`auth='user'` / `auth='app'`) this
    client supports and why `auth='user'` is the default/primary one."""

    def __init__(self, config, config_path=None):
        """`config_path`, if given, is the on-disk path to the tap's config
        file - required for `refresh_access_token()` to persist rotated
        tokens; without it, a warning is logged and rotated tokens are lost
        at process exit."""
        self.client_id = config.get('client_id')
        self.client_secret = config.get('client_secret')
        self.access_token = config.get('access_token')
        self.refresh_token = config.get('refresh_token')
        self.config_path = config_path

        # Optional: a genuine App-only Bearer Token pasted into config, used
        # as-is for `auth='app'` streams. `client_credentials` minting is kept
        # as a best-effort fallback (see module docstring) but is NOT relied
        # on by default since it's been confirmed non-functional for apps
        # that only have an OAuth 2.0 Client ID/Secret (no classic API Key/Secret).
        self.app_access_token = config.get('bearer_token')

        request_timeout = config.get('request_timeout')
        try:
            self.request_timeout = float(request_timeout) if request_timeout else DEFAULT_REQUEST_TIMEOUT
        except (TypeError, ValueError):
            self.request_timeout = DEFAULT_REQUEST_TIMEOUT

        if not all([self.client_id, self.client_secret, self.access_token, self.refresh_token]):
            raise XApiAuthenticationError(
                'Missing OAuth 2.0 credential(s): client_id, client_secret, access_token, '
                'and refresh_token are all required in config')

        self.session = requests.Session()

    # ---------------------------------------------------------------- #
    # User context (Authorization Code + PKCE) token management
    # ---------------------------------------------------------------- #
    def _persist_tokens(self):
        """Write the current access_token/refresh_token back to the config file on disk."""
        if not self.config_path:
            LOGGER.warning(
                'No config_path available - rotated OAuth 2.0 tokens were NOT persisted to '
                'disk. The next tap run will fail with an invalid/expired refresh_token.')
            return
        with open(self.config_path, 'r', encoding='utf-8') as f:
            on_disk_config = json.load(f)
        on_disk_config['access_token'] = self.access_token
        on_disk_config['refresh_token'] = self.refresh_token
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(on_disk_config, f, indent=2)
        LOGGER.info('Persisted rotated OAuth 2.0 access_token/refresh_token to config file')

    @backoff.on_exception(backoff.expo, (XApiServerError, requests.exceptions.ConnectionError),
                           max_tries=5, factor=2)
    def refresh_access_token(self):
        """Exchange the current refresh_token for a new access_token + refresh_token."""
        if not self.refresh_token:
            raise XApiAuthenticationError('No refresh_token available to refresh the OAuth 2.0 access token')

        LOGGER.info('User-context OAuth 2.0 access token expired/invalid - refreshing')
        response = self.session.post(
            TOKEN_URL,
            data={
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token,
                'client_id': self.client_id,
            },
            auth=(self.client_id, self.client_secret),
            timeout=self.request_timeout,
        )
        if response.status_code != 200:
            raise_for_error_v2(response)

        body = response.json()
        self.access_token = body['access_token']
        self.refresh_token = body.get('refresh_token', self.refresh_token)
        # Persist immediately - the previous refresh_token is now invalid and unusable.
        self._persist_tokens()

    # ---------------------------------------------------------------- #
    # App-only Bearer token management (client_credentials grant)
    # ---------------------------------------------------------------- #
    @backoff.on_exception(backoff.expo, (XApiServerError, requests.exceptions.ConnectionError),
                           max_tries=5, factor=2)
    def _mint_app_token(self):
        """Exchange client_id/client_secret for an App-only Bearer token via a
        `client_credentials` grant. Kept as a best-effort fallback when no
        static `bearer_token` was configured, but per the module docstring
        this is confirmed NOT to produce a token X API's data endpoints will
        actually accept for this app - only useful if X's behavior changes."""
        LOGGER.info('Minting App-only Bearer token (client_credentials grant)')
        response = self.session.post(
            TOKEN_URL,
            data={
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'client_type': 'third_party_app',
                'scope': APP_TOKEN_SCOPE,
            },
            auth=(self.client_id, self.client_secret),
            timeout=self.request_timeout,
        )
        if response.status_code != 200:
            raise_for_error_v2(response)
        self.app_access_token = response.json()['access_token']

    # ---------------------------------------------------------------- #
    # HTTP
    # ---------------------------------------------------------------- #
    @backoff.on_exception(backoff.expo,
                           (XApiServerError, XApiRateLimitError, requests.exceptions.ConnectionError),
                           max_tries=5, factor=2)
    def _request(self, path, params, token):
        """Perform a single GET with the given Bearer `token`, handling 429
        (sleep until `x-rate-limit-reset`, then let the `@backoff` decorator
        retry) and raising a mapped `XApi*Error` for any other non-200."""
        url = '{}{}'.format(API_BASE_URL, path)
        headers = {'Authorization': 'Bearer {}'.format(token)}
        response = self.session.get(url, headers=headers, params=params, timeout=self.request_timeout)

        if response.status_code == 429:
            reset_at = response.headers.get('x-rate-limit-reset')
            sleep_secs = 30
            if reset_at:
                try:
                    sleep_secs = max(1, min(60, int(reset_at) - int(time.time())))
                except ValueError:
                    pass
            LOGGER.warning('Rate limited (429) on %s - sleeping %s seconds before retry', path, sleep_secs)
            time.sleep(sleep_secs)

        if response.status_code != 200:
            raise_for_error_v2(response)

        return response

    def get(self, path, params=None, auth='app', _allow_refresh=True):
        """GET `path`. `auth='app'` uses (and lazily mints) an App-only Bearer
        token; `auth='user'` uses the user-context access_token, refreshing
        once on 401 before giving up."""
        LOGGER.info('Request URL: %s%s, params: %s, auth: %s', API_BASE_URL, path, params, auth)

        if auth == 'app':
            if not self.app_access_token:
                self._mint_app_token()
            try:
                response = self._request(path, params, self.app_access_token)
            except XApiAuthenticationError:
                if not _allow_refresh:
                    raise
                self._mint_app_token()
                return self.get(path, params=params, auth='app', _allow_refresh=False)
            return response.json()

        try:
            response = self._request(path, params, self.access_token)
        except XApiAuthenticationError:
            if not _allow_refresh:
                raise
            self.refresh_access_token()
            return self.get(path, params=params, auth='user', _allow_refresh=False)
        return response.json()

    def check_credentials(self):
        """Validate the configured OAuth 2.0 credentials by making one
        lightweight authenticated call (`GET /2/users/me`, `auth='user'`).
        Called once at tap startup (before sync) so a bad/expired
        client_id/client_secret/access_token/refresh_token combination fails
        fast with a clear error instead of partway through the first stream.
        A 401 here is still handled by the normal refresh-and-retry path in
        `get()`, so a merely-expired access_token does not fail this check.
        Raises `XApiAuthenticationError` (chaining the original error) if
        credentials are invalid/rejected.
        """
        try:
            self.get('/2/users/me', auth='user')
        except (XApiClientError, XApiBackoffError) as exc:
            LOGGER.error('Credential check failed - could not authenticate with X API v2: %s', exc)
            raise XApiAuthenticationError(
                'Unable to authenticate with X API v2 using the configured OAuth 2.0 '
                'credentials. Verify client_id, client_secret, access_token, and '
                'refresh_token are all valid.') from exc
        LOGGER.info('Credential check passed - OAuth 2.0 credentials are valid')

    def get_paginated(self, path, params, auth='app', page_size=None, max_pages=None):
        """Yield each page (raw response dict) for a cursor-paginated X API v2
        endpoint that returns a `meta.next_token`."""
        params = dict(params or {})
        if page_size:
            params['max_results'] = page_size

        page_count = 0
        while True:
            page = self.get(path, params=params, auth=auth)
            yield page
            page_count += 1

            next_token = page.get('meta', {}).get('next_token')
            if not next_token:
                break
            if max_pages and page_count >= max_pages:
                LOGGER.warning('Stream path %s - stopping after max_pages=%s safety cap', path, max_pages)
                break
            params['pagination_token'] = next_token
