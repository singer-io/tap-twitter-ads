"""Unit tests for discovery and stream access checks."""
import subprocess
import sys
import textwrap
import unittest
from unittest import mock

from tap_twitter_ads.discover import _apply_access_checks, _children_by_parent, check_access, discover
from tap_twitter_ads.exceptions import (
    XApiAuthenticationError,
    XApiBadRequestError,
    XApiForbiddenError,
    XApiNotFoundError,
    XApiPaymentRequiredError,
    XApiServerError,
)
from tap_twitter_ads.streams import STREAMS


def make_config():
    return {
        'start_date': '2020-01-01T00:00:00Z',
        'client_id': 'cid',
        'client_secret': 'csecret',
        'access_token': 'at',
        'refresh_token': 'rt',
    }


def make_all_accessible_client():
    """Return a client whose requests always succeed."""
    client = mock.Mock()
    client.get.return_value = {'data': {'id': '123', 'username': 'someuser'}}
    return client


class TestDiscover(unittest.TestCase):
    def test_discover_returns_one_entry_per_stream_when_all_accessible(self):
        catalog = discover(make_all_accessible_client(), make_config())
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertEqual(stream_ids, set(STREAMS.keys()))

    def test_every_stream_schema_loads_without_error(self):
        catalog = discover(make_all_accessible_client(), make_config())
        for entry in catalog.streams:
            schema_dict = entry.schema.to_dict()
            self.assertIn('properties', schema_dict)
            self.assertTrue(len(schema_dict['properties']) > 0, entry.tap_stream_id)

    def test_key_properties_match_stream_definition(self):
        catalog = discover(make_all_accessible_client(), make_config())
        by_id = {entry.tap_stream_id: entry for entry in catalog.streams}
        for name, stream in STREAMS.items():
            self.assertEqual(by_id[name].key_properties, stream.key_properties, name)

    def test_child_streams_have_parent_tap_stream_id_metadata(self):
        catalog = discover(make_all_accessible_client(), make_config())
        by_id = {entry.tap_stream_id: entry for entry in catalog.streams}
        for name, stream in STREAMS.items():
            if not stream.parent:
                continue
            table_md = next(m['metadata'] for m in by_id[name].metadata if m['breadcrumb'] == ())
            self.assertEqual(table_md.get('parent-tap-stream-id'), stream.parent, name)

    def test_incremental_streams_have_replication_key_marked_automatic(self):
        catalog = discover(make_all_accessible_client(), make_config())
        by_id = {entry.tap_stream_id: entry for entry in catalog.streams}
        for name, stream in STREAMS.items():
            if stream.replication_method != 'INCREMENTAL':
                continue
            field_md = next(
                m['metadata'] for m in by_id[name].metadata
                if m['breadcrumb'] == ('properties', stream.replication_key))
            self.assertEqual(field_md.get('inclusion'), 'automatic', name)


class FakeClient:
    """Lightweight client stub for controlling discovery responses."""

    def __init__(self, responses=None, raises=None, default_raise=None):
        self.responses = responses or {}
        self.raises = raises or {}
        self.default_raise = default_raise
        self.calls = []

    def get(self, path, params=None, auth='app'):
        self.calls.append((path, auth))
        for prefix, exc in self.raises.items():
            if path.startswith(prefix):
                raise exc
        if self.default_raise is not None:
            raise self.default_raise
        for prefix, resp in self.responses.items():
            if path.startswith(prefix):
                return resp
        return {'data': {'id': '999', 'username': 'defaultuser'}}


class TestCheckAccessBasics(unittest.TestCase):

    def test_accessible_stream_returns_true(self):
        client = FakeClient()
        self.assertTrue(check_access(STREAMS['account'], client, make_config()))

    def test_accessible_stream_remains_in_catalog(self):
        client = FakeClient()
        catalog = discover(client, make_config())
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertIn('account', stream_ids)

    def test_unexpected_client_error_propagates_not_treated_as_inaccessible(self):
        """A 400 (malformed request) must never be silently treated as
        'inaccessible' - it's a real bug/bad-request, not an access problem."""
        client = FakeClient(raises={'/2/account': XApiBadRequestError('HTTP-error-code: 400, Message: bad request')})
        with self.assertRaises(XApiBadRequestError):
            check_access(STREAMS['account'], client, make_config())

    def test_unexpected_server_error_propagates(self):
        client = FakeClient(raises={'/2/account': XApiServerError('HTTP-error-code: 500, Message: server error')})
        with self.assertRaises(XApiServerError):
            check_access(STREAMS['account'], client, make_config())

    def test_apply_access_checks_does_not_swallow_unexpected_errors(self):
        client = FakeClient(raises={'/2/account': XApiBadRequestError('HTTP-error-code: 400, Message: bad request')})
        with self.assertRaises(XApiBadRequestError):
            _apply_access_checks(client, make_config())

    def test_auth_user_stream_probed_with_user_auth(self):
        """I. auth=user stream - access check uses the OAuth2 user token path."""
        client = FakeClient()
        self.assertEqual(STREAMS['account'].auth, 'user')
        check_access(STREAMS['account'], client, make_config())
        self.assertTrue(any(path == '/2/account' and auth == 'user' for path, auth in client.calls))

    def test_auth_app_stream_probed_with_app_auth(self):
        """J. auth=app stream - access check uses the existing OAuth2
        Application-Only path (no new authentication mechanism introduced)."""
        client = FakeClient()
        self.assertEqual(STREAMS['usage_tweets'].auth, 'app')
        check_access(STREAMS['usage_tweets'], client, make_config())
        self.assertTrue(any(path == '/2/usage/tweets' and auth == 'app' for path, auth in client.calls))


class TestCheckAccessStatusMapping(unittest.TestCase):

    def test_401_propagates_and_fails_discovery(self):
        """401 is a connection-level failure, not a stream-level exclusion."""
        client = FakeClient(raises={'/2/usage/credits': XApiAuthenticationError(
            'HTTP-error-code: 401, Message: Unauthorized')})
        with self.assertRaises(XApiAuthenticationError):
            check_access(STREAMS['usage_credits'], client, make_config())

    def test_403_marks_stream_inaccessible(self):
        client = FakeClient(raises={'/2/usage/tweets': XApiForbiddenError(
            'HTTP-error-code: 403, Message: Forbidden')})
        self.assertFalse(check_access(STREAMS['usage_tweets'], client, make_config()))

    def test_402_marks_stream_inaccessible(self):
        """C. A 402 (credits/usage restriction) must exclude the stream, same
        as 403 - and only because it's genuinely mapped to
        XApiPaymentRequiredError, not via a blanket except-Exception."""
        client = FakeClient(raises={'/2/account': XApiPaymentRequiredError(
            'HTTP-error-code: 402, Message: Usage cap exceeded')})
        self.assertFalse(check_access(STREAMS['account'], client, make_config()))

    def test_404_does_not_mark_stream_inaccessible(self):
        """404 means the endpoint is reachable but the looked-up resource
        doesn't exist - not the same as the stream itself being
        inaccessible, so it must stay True."""
        client = FakeClient(raises={'/2/account': XApiNotFoundError('HTTP-error-code: 404, Message: Not Found')})
        self.assertTrue(check_access(STREAMS['account'], client, make_config()))

    def test_inaccessible_stream_excluded_from_catalog(self):
        """B. inaccessible stream is excluded from the discovered catalog."""
        client = FakeClient(raises={'/2/usage/credits': XApiForbiddenError(
            'HTTP-error-code: 403, Message: Forbidden')})
        catalog = discover(client, make_config())
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertNotIn('usage_credits', stream_ids)


class TestParentChildAccessChecks(unittest.TestCase):

    def test_parent_inaccessible_child_never_independently_checked(self):
        client = FakeClient(raises={'/2/users/me': XApiForbiddenError(
            'HTTP-error-code: 403, Message: Forbidden')})

        with mock.patch('tap_twitter_ads.discover.check_access', wraps=check_access) as spy:
            accessible = _apply_access_checks(client, make_config())

        children = _children_by_parent()['users_me']
        checked_stream_ids = [call.args[0].tap_stream_id for call in spy.call_args_list]

        for child in children:
            self.assertNotIn(child, accessible)
            self.assertNotIn(child, checked_stream_ids)

    def test_parent_inaccessible_excludes_parent_from_catalog_too(self):
        client = FakeClient(raises={'/2/users/me': XApiForbiddenError(
            'HTTP-error-code: 403, Message: Forbidden')})
        catalog = discover(client, make_config())
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertNotIn('users_me', stream_ids)
        self.assertNotIn('user_tweets', stream_ids)
        self.assertNotIn('user_mentions', stream_ids)

    def test_parent_accessible_child_is_independently_checked(self):
        client = FakeClient()  # everything succeeds

        with mock.patch('tap_twitter_ads.discover.check_access', wraps=check_access) as spy:
            accessible = _apply_access_checks(client, make_config())

        checked_stream_ids = [call.args[0].tap_stream_id for call in spy.call_args_list]
        self.assertIn('user_tweets', checked_stream_ids)
        self.assertIn('user_tweets', accessible)

    def test_parent_accessible_but_one_child_forbidden_only_that_child_excluded(self):
        client = FakeClient(raises={'/2/users/999/tweets': XApiForbiddenError(
            'HTTP-error-code: 403, Message: Forbidden')})

        accessible = _apply_access_checks(client, make_config())

        self.assertIn('users_me', accessible)
        self.assertNotIn('user_tweets', accessible)
        # A sibling child stream, independently accessible, must remain.
        self.assertIn('user_mentions', accessible)


class TestMultipleAndAllInaccessible(unittest.TestCase):

    def test_multiple_inaccessible_streams_removed_others_remain(self):
        client = FakeClient(raises={
            '/2/account': XApiForbiddenError('HTTP-error-code: 403, Message: Forbidden'),
            '/2/usage/credits': XApiPaymentRequiredError('HTTP-error-code: 402, Message: Usage cap exceeded'),
        })

        accessible = _apply_access_checks(client, make_config())

        self.assertNotIn('account', accessible)
        self.assertNotIn('usage_credits', accessible)
        # users_me (and therefore its children) were never targeted - stay accessible.
        self.assertIn('users_me', accessible)
        self.assertIn('user_tweets', accessible)

    def test_all_streams_inaccessible_raises_actionable_error(self):
        """H. Rather than silently returning an empty catalog, a fully
        inaccessible connection raises an actionable error (existing
        convention used elsewhere for a fully-forbidden connection).
        Explicit ids are configured so every id-driven stream (including
        list_by_id/space_by_id and their children) is actively probed
        instead of left unverified - see check_access's 'not configured'
        semantics."""
        config = dict(make_config(), tweet_ids='1', list_ids='1', space_ids='1',
                      creator_ids='1', post_search_query='from:someuser')
        client = FakeClient(default_raise=XApiForbiddenError('HTTP-error-code: 403, Message: Forbidden'))

        with self.assertRaises(XApiForbiddenError):
            _apply_access_checks(client, config)

    def test_all_streams_inaccessible_propagates_through_discover(self):
        """H. all streams -> 403 -> catalog empty -> XApiForbiddenError. The
        actual CLI-process-exit-code assertion lives in
        TestCLIProcessExitOnAllStreamsInaccessible below."""
        config = dict(make_config(), tweet_ids='1', list_ids='1', space_ids='1',
                      creator_ids='1', post_search_query='from:someuser')
        client = FakeClient(default_raise=XApiForbiddenError('HTTP-error-code: 403, Message: Forbidden'))

        with self.assertRaises(XApiForbiddenError):
            discover(client, config)

    def test_401_from_any_stream_propagates_through_apply_access_checks(self):
        """A single stream returning 401 must fail the whole access-check
        pass immediately - it is a connection-level credential failure, not
        aggregated together with per-stream 402/403 exclusions."""
        client = FakeClient(raises={'/2/account': XApiAuthenticationError(
            'HTTP-error-code: 401, Message: Unauthorized')})
        with self.assertRaises(XApiAuthenticationError):
            _apply_access_checks(client, make_config())

    def test_401_from_users_me_propagates_through_discover(self):
        """The dedicated users_me probe must not swallow a 401 either - it
        must propagate all the way through discover() and fail discovery."""
        client = FakeClient(raises={'/2/users/me': XApiAuthenticationError(
            'HTTP-error-code: 401, Message: Unauthorized')})
        with self.assertRaises(XApiAuthenticationError):
            discover(client, make_config())


class TestCLIProcessExitOnAllStreamsInaccessible(unittest.TestCase):
    """Verify discovery failure results in a non-zero CLI exit."""

    def test_cli_exits_nonzero_and_emits_no_catalog_when_all_streams_inaccessible(self):
        script = textwrap.dedent("""
            import sys
            from unittest import mock

            from tap_twitter_ads.exceptions import XApiForbiddenError

            class _Args:
                discover = True
                state = None
                catalog = None
                config_path = None
                config = {
                    'start_date': '2020-01-01T00:00:00Z',
                    'client_id': 'cid',
                    'client_secret': 'csecret',
                    'access_token': 'at',
                    'refresh_token': 'rt',
                }

            with mock.patch('singer.utils.parse_args', return_value=_Args()), \\
                 mock.patch('tap_twitter_ads.XApiClient', return_value=mock.MagicMock()), \\
                 mock.patch(
                     'tap_twitter_ads.discover',
                     side_effect=XApiForbiddenError(
                         'HTTP-error-code: 403, Error: The credentials do not '
                         'have access to any supported streams.'
                     )
                 ):
                import tap_twitter_ads
                # Mirrors the generated console-script wrapper: sys.exit(main())
                sys.exit(tap_twitter_ads.main())
        """)

        result = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(
            result.returncode, 0,
            "CLI process must exit non-zero when all streams are inaccessible. "
            "stdout={!r} stderr={!r}".format(result.stdout, result.stderr)
        )
        self.assertNotIn('"streams"', result.stdout)
