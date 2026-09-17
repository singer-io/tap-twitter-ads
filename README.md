# tap-twitter-ads

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap extracts data from the **X API v2** (`api.x.com/2`, formerly Twitter
API v2) using **OAuth 2.0** exclusively. The legacy OAuth 1.0a Twitter/X Ads
API (`ads-api.x.com`) integration has been removed - see `CHANGELOG.md`.

## Authentication

Two OAuth 2.0 mechanisms are used, depending on what each endpoint supports:

- **User context** (Authorization Code + PKCE) - `client_id` / `client_secret`
  / `access_token` / `refresh_token`. This is required for every stream
  in this tap and is the only mechanism verified to work end-to-end.
  X API v2 **rotates `refresh_token` on every use** (the previous one is
  invalidated instantly) - this tap persists the rotated `access_token` and
  `refresh_token` back to the config file immediately after every refresh.
- **App-only Bearer Token** - required (with no user-context alternative) by
  `compliance_jobs` and `usage_tweets`. A `client_credentials` grant using
  only an OAuth 2.0 `client_id`/`client_secret` does **not** reliably produce
  a token these endpoints accept (confirmed via live testing - X responds
  with 403 "Authenticating with Unknown is forbidden"). If you have a
  genuine classic App-only Bearer Token from the developer portal, set it as
  `bearer_token` in config and these two streams will use it as-is.

## Configuration Reference

Only **5 fields are ever required** - the tap runs fully with just these
(see `config.json.example`):

| Field | Purpose |
|---|---|
| `start_date` | Initial bookmark value for INCREMENTAL streams |
| `client_id` | OAuth 2.0 Client ID |
| `client_secret` | OAuth 2.0 Client Secret |
| `access_token` | OAuth 2.0 user-context access token |
| `refresh_token` | OAuth 2.0 user-context refresh token (rotated + persisted on every use) |

Every other config field is a purely **optional override** of a self-default
the tap resolves automatically from the authenticated user's own data (via
`users_me` and its children) - set one only to look up someone/something
else instead of the authenticated user's own data. This mirrors the
config-field reference documented in the `streams.py` module docstring - if
you change what a field controls, update both.

**Optional - id-list fields** (comma-separated string or JSON array):

| Field | Activates | Default when unset |
|---|---|---|
| `tweet_ids` | `tweets_by_ids`, `post_liking_users`, `post_quote_tweets`, `post_reposted_by`, `post_reposts` | the authenticated user's own post ids (from `user_tweets`) |
| `space_ids` | `spaces_by_ids`, `space_by_id` (+ children `space_tweets`, `space_buyers`) | the authenticated user's own space ids (from `spaces_by_creator_ids`) |
| `creator_ids` | `spaces_by_creator_ids` | the authenticated user's own id |
| `list_ids` | `list_by_id` (+ children `list_tweets`, `list_members`, `list_followers`) | the authenticated user's own owned list ids (from `user_owned_lists`) |
| `woeids` | `trends_by_woeid` | `'1'` (worldwide) |

**Optional - single-value fields**:

| Field | Activates | Default if unset |
|---|---|---|
| `compliance_job_type` | `compliance_jobs` | `tweets` (stream still runs) |
| `post_search_query` | `post_search_recent`, `post_search_all`, `post_counts_recent`, `post_counts_all` | `from:<authenticated username>` (the user's own posts) |
| `community_notes_test_mode` | `community_notes_search_written`, `community_notes_eligible_posts` | `false` (streams still run) |
| `bearer_token` | A genuine App-only Bearer Token, used as-is (no minting attempted) by every `auth='app'` stream: `usage_tweets`, `compliance_jobs`, `bots`, `post_counts_all` | none - those 4 streams fail clearly without it (see `client.py`) |
| `page_size` | Tunes pagination page size for ALL streams | `100` |
| `request_timeout` | Tunes HTTP request timeout (seconds) for ALL streams | `300` |

## Streams

46 streams are implemented, covering every GET endpoint in the X API v2
OpenAPI spec (`docs.x.com/openapi.json`) that supports OAuth 2.0, fits a
batch-poll Singer tap model, and has either no extra id/query dependency or
a sensible self-default derived from the authenticated user's own data (see
Exclusions below for endpoints that need an arbitrary external id/query with
no such default and were removed).

**Authenticated-user streams** (parent `users_me`, no config needed):

| Stream | Endpoint | Replication | Scope |
|---|---|---|---|
| `users_me` | `GET /2/users/me` | FULL_TABLE | `users.read`, `tweet.read` |
| `account` | `GET /2/account` | FULL_TABLE | `developer.read` |
| `usage_tweets` | `GET /2/usage/tweets` | FULL_TABLE | App-only Bearer only |
| `usage_credits` | `GET /2/usage/credits` | FULL_TABLE | - |
| `personalized_trends` | `GET /2/users/personalized_trends` | FULL_TABLE | `users.read`, `tweet.read` |
| `user_reposts_of_me` | `GET /2/users/reposts_of_me` | FULL_TABLE | `timeline.read`, `tweet.read` |
| `bots` | `GET /2/bots` | FULL_TABLE | App-only Bearer only |
| `webhooks` | `GET /2/webhooks` | FULL_TABLE | - |
| `user_tweets` | `GET /2/users/{id}/tweets` | INCREMENTAL (`created_at`) | `tweet.read`, `users.read` |
| `user_mentions` | `GET /2/users/{id}/mentions` | INCREMENTAL (`created_at`) | `tweet.read`, `users.read` |
| `user_home_timeline` | `GET /2/users/{id}/timelines/reverse_chronological` | INCREMENTAL (`created_at`) | `tweet.read`, `users.read` |
| `user_liked_tweets` | `GET /2/users/{id}/liked_tweets` | FULL_TABLE | `like.read` |
| `user_bookmarks` | `GET /2/users/{id}/bookmarks` | FULL_TABLE | `bookmark.read` |
| `user_bookmark_folders` | `GET /2/users/{id}/bookmarks/folders` | FULL_TABLE | `bookmark.read` |
| `user_followers` | `GET /2/users/{id}/followers` | FULL_TABLE | `follows.read` |
| `user_following` | `GET /2/users/{id}/following` | FULL_TABLE | `follows.read` |
| `user_blocking` | `GET /2/users/{id}/blocking` | FULL_TABLE | `block.read` |
| `user_muting` | `GET /2/users/{id}/muting` | FULL_TABLE | `mute.read` |
| `user_owned_lists` | `GET /2/users/{id}/owned_lists` | FULL_TABLE | `list.read` |
| `user_pinned_lists` | `GET /2/users/{id}/pinned_lists` | FULL_TABLE | `list.read` |
| `user_list_memberships` | `GET /2/users/{id}/list_memberships` | FULL_TABLE | `list.read` |
| `user_followed_lists` | `GET /2/users/{id}/followed_lists` | FULL_TABLE | `list.read` |
| `user_affiliates` | `GET /2/users/{id}/affiliates` | FULL_TABLE | `tweet.read`, `users.read` |
| `dm_events` | `GET /2/dm_events` | FULL_TABLE | `dm.read` |

**Config-driven batch lookups** (no parent needed - `config_ids`; each
self-defaults as noted above):

| Stream | Endpoint | Config field |
|---|---|---|
| `tweets_by_ids` | `GET /2/tweets` | `tweet_ids` |
| `spaces_by_ids` | `GET /2/spaces` | `space_ids` |
| `spaces_by_creator_ids` | `GET /2/spaces/by/creator_ids` | `creator_ids` |

**Config-driven parent loops** (each resolved id is its own record *and* a
parent for its children):

| Parent | Endpoint | Config field | Children |
|---|---|---|---|
| `list_by_id` | `GET /2/lists/{id}` | `list_ids` | `list_tweets`, `list_members`, `list_followers` |
| `space_by_id` | `GET /2/spaces/{id}` | `space_ids` | `space_tweets`, `space_buyers` |
| `trends_by_woeid` | `GET /2/trends/by/woeid/{id}` | `woeids` | - |

**Per-post engagement lookups** (config `tweet_ids`, paginated):
`post_liking_users`, `post_quote_tweets`, `post_reposted_by`, `post_reposts`.

**Search / query-driven streams**:

| Stream | Endpoint | Config |
|---|---|---|
| `post_search_recent` | `GET /2/tweets/search/recent` | `post_search_query` (INCREMENTAL) |
| `post_search_all` | `GET /2/tweets/search/all` | `post_search_query` (INCREMENTAL, needs elevated access) |
| `post_counts_recent` | `GET /2/tweets/counts/recent` | `post_search_query` |
| `post_counts_all` | `GET /2/tweets/counts/all` | `post_search_query` (App-only Bearer only) |
| `community_notes_search_written` | `GET /2/notes/search/notes_written` | `community_notes_test_mode` (defaults `false`) |
| `community_notes_eligible_posts` | `GET /2/notes/search/posts_eligible_for_notes` | `community_notes_test_mode` (defaults `false`) |
| `compliance_jobs` | `GET /2/compliance/jobs` | `compliance_job_type` (defaults `tweets`, App-only Bearer only) |

Compliance Notes access, Academic/Pro search tiers, and App-only-Bearer-only
endpoints may return 403/404 depending on your account's access level - this
is an X API access-tier limitation, not a tap defect.

Streams whose endpoint doesn't support the OAuth 2.0 scopes your app was
granted will fail clearly (HTTP 403) without affecting other streams - one
stream's failure never discards data already synced by others.

**Excluded by design**:
- Not REST/batch-poll compatible, or not a data endpoint: persistent
  streaming connections (filtered/sampled/firehose streams, 17 endpoints),
  Webhooks/Account Activity/Activity subscription *management*
  (create/delete/validate - `webhooks`/`GET /2/webhooks` itself IS included
  as a read), Chat (E2EE messaging), Broadcast chat, and media
  upload/status/analytics endpoints (binary uploads, not pollable data).
- Removed because they need an arbitrary external id/query with no
  derivable self-default anywhere in this tap (i.e. no config beyond the 5
  required fields could ever make them return data): `users_by_ids`
  (`user_ids`), `users_by_usernames` (`usernames`), `media_by_keys`
  (`media_keys`), `broadcast_by_id` (`broadcast_ids`),
  `scheduled_broadcast_by_id` (`scheduled_broadcast_ids`), `community_by_id`
  (`community_ids`), `news_by_id` (`news_ids`), `users_search`
  (`users_search_query`), `communities_search` (`communities_search_query`),
  `news_search` (`news_search_query`).

## Quick Start

1. Install:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > pip install -e .
    ```

2. Create `config.json` (see `config.json.example`). Only `start_date`,
   `client_id`, `client_secret`, `access_token`, and `refresh_token` are
   required; every other field is optional and only needed by the specific
   streams that use it (see the Streams table above).

3. Run discovery (does not require a live/valid token - streams are
   statically defined):

    ```bash
    > tap-twitter-ads --config config.json --discover > catalog.json
    ```

4. Select streams in `catalog.json` (set `"selected": true` in each stream's
   top-level metadata), then run sync:

    ```bash
    > tap-twitter-ads --config config.json --catalog catalog.json --state state.json
    ```

## Tests

```bash
> pip install -e .
> python -m pytest tests/unittests -q
```
