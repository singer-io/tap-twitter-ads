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

`config.json` fields fall into two groups: fields that are always required,
and optional fields that each activate a specific set of streams (see
`config.json.example`). This mirrors the config-field reference documented
in the `streams.py` module docstring - if you change what a field controls,
update both.

**Always required** (every sync needs these regardless of which streams are selected):

| Field | Purpose |
|---|---|
| `start_date` | Initial bookmark value for INCREMENTAL streams |
| `client_id` | OAuth 2.0 Client ID |
| `client_secret` | OAuth 2.0 Client Secret |
| `access_token` | OAuth 2.0 user-context access token |
| `refresh_token` | OAuth 2.0 user-context refresh token (rotated + persisted on every use) |

**Optional - id-list fields** (comma-separated string or JSON array; the
stream(s) listed are skipped with a logged warning, never an error, if the
field is unset):

| Field | Activates |
|---|---|
| `user_ids` | `users_by_ids` |
| `usernames` | `users_by_usernames` |
| `tweet_ids` | `tweets_by_ids`, `post_liking_users`, `post_quote_tweets`, `post_reposted_by`, `post_reposts` |
| `space_ids` | `spaces_by_ids`, `space_by_id` (+ children `space_tweets`, `space_buyers`) |
| `creator_ids` | `spaces_by_creator_ids` (falls back to the authenticated user's own id if unset - never skipped) |
| `list_ids` | `list_by_id` (+ children `list_tweets`, `list_members`, `list_followers`) |
| `media_keys` | `media_by_keys` |
| `broadcast_ids` | `broadcast_by_id` |
| `scheduled_broadcast_ids` | `scheduled_broadcast_by_id` |
| `community_ids` | `community_by_id` |
| `news_ids` | `news_by_id` |
| `woeids` | `trends_by_woeid` |

**Optional - single-value fields**:

| Field | Activates | Default if unset |
|---|---|---|
| `compliance_job_type` | `compliance_jobs` | `tweets` (stream still runs) |
| `users_search_query` | `users_search` | none - **required for that stream**, skipped without it |
| `post_search_query` | `post_search_recent`, `post_search_all`, `post_counts_recent`, `post_counts_all` | none - **required for those streams**, skipped without it |
| `communities_search_query` | `communities_search` | none - **required for that stream**, skipped without it |
| `news_search_query` | `news_search` | none - **required for that stream**, skipped without it |
| `community_notes_test_mode` | `community_notes_search_written`, `community_notes_eligible_posts` | `false` (streams still run) |
| `bearer_token` | A genuine App-only Bearer Token, used as-is (no minting attempted) by every `auth='app'` stream: `usage_tweets`, `compliance_jobs`, `bots`, `post_counts_all` | none - those 4 streams fail clearly without it (see `client.py`) |
| `page_size` | Tunes pagination page size for ALL streams | `100` |
| `request_timeout` | Tunes HTTP request timeout (seconds) for ALL streams | `300` |

## Streams

56 streams are implemented, covering every GET endpoint in the X API v2
OpenAPI spec (`docs.x.com/openapi.json`) that supports OAuth 2.0 and fits a
batch-poll Singer tap model (see Exclusions below).

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

**Config-driven batch lookups** (no parent needed - `config_ids`):

| Stream | Endpoint | Config field |
|---|---|---|
| `users_by_ids` | `GET /2/users` | `user_ids` |
| `users_by_usernames` | `GET /2/users/by` | `usernames` |
| `tweets_by_ids` | `GET /2/tweets` | `tweet_ids` |
| `spaces_by_ids` | `GET /2/spaces` | `space_ids` |
| `spaces_by_creator_ids` | `GET /2/spaces/by/creator_ids` | `creator_ids` (defaults to the authenticated user) |
| `media_by_keys` | `GET /2/media` | `media_keys` |

**Config-driven parent loops** (each configured id is its own record *and* a
parent for its children):

| Parent | Endpoint | Config field | Children |
|---|---|---|---|
| `list_by_id` | `GET /2/lists/{id}` | `list_ids` | `list_tweets`, `list_members`, `list_followers` |
| `space_by_id` | `GET /2/spaces/{id}` | `space_ids` | `space_tweets`, `space_buyers` |
| `broadcast_by_id` | `GET /2/broadcasts/{id}` | `broadcast_ids` | - |
| `scheduled_broadcast_by_id` | `GET /2/broadcasts/scheduled/{id}` | `scheduled_broadcast_ids` | - |
| `community_by_id` | `GET /2/communities/{id}` | `community_ids` | - |
| `news_by_id` | `GET /2/news/{id}` | `news_ids` | - |
| `trends_by_woeid` | `GET /2/trends/by/woeid/{id}` | `woeids` | - |

**Per-post engagement lookups** (config `tweet_ids`, paginated):
`post_liking_users`, `post_quote_tweets`, `post_reposted_by`, `post_reposts`.

**Search / query-driven streams** (skipped with a clear warning if their
required config field isn't set):

| Stream | Endpoint | Required config |
|---|---|---|
| `users_search` | `GET /2/users/search` | `users_search_query` |
| `post_search_recent` | `GET /2/tweets/search/recent` | `post_search_query` (INCREMENTAL) |
| `post_search_all` | `GET /2/tweets/search/all` | `post_search_query` (INCREMENTAL, needs elevated access) |
| `post_counts_recent` | `GET /2/tweets/counts/recent` | `post_search_query` |
| `post_counts_all` | `GET /2/tweets/counts/all` | `post_search_query` (App-only Bearer only) |
| `communities_search` | `GET /2/communities/search` | `communities_search_query` |
| `news_search` | `GET /2/news/search` | `news_search_query` |
| `community_notes_search_written` | `GET /2/notes/search/notes_written` | `community_notes_test_mode` (defaults `false`) |
| `community_notes_eligible_posts` | `GET /2/notes/search/posts_eligible_for_notes` | `community_notes_test_mode` (defaults `false`) |
| `compliance_jobs` | `GET /2/compliance/jobs` | `compliance_job_type` (defaults `tweets`, App-only Bearer only) |

Compliance Notes access, Academic/Pro search tiers, and App-only-Bearer-only
endpoints may return 403/404 depending on your account's access level - this
is an X API access-tier limitation, not a tap defect.

Streams whose endpoint doesn't support the OAuth 2.0 scopes your app was
granted will fail clearly (HTTP 403) without affecting other streams - one
stream's failure never discards data already synced by others, and a
required-but-unconfigured stream is skipped with an explicit warning
(never silently).

**Excluded by design** (not REST/batch-poll compatible, or not a data
endpoint): persistent streaming connections (filtered/sampled/firehose
streams, 17 endpoints), Webhooks/Account Activity/Activity subscription
*management* (create/delete/validate - `webhooks`/`GET /2/webhooks` itself
IS included as a read), Chat (E2EE messaging), Broadcast chat, and media
upload/status/analytics endpoints (binary uploads, not pollable data).

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
