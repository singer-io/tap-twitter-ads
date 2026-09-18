"""
Registry of every OAuth 2.0 X API v2 (api.x.com/2) stream this tap
implements, as real Python classes (one concrete class per stream, each with
its own JSON schema file `schemas/<tap_stream_id>.json`).

Class hierarchy:
  `Stream` (abstract base - common attributes/defaults, `.parent` and
  `.schema_file` properties, auto-registration via `__init_subclass__`)
    -> category base classes, one per `source_type`, encoding the shared
       sync behavior for that shape of endpoint (see sync.py):
         SingletonStream, SingletonListStream, ParentStream, ConfigIdsStream,
         ConfigIdsSelfDefaultStream(ConfigIdsStream), ConfigLoopStream,
         ConfigLoopMultiStream, SearchStream
    -> concrete stream classes (UsersMe, UserTweets, ...), each setting only
       the attributes that differ per endpoint (path, key_properties, auth,
       params, source_key, replication_method/key, required_params, ...).
       Each concrete class's docstring states, in one line, what the stream
       returns.

The tap runs with just `start_date`, `client_id`, `client_secret`,
`access_token`, and `refresh_token`. Streams that look up a specific id/list
(tweets, spaces, lists, trends, searches, ...) default to the authenticated
user's own data unless told otherwise.

Field-selection constants below intentionally request a curated, useful
subset of each object's ~20-50 available fields (not every field X offers)
to keep schemas/records readable; add to these lists to pull more fields.

Where a stream's data requires the resource owner's own permissions and the
endpoint does NOT support an App-only Bearer token, `auth='user'` is used
(rotating OAuth2UserToken flow). Every other stream uses `auth='app'`
(App-only Bearer, minted from client_id/client_secret - never rotates, so
it carries none of the refresh_token loss risk).
"""

DEFAULT_PAGE_SIZE = 100
ID_CHUNK_SIZE = 100  # X API v2 caps most `ids`/`usernames` query params at 100

USER_FIELDS = (
    'id,name,username,created_at,description,location,url,profile_image_url,'
    'protected,verified,verified_type,pinned_post_id,most_recent_post_id,public_metrics'
)

POST_FIELDS = (
    'id,text,created_at,author_id,conversation_id,in_reply_to_user_id,lang,source,'
    'possibly_sensitive,reply_settings,edit_history_post_ids,public_metrics,'
    'referenced_posts,attachments'
)

LIST_FIELDS = 'id,name,description,owner_id,created_at,private,follower_count,member_count'

SPACE_FIELDS = (
    'id,title,state,created_at,started_at,ended_at,scheduled_start,lang,'
    'creator_id,host_ids,speaker_ids,invited_user_ids,participant_count,'
    'subscriber_count,is_ticketed,topic_ids,updated_at'
)

TREND_FIELDS = 'trend_name,tweet_count'
PERSONALIZED_TREND_FIELDS = 'trend_name,post_count,category,trending_since'
COMPLIANCE_JOB_FIELDS = (
    'id,type,status,created_at,name,upload_url,upload_expires_at,download_url,'
    'download_expires_at,resumable'
)
USAGE_FIELDS = 'cap_reset_day,project_cap,project_id,project_usage,daily_project_usage'
USAGE_CREDITS_FIELDS = 'total_balance,free_balance,prepaid_balance'
DM_EVENT_FIELDS = ('id,event_type,text,dm_conversation_id,sender_id,participant_ids,'
                    'created_at,referenced_posts,attachments')
COMMUNITY_NOTE_FIELDS = 'id,status,scoring_status,info,test_result'
POST_COUNT_FIELDS = 'start,end,post_count'
WEBHOOK_FIELDS = 'id,url,valid,created_at'


class Stream:
    """Abstract base for every X API v2 stream - do not instantiate directly.

    Concrete subclasses (bottom of this file) set the per-endpoint
    attributes (`tap_stream_id`, `path`, `key_properties`, `auth`, `params`,
    ...); category base classes just below set the shared defaults/behavior
    for one `source_type` (see sync.py for how each source_type is synced).
    """

    tap_stream_id = None
    path = None
    key_properties = []
    replication_method = 'FULL_TABLE'
    replication_key = None
    auth = 'app'
    params = {}
    required_params = {}
    paginated = True
    source_key = None  # meaning depends on source_type - see category classes below
    id_query_param = None
    chunk_size = ID_CHUNK_SIZE
    source_type = None

    _registry = {}

    def __init_subclass__(cls, **kwargs):
        """Auto-register every concrete (tap_stream_id-bearing) subclass into
        `Stream._registry`, so `STREAMS` never needs a manually-maintained
        list of stream classes to stay in sync with."""
        super().__init_subclass__(**kwargs)
        if cls.tap_stream_id:
            Stream._registry[cls.tap_stream_id] = cls

    @property
    def schema_file(self):
        """Every stream owns its own schema JSON file: schemas/<tap_stream_id>.json."""
        return self.tap_stream_id

    @property
    def parent(self):
        """Only 'parent' streams have a true parent STREAM (used for
        parent-tap-stream-id catalog metadata and for locating children via
        `_children_of()` in sync.py). Other source types also set
        `source_key`, but to something other than a stream id, so it must
        NOT be exposed as `.parent`."""
        return self.source_key if self.source_type == 'parent' else None


# ==========================================================================
# Category base classes - one per source_type, shared sync behavior/defaults
# ==========================================================================

class SingletonStream(Stream):
    """One record, no id needed, `data` is a single object."""
    source_type = 'singleton'
    paginated = False


class SingletonListStream(Stream):
    """One call, no id needed, `data` is an array of records (all emitted)."""
    source_type = 'singleton_list'
    paginated = False


class ParentStream(Stream):
    """`{id}` in `path` comes from a parent stream's record id (`source_key`
    holds that parent stream's tap_stream_id)."""
    source_type = 'parent'


class ConfigIdsStream(Stream):
    """Batch lookup: `{id_query_param}` populated (chunked) from a list of ids
    (`source_key` names the field holding them)."""
    source_type = 'config_ids'
    paginated = False


class ConfigIdsSelfDefaultStream(ConfigIdsStream):
    """Like ConfigIdsStream, but falls back to the authenticated user's own id
    if no ids are given."""
    source_type = 'config_ids_self_default'


class ConfigLoopStream(Stream):
    """Loop over a list of ids (`source_key`); each id is BOTH its own
    emitted record (via `{id}` in path) AND a parent id for this stream's
    children."""
    source_type = 'config_loop'
    paginated = False


class ConfigLoopMultiStream(Stream):
    """Loop over a list of ids (`source_key`); each id's response `data` is
    an array of records, all emitted (no parent relationship, e.g. trends).
    Supports `paginated=True` (loops pagination per id too)."""
    source_type = 'config_loop_multi'


class SearchStream(Stream):
    """One or more required query params (`required_params`); if any resolve
    to None the stream is skipped (with a warning, not silently). Supports
    `paginated=True` and, if `replication_key` is set, INCREMENTAL
    bookmarking keyed by the resolved query value itself."""
    source_type = 'search'


# ==========================================================================
# Concrete streams
# ==========================================================================

# ---- Authenticated-user singletons -------------------------------------
class UsersMe(SingletonStream):
    """The authenticated user's own profile."""
    tap_stream_id = 'users_me'
    path = '/2/users/me'
    key_properties = ['id']
    auth = 'user'
    params = {'user.fields': USER_FIELDS}


class Account(SingletonStream):
    """The authenticated user's X Developer Platform account info."""
    tap_stream_id = 'account'
    path = '/2/account'
    key_properties = ['account_id']
    auth = 'user'


class UsageTweets(SingletonStream):
    """Post-consumption usage for the current project. Requires a genuine
    App-only Bearer Token to actually succeed - a minted token is rejected."""
    tap_stream_id = 'usage_tweets'
    path = '/2/usage/tweets'
    key_properties = ['project_id']
    auth = 'app'
    params = {'usage.fields': USAGE_FIELDS}


class UsageCredits(SingletonStream):
    """Pay-as-you-go credit balance for the current project."""
    tap_stream_id = 'usage_credits'
    path = '/2/usage/credits'
    key_properties = []
    auth = 'user'


class PersonalizedTrends(SingletonListStream):
    """Trending topics personalized for the authenticated user."""
    tap_stream_id = 'personalized_trends'
    path = '/2/users/personalized_trends'
    key_properties = ['trend_name']
    auth = 'user'
    params = {'personalized_trend.fields': PERSONALIZED_TREND_FIELDS}


class UserRepostsOfMe(SingletonListStream):
    """Posts of the authenticated user that have been reposted by others."""
    tap_stream_id = 'user_reposts_of_me'
    path = '/2/users/reposts_of_me'
    key_properties = ['id']
    auth = 'user'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class Bots(SingletonListStream):
    """Bot accounts associated with this app. Requires a genuine App-only
    Bearer Token to actually succeed - a minted token is rejected."""
    tap_stream_id = 'bots'
    path = '/2/bots'
    key_properties = ['id']
    auth = 'app'
    params = {'user.fields': USER_FIELDS}


class Webhooks(SingletonListStream):
    """Webhooks registered for this app."""
    tap_stream_id = 'webhooks'
    path = '/2/webhooks'
    key_properties = ['id']
    auth = 'user'
    params = {'webhook_config.fields': WEBHOOK_FIELDS}


# ---- Children of users_me (parent id = authenticated user's id) --------
class UserTweets(ParentStream):
    """Posts authored by the authenticated user. INCREMENTAL on `created_at`."""
    tap_stream_id = 'user_tweets'
    path = '/2/users/{id}/tweets'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserMentions(ParentStream):
    """Posts mentioning the authenticated user. INCREMENTAL on `created_at`."""
    tap_stream_id = 'user_mentions'
    path = '/2/users/{id}/mentions'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserLikedTweets(ParentStream):
    """Posts liked by the authenticated user."""
    tap_stream_id = 'user_liked_tweets'
    path = '/2/users/{id}/liked_tweets'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserBookmarks(ParentStream):
    """Posts bookmarked by the authenticated user."""
    tap_stream_id = 'user_bookmarks'
    path = '/2/users/{id}/bookmarks'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserFollowers(ParentStream):
    """Users following the authenticated user."""
    tap_stream_id = 'user_followers'
    path = '/2/users/{id}/followers'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserFollowing(ParentStream):
    """Users the authenticated user follows."""
    tap_stream_id = 'user_following'
    path = '/2/users/{id}/following'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserBlocking(ParentStream):
    """Users blocked by the authenticated user."""
    tap_stream_id = 'user_blocking'
    path = '/2/users/{id}/blocking'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserMuting(ParentStream):
    """Users muted by the authenticated user."""
    tap_stream_id = 'user_muting'
    path = '/2/users/{id}/muting'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserOwnedLists(ParentStream):
    """Lists owned by the authenticated user."""
    tap_stream_id = 'user_owned_lists'
    path = '/2/users/{id}/owned_lists'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'list.fields': LIST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserPinnedLists(ParentStream):
    """Lists pinned by the authenticated user."""
    tap_stream_id = 'user_pinned_lists'
    path = '/2/users/{id}/pinned_lists'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    paginated = False
    params = {'list.fields': LIST_FIELDS}


class UserListMemberships(ParentStream):
    """Lists the authenticated user is a member of."""
    tap_stream_id = 'user_list_memberships'
    path = '/2/users/{id}/list_memberships'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'list.fields': LIST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserFollowedLists(ParentStream):
    """Lists the authenticated user follows."""
    tap_stream_id = 'user_followed_lists'
    path = '/2/users/{id}/followed_lists'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'list.fields': LIST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class DmEvents(ParentStream):
    """Direct Message events visible to the authenticated user."""
    tap_stream_id = 'dm_events'
    path = '/2/dm_events'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'dm_event.fields': DM_EVENT_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserAffiliates(ParentStream):
    """Accounts affiliated with the authenticated user (e.g. represented brands)."""
    tap_stream_id = 'user_affiliates'
    path = '/2/users/{id}/affiliates'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class UserBookmarkFolders(ParentStream):
    """Bookmark folders owned by the authenticated user."""
    tap_stream_id = 'user_bookmark_folders'
    path = '/2/users/{id}/bookmarks/folders'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    params = {'max_results': DEFAULT_PAGE_SIZE}


class UserHomeTimeline(ParentStream):
    """The authenticated user's reverse-chronological home timeline.
    INCREMENTAL on `created_at`."""
    tap_stream_id = 'user_home_timeline'
    path = '/2/users/{id}/timelines/reverse_chronological'
    key_properties = ['id']
    source_key = 'users_me'
    auth = 'user'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


# ---- Batch lookups (no parent needed) ------------------------------------
class TweetsByIds(ConfigIdsStream):
    """Posts looked up by id, defaulting to the authenticated user's own
    posts (from `user_tweets`)."""
    tap_stream_id = 'tweets_by_ids'
    path = '/2/tweets'
    key_properties = ['id']
    source_key = 'tweet_ids'
    id_query_param = 'ids'
    auth = 'user'
    params = {'post.fields': POST_FIELDS}


class SpacesByIds(ConfigIdsStream):
    """Spaces looked up by id, defaulting to the authenticated user's own
    spaces (from `spaces_by_creator_ids`)."""
    tap_stream_id = 'spaces_by_ids'
    path = '/2/spaces'
    key_properties = ['id']
    source_key = 'space_ids'
    id_query_param = 'ids'
    auth = 'user'
    params = {'space.fields': SPACE_FIELDS}


class SpacesByCreatorIds(ConfigIdsSelfDefaultStream):
    """Spaces created by the given users, defaulting to just the
    authenticated user."""
    tap_stream_id = 'spaces_by_creator_ids'
    path = '/2/spaces/by/creator_ids'
    key_properties = ['id']
    source_key = 'creator_ids'
    id_query_param = 'user_ids'
    auth = 'user'
    params = {'space.fields': SPACE_FIELDS}


class TrendsByWoeid(ConfigLoopMultiStream):
    """Trending topics for one or more WOEIDs (Where On Earth IDs), defaulting
    to worldwide."""
    tap_stream_id = 'trends_by_woeid'
    path = '/2/trends/by/woeid/{id}'
    key_properties = ['trend_name']
    source_key = 'woeids'
    auth = 'user'
    paginated = False
    params = {'trend.fields': TREND_FIELDS}


class ComplianceJobs(SingletonListStream):
    """Batch compliance jobs. Requires a genuine App-only Bearer Token to
    actually succeed - a minted token is rejected."""
    tap_stream_id = 'compliance_jobs'
    path = '/2/compliance/jobs'
    key_properties = ['id']
    auth = 'app'
    params = {'compliance_job.fields': COMPLIANCE_JOB_FIELDS}
    required_params = {'type': ('compliance_job_type', 'tweets')}  # (field, default)


# ---- Lists (defaults to the authenticated user's own lists) -------------
class ListById(ConfigLoopStream):
    """A List's own metadata, looked up by id; also the parent for
    list_tweets/list_members/list_followers. Defaults to the authenticated
    user's own owned lists (from `user_owned_lists`)."""
    tap_stream_id = 'list_by_id'
    path = '/2/lists/{id}'
    key_properties = ['id']
    source_key = 'list_ids'
    auth = 'user'
    params = {'list.fields': LIST_FIELDS}


class ListTweets(ParentStream):
    """Posts in a List's timeline."""
    tap_stream_id = 'list_tweets'
    path = '/2/lists/{id}/tweets'
    key_properties = ['id']
    source_key = 'list_by_id'
    auth = 'user'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class ListMembers(ParentStream):
    """Members of a List."""
    tap_stream_id = 'list_members'
    path = '/2/lists/{id}/members'
    key_properties = ['id']
    source_key = 'list_by_id'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class ListFollowers(ParentStream):
    """Followers of a List."""
    tap_stream_id = 'list_followers'
    path = '/2/lists/{id}/followers'
    key_properties = ['id']
    source_key = 'list_by_id'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


# ---- Spaces (defaults to the authenticated user's own spaces) -----------
class SpaceById(ConfigLoopStream):
    """A Space's own metadata, looked up by id; also the parent for
    space_tweets/space_buyers. Defaults to the authenticated user's own
    spaces (from `spaces_by_creator_ids`)."""
    tap_stream_id = 'space_by_id'
    path = '/2/spaces/{id}'
    key_properties = ['id']
    source_key = 'space_ids'
    auth = 'user'
    params = {'space.fields': SPACE_FIELDS}


class SpaceTweets(ParentStream):
    """Posts shared in a Space."""
    tap_stream_id = 'space_tweets'
    path = '/2/spaces/{id}/tweets'
    key_properties = ['id']
    source_key = 'space_by_id'
    auth = 'user'
    paginated = False
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class SpaceBuyers(ParentStream):
    """Ticket buyers for a ticketed Space."""
    tap_stream_id = 'space_buyers'
    path = '/2/spaces/{id}/buyers'
    key_properties = ['id']
    source_key = 'space_by_id'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


# ---- Per-post engagement lookups (default to the authenticated user's own
# posts via user_tweets) ---------------------------------------------------
class PostLikingUsers(ConfigLoopMultiStream):
    """Users who liked a Post, defaulting to the authenticated user's own
    posts (from `user_tweets`)."""
    tap_stream_id = 'post_liking_users'
    path = '/2/tweets/{id}/liking_users'
    key_properties = ['id']
    source_key = 'tweet_ids'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class PostQuoteTweets(ConfigLoopMultiStream):
    """Quote Posts of a Post, defaulting to the authenticated user's own
    posts (from `user_tweets`)."""
    tap_stream_id = 'post_quote_tweets'
    path = '/2/tweets/{id}/quote_tweets'
    key_properties = ['id']
    source_key = 'tweet_ids'
    auth = 'user'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class PostRepostedBy(ConfigLoopMultiStream):
    """Users who reposted a Post, defaulting to the authenticated user's own
    posts (from `user_tweets`)."""
    tap_stream_id = 'post_reposted_by'
    path = '/2/tweets/{id}/retweeted_by'
    key_properties = ['id']
    source_key = 'tweet_ids'
    auth = 'user'
    params = {'user.fields': USER_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


class PostReposts(ConfigLoopMultiStream):
    """Reposts of a Post, defaulting to the authenticated user's own posts
    (from `user_tweets`)."""
    tap_stream_id = 'post_reposts'
    path = '/2/tweets/{id}/retweets'
    key_properties = ['id']
    source_key = 'tweet_ids'
    auth = 'user'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}


# ---- Search / query-driven streams -----------------------------------
class PostSearchRecent(SearchStream):
    """Search Posts from the last 7 days, defaulting to the authenticated
    user's own posts. INCREMENTAL on `created_at`, bookmarked by the
    resolved query text."""
    tap_stream_id = 'post_search_recent'
    path = '/2/tweets/search/recent'
    key_properties = ['id']
    auth = 'user'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}
    required_params = {'query': ('post_search_query', None)}


class PostSearchAll(SearchStream):
    """Full-archive Post search (needs an elevated/Academic-Research-tier
    access level), defaulting to the authenticated user's own posts.
    INCREMENTAL on `created_at`, bookmarked by the resolved query text."""
    tap_stream_id = 'post_search_all'
    path = '/2/tweets/search/all'
    key_properties = ['id']
    auth = 'user'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at'
    params = {'post.fields': POST_FIELDS, 'max_results': DEFAULT_PAGE_SIZE}
    required_params = {'query': ('post_search_query', None)}


class PostCountsRecent(SearchStream):
    """Post volume (last 7 days) matching a query, defaulting to the
    authenticated user's own posts."""
    tap_stream_id = 'post_counts_recent'
    path = '/2/tweets/counts/recent'
    key_properties = ['start']
    auth = 'user'
    params = {'granularity': 'hour'}
    required_params = {'query': ('post_search_query', None)}


class PostCountsAll(SearchStream):
    """Full-archive Post volume matching a query, defaulting to the
    authenticated user's own posts. Requires a genuine App-only Bearer Token
    to actually succeed - a minted token is rejected."""
    tap_stream_id = 'post_counts_all'
    path = '/2/tweets/counts/all'
    key_properties = ['start']
    auth = 'app'
    params = {'granularity': 'day'}
    required_params = {'query': ('post_search_query', None)}


class CommunityNotesSearchWritten(SearchStream):
    """Community Notes written by (or eligible for) the authenticated
    account."""
    tap_stream_id = 'community_notes_search_written'
    path = '/2/notes/search/notes_written'
    key_properties = ['id']
    auth = 'user'
    params = {'note.fields': COMMUNITY_NOTE_FIELDS}
    required_params = {'test_mode': ('community_notes_test_mode', False)}


class CommunityNotesEligiblePosts(SearchStream):
    """Posts eligible for a Community Note from the authenticated account."""
    tap_stream_id = 'community_notes_eligible_posts'
    path = '/2/notes/search/posts_eligible_for_notes'
    key_properties = ['id']
    auth = 'user'
    params = {'post.fields': POST_FIELDS}
    required_params = {'test_mode': ('community_notes_test_mode', False)}


STREAMS = {tap_stream_id: cls() for tap_stream_id, cls in Stream._registry.items()}
