"""
Sync orchestration for the OAuth 2.0 X API v2 streams (streams.py).

Each `Stream.source_type` (see streams.py) is synced by a dedicated function
below:
  - 'singleton'          -> sync_singleton_stream        one record, no id
  - 'singleton_list'     -> sync_singleton_list_stream    one call, `data` is an array
  - 'parent'             -> sync_child_stream             `{id}` from a parent record
  - 'config_ids'/
    'config_ids_self_default' -> sync_config_ids_stream   batch lookup by config id list
  - 'config_loop'        -> sync_config_loop_stream        loop config ids; each is its
                             own record AND a parent id for its children (via
                             sync_config_loop_parent_group)
  - 'config_loop_multi'  -> sync_config_loop_multi_stream  loop config ids; each id's
                             `data` is an array, no parent relationship
  - 'search'             -> sync_search_stream             required config query
                             param(s); INCREMENTAL bookmark keyed by the query itself

See streams.py's module docstring for the full config-field reference (which
optional fields activate which streams).

One failing stream never discards data already written by other streams -
each stream is synced in its own try/except and a single summarizing
exception (if any) is raised only after every selected stream was attempted.
"""
import singer
from singer import metadata, Transformer, utils
from singer.utils import strptime_to_utc

from tap_twitter_ads.streams import STREAMS, ID_CHUNK_SIZE
from tap_twitter_ads.exceptions import XApiClientError, XApiBackoffError

LOGGER = singer.get_logger()
BOOKMARK_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
MIN_PAGE_SIZE = 5
MAX_PAGE_SIZE = 100
MAX_PAGES_SAFETY_CAP = 1000  # guards against runaway pagination

# Errors raised by the client once retries (if any) are exhausted - caught
# per-stream so one stream's failure (e.g. a 402 quota/credits error, which
# is not retryable) does not discard data already synced by others.
STREAM_FAILURE_EXCEPTIONS = (XApiClientError, XApiBackoffError)


def get_selected_streams(catalog, state):
    """Return the tap_stream_id of every stream marked selected in the catalog."""
    return [stream.stream for stream in catalog.get_selected_streams(state)]


def get_page_size(config):
    """Resolve the `page_size` config value (clamped to [MIN_PAGE_SIZE,
    MAX_PAGE_SIZE]), falling back to MAX_PAGE_SIZE if unset or invalid."""
    raw = config.get('page_size')
    if not raw:
        return MAX_PAGE_SIZE
    try:
        size = int(raw)
    except (TypeError, ValueError):
        return MAX_PAGE_SIZE
    return max(MIN_PAGE_SIZE, min(MAX_PAGE_SIZE, size))


def get_bookmark(state, stream_name, parent_id, default):
    """Read the bookmark value for `stream_name` scoped to `parent_id` (a real
    parent record id, a config_loop id, or a resolved search-query key),
    falling back to `default` (typically `start_date`) if none exists yet."""
    return state.get('bookmarks', {}).get(stream_name, {}).get(parent_id, default)


def write_bookmark(state, stream_name, parent_id, value):
    """Persist a new bookmark value for `stream_name`/`parent_id` and flush
    state immediately (so an interrupted sync can resume from here)."""
    state.setdefault('bookmarks', {}).setdefault(stream_name, {})[parent_id] = value
    singer.write_state(state)


def write_schema(catalog, stream_name):
    """Emit the SCHEMA message for `stream_name` from the catalog's resolved schema."""
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    singer.write_schema(stream_name, schema, stream.key_properties)


def config_id_list(config, key):
    """Parse a config id-list field (`config[key]`) into a list of strings.
    Accepts either a JSON array or a comma-separated string; returns `[]` if
    the key is absent/empty (the caller then skips the stream with a
    warning - see e.g. `sync_config_ids_stream`)."""
    raw = config.get(key)
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(v) for v in raw]
    return [v.strip() for v in str(raw).split(',') if v.strip()]


def chunked(items, size):
    """Yield successive `size`-sized slices of `items` (for id-list batching)."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


# --------------------------------------------------------------------- #
# Generic single-record fetch/write (singleton streams, and one iteration
# of a `parent`/`config_loop` stream that isn't paginated)
# --------------------------------------------------------------------- #
def fetch_record(client, config, stream, path=None, extra_params=None):
    """GET `path` (or `stream.path`) and return the single `data` object -
    used by 'singleton' streams and by one iteration of a 'config_loop' stream."""
    params = resolve_params(config, stream, extra_params)
    response = client.get(path or stream.path, params=params, auth=stream.auth)
    return response.get('data')


def resolve_params(config, stream, extra_params=None):
    """Merge `stream.params` with `extra_params`, then fill in `stream.required_params`
    from config (each value is a `(config_key, default)` tuple - see
    `required_params_satisfied` for how an unset-with-no-default param is detected)."""
    params = dict(stream.params)
    if extra_params:
        params.update(extra_params)
    for param_name, (config_key, default) in stream.required_params.items():
        params[param_name] = config.get(config_key, default)
    return params


def required_params_satisfied(config, stream):
    """False if any required param has no configured value and no default
    (i.e. resolves to None) - used to skip a stream cleanly instead of
    sending a request that X API will reject with a 400."""
    for config_key, default in stream.required_params.values():
        if config.get(config_key, default) is None:
            return False
    return True


def write_one_record(catalog, stream_name, record, transformer):
    """Transform (per catalog schema/metadata) and emit a single RECORD message."""
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)
    time_extracted = utils.now()
    transformed = transformer.transform(record, schema, stream_metadata)
    singer.write_record(stream_name, transformed, time_extracted=time_extracted)


def sync_singleton_stream(client, config, catalog, stream_name, transformer):
    """Sync a 'singleton' stream: one call, no id needed, `data` is a single
    object (or absent, in which case zero records are written)."""
    stream = STREAMS[stream_name]
    write_schema(catalog, stream_name)
    record = fetch_record(client, config, stream)
    total = 0
    with singer.metrics.record_counter(stream_name) as counter:
        if record:
            write_one_record(catalog, stream_name, record, transformer)
            counter.increment()
            total = 1
    LOGGER.info('Stream: %s - FINISHED Syncing, Total Records: %s', stream_name, total)
    return record


def sync_singleton_list_stream(client, config, catalog, stream_name, transformer):
    """One call, no id needed, `data` is an array of records (compliance_jobs,
    personalized_trends)."""
    stream = STREAMS[stream_name]
    write_schema(catalog, stream_name)
    if not required_params_satisfied(config, stream):
        LOGGER.warning('Stream: %s - SKIPPED: required parameter(s) not configured (%s)',
                       stream_name, list(stream.required_params.values()))
        return 0
    params = resolve_params(config, stream)
    response = client.get(stream.path, params=params, auth=stream.auth)
    total = 0
    with singer.metrics.record_counter(stream_name) as counter:
        for record in response.get('data', []):
            write_one_record(catalog, stream_name, record, transformer)
            counter.increment()
            total += 1
    LOGGER.info('Stream: %s - FINISHED Syncing, Total Records: %s', stream_name, total)
    return total


def sync_config_loop_multi_stream(client, config, catalog, stream_name, transformer):
    """Loop over a configured id list; each id's response `data` is an array of
    records, all emitted (trends_by_woeid, post_liking_users, etc. - no parent
    relationship). Paginates per id when `stream.paginated` is set."""
    stream = STREAMS[stream_name]
    write_schema(catalog, stream_name)

    ids = config_id_list(config, stream.source_key)
    if not ids:
        LOGGER.warning('Stream: %s - SKIPPED: no `%s` configured', stream_name, stream.source_key)
        return 0

    total = 0
    page_size = get_page_size(config)
    params = resolve_params(config, stream)
    with singer.metrics.record_counter(stream_name) as counter:
        for item_id in ids:
            path = stream.path.replace('{id}', str(item_id))
            if stream.paginated:
                pages = client.get_paginated(path, dict(params), auth=stream.auth, page_size=page_size,
                                              max_pages=MAX_PAGES_SAFETY_CAP)
            else:
                pages = [client.get(path, params=params, auth=stream.auth)]
            for page in pages:
                for record in page.get('data', []):
                    write_one_record(catalog, stream_name, record, transformer)
                    counter.increment()
                    total += 1
    LOGGER.info('Stream: %s - FINISHED Syncing, Total Records: %s', stream_name, total)
    return total


def sync_config_ids_stream(client, config, catalog, stream_name, transformer):
    """Sync a 'config_ids'/'config_ids_self_default' stream: batch-lookup a
    config-supplied id list (`stream.source_key`), chunked to `stream.chunk_size`,
    via the query param `stream.id_query_param`. Skipped (with a warning) if
    the id list is empty and there's no self-default id to fall back to."""
    stream = STREAMS[stream_name]
    write_schema(catalog, stream_name)

    ids = config_id_list(config, stream.source_key)
    if not ids and stream.source_type == 'config_ids_self_default':
        # falls back to the authenticated user's own id - resolved by the caller
        # and passed in via config['_resolved_creator_ids'] (see sync()).
        ids = config_id_list(config, '_resolved_' + stream.source_key)
    if not ids:
        LOGGER.warning('Stream: %s - SKIPPED: no `%s` configured', stream_name, stream.source_key)
        return 0

    total = 0
    with singer.metrics.record_counter(stream_name) as counter:
        for chunk in chunked(ids, stream.chunk_size):
            params = dict(stream.params)
            params[stream.id_query_param] = ','.join(chunk)
            response = client.get(stream.path, params=params, auth=stream.auth)
            time_extracted = utils.now()
            for record in response.get('data', []):
                write_one_record(catalog, stream_name, record, transformer)
                counter.increment()
                total += 1
    LOGGER.info('Stream: %s - FINISHED Syncing, Total Records: %s', stream_name, total)
    return total


def sync_config_loop_stream(client, config, catalog, stream_name, transformer):
    """Loop over a configured id list; each id is fetched as its own record
    AND returned so the caller can use it as a parent id for child streams."""
    stream = STREAMS[stream_name]
    write_schema(catalog, stream_name)

    ids = config_id_list(config, stream.source_key)
    if not ids:
        LOGGER.warning('Stream: %s - SKIPPED: no `%s` configured', stream_name, stream.source_key)
        return []

    resolved = []
    total = 0
    with singer.metrics.record_counter(stream_name) as counter:
        for item_id in ids:
            path = stream.path.replace('{id}', str(item_id))
            record = fetch_record(client, config, stream, path=path)
            if record:
                write_one_record(catalog, stream_name, record, transformer)
                counter.increment()
                total += 1
                resolved.append((str(item_id), record))
            else:
                LOGGER.warning('Stream: %s - no data returned for id=%s', stream_name, item_id)
    LOGGER.info('Stream: %s - FINISHED Syncing, Total Records: %s', stream_name, total)
    return resolved


def sync_child_stream(client, config, catalog, state, stream_name, parent_id, transformer):
    """Sync a 'parent' stream: `{id}` in `stream.path` is replaced with
    `parent_id`. Handles pagination (if `stream.paginated`) and, for
    INCREMENTAL streams, bookmark filtering/advancement keyed by `parent_id`
    (mirrors `sync_search_stream`'s bookmark logic, but keyed by an actual
    parent record id instead of a resolved search-query value)."""
    stream = STREAMS[stream_name]
    write_schema(catalog, stream_name)
    schema_stream = catalog.get_stream(stream_name)
    stream_metadata = metadata.to_map(schema_stream.metadata)
    schema = schema_stream.schema.to_dict()

    path = stream.path.replace('{id}', str(parent_id))
    page_size = get_page_size(config)
    params = dict(stream.params)
    if 'max_results' in params:
        params['max_results'] = page_size

    is_incremental = stream.replication_method == 'INCREMENTAL'
    start_date = config.get('start_date')
    has_prior_bookmark = False
    last_bookmark_dttm = None
    max_bookmark_dttm = None

    if is_incremental:
        has_prior_bookmark = parent_id in state.get('bookmarks', {}).get(stream_name, {})
        last_bookmark = get_bookmark(state, stream_name, parent_id, start_date)
        last_bookmark_dttm = strptime_to_utc(last_bookmark)
        max_bookmark_dttm = last_bookmark_dttm
        params['start_time'] = last_bookmark_dttm.strftime(BOOKMARK_FORMAT)

    total_records = 0
    with singer.metrics.record_counter(stream_name) as counter:
        if stream.paginated:
            pages = client.get_paginated(path, params, auth=stream.auth, page_size=page_size,
                                          max_pages=MAX_PAGES_SAFETY_CAP)
        else:
            pages = [client.get(path, params=params, auth=stream.auth)]

        for page in pages:
            records = page.get('data', [])
            time_extracted = utils.now()

            for record in records:
                if is_incremental:
                    record_bookmark_dttm = strptime_to_utc(record[stream.replication_key])
                    if record_bookmark_dttm > max_bookmark_dttm:
                        max_bookmark_dttm = record_bookmark_dttm
                    # Skip records already synced in a previous run. On an initial
                    # sync (no prior bookmark) start_date is just a lower-bound
                    # filter, not an "already synced" marker, so a record dated
                    # exactly at start_date must NOT be skipped.
                    already_synced = record_bookmark_dttm < last_bookmark_dttm or (
                        has_prior_bookmark and record_bookmark_dttm == last_bookmark_dttm)
                    if already_synced:
                        continue

                transformed = transformer.transform(record, schema, stream_metadata)
                singer.write_record(stream_name, transformed, time_extracted=time_extracted)
                counter.increment()
                total_records += 1

    if is_incremental:
        # Always write the bookmark (even when max ties last_bookmark_dttm) so an
        # initial sync whose only/newest record sits exactly at start_date still
        # persists its bookmark instead of silently never advancing past default.
        write_bookmark(state, stream_name, parent_id, max_bookmark_dttm.strftime(BOOKMARK_FORMAT))

    LOGGER.info('Stream: %s - FINISHED Syncing, parent_id: %s, Total Records: %s',
                stream_name, parent_id, total_records)
    return total_records


def sync_search_stream(client, config, catalog, state, stream_name, transformer):
    """One or more required query params sourced from config (`required_params`).
    Skipped cleanly (not silently) if any required value is unconfigured with
    no default. Supports pagination and, for streams with a `replication_key`,
    INCREMENTAL bookmarking keyed by the resolved query value itself (so
    different `*_search_query` values each track their own bookmark)."""
    stream = STREAMS[stream_name]
    write_schema(catalog, stream_name)

    if not required_params_satisfied(config, stream):
        LOGGER.warning('Stream: %s - SKIPPED: required parameter(s) not configured (%s)',
                       stream_name, list(stream.required_params.values()))
        return 0

    schema_stream = catalog.get_stream(stream_name)
    stream_metadata = metadata.to_map(schema_stream.metadata)
    schema = schema_stream.schema.to_dict()

    page_size = get_page_size(config)
    params = resolve_params(config, stream)
    if 'max_results' in params:
        params['max_results'] = page_size

    # Bookmark key: the resolved query/search value itself, so multiple
    # differently-configured searches each keep an independent bookmark.
    bookmark_key = '|'.join(str(v) for v in params.values() if isinstance(v, (str, bool)))[:200] or 'default'

    is_incremental = stream.replication_method == 'INCREMENTAL'
    start_date = config.get('start_date')
    has_prior_bookmark = False
    last_bookmark_dttm = None
    max_bookmark_dttm = None

    if is_incremental:
        has_prior_bookmark = bookmark_key in state.get('bookmarks', {}).get(stream_name, {})
        last_bookmark = get_bookmark(state, stream_name, bookmark_key, start_date)
        last_bookmark_dttm = strptime_to_utc(last_bookmark)
        max_bookmark_dttm = last_bookmark_dttm
        params['start_time'] = last_bookmark_dttm.strftime(BOOKMARK_FORMAT)

    total_records = 0
    with singer.metrics.record_counter(stream_name) as counter:
        if stream.paginated:
            pages = client.get_paginated(stream.path, params, auth=stream.auth, page_size=page_size,
                                          max_pages=MAX_PAGES_SAFETY_CAP)
        else:
            pages = [client.get(stream.path, params=params, auth=stream.auth)]

        for page in pages:
            records = page.get('data', [])
            time_extracted = utils.now()

            for record in records:
                if is_incremental:
                    record_bookmark_dttm = strptime_to_utc(record[stream.replication_key])
                    if record_bookmark_dttm > max_bookmark_dttm:
                        max_bookmark_dttm = record_bookmark_dttm
                    already_synced = record_bookmark_dttm < last_bookmark_dttm or (
                        has_prior_bookmark and record_bookmark_dttm == last_bookmark_dttm)
                    if already_synced:
                        continue

                transformed = transformer.transform(record, schema, stream_metadata)
                singer.write_record(stream_name, transformed, time_extracted=time_extracted)
                counter.increment()
                total_records += 1

    if is_incremental:
        write_bookmark(state, stream_name, bookmark_key, max_bookmark_dttm.strftime(BOOKMARK_FORMAT))

    LOGGER.info('Stream: %s - FINISHED Syncing, Total Records: %s', stream_name, total_records)
    return total_records


def _children_of(parent_name, selected_streams):
    """Return the selected streams whose `.parent` is `parent_name` (only
    true `source_type='parent'` streams ever have a non-None `.parent` -
    see `Stream.parent` in streams.py)."""
    return [name for name in selected_streams if STREAMS[name].parent == parent_name]


def sync_config_loop_parent_group(client, config, catalog, state, parent_name,
                                   selected_streams, stream_errors, transformer):
    """Sync a `config_loop` stream (e.g. `list_by_id`, `space_by_id`) together
    with its children - each configured id becomes a parent id for the
    children, in addition to being its own emitted record."""
    children = _children_of(parent_name, selected_streams)
    needs_parent = parent_name in selected_streams or bool(children)
    if not needs_parent:
        return

    singer.set_currently_syncing(state, parent_name)
    resolved = []
    try:
        resolved = sync_config_loop_stream(client, config, catalog, parent_name, transformer)
    except STREAM_FAILURE_EXCEPTIONS as err:
        LOGGER.error('Stream: %s - FAILED: %s', parent_name, err)
        stream_errors[parent_name] = str(err)
    singer.set_currently_syncing(state, None)
    singer.write_state(state)

    if children and not resolved:
        for name in children:
            LOGGER.warning('Stream: %s - SKIPPED: no `%s` configured '
                            '(or parent stream %s returned no data)',
                            name, STREAMS[parent_name].source_key, parent_name)

    for item_id, _ in resolved:
        for name in children:
            singer.set_currently_syncing(state, name)
            try:
                sync_child_stream(client, config, catalog, state, name, item_id, transformer)
            except STREAM_FAILURE_EXCEPTIONS as err:
                LOGGER.error('Stream: %s - FAILED (%s=%s): %s', name, parent_name, item_id, err)
                stream_errors.setdefault(name, str(err))
            singer.set_currently_syncing(state, None)
            singer.write_state(state)


def sync(client, config, catalog, state):
    """Sync every selected stream, dispatching each to the function matching
    its `source_type` (see module docstring). `users_me` and the two
    config_loop parents with children (`list_by_id`, `space_by_id`) are
    synced first/specially since their children need a resolved parent id;
    every other stream is independent and order-agnostic. Raises a single
    summarizing exception at the end if any stream(s) failed, after every
    selected stream was attempted - never partway through."""
    selected_streams = get_selected_streams(catalog, state)
    LOGGER.info('Selected Streams: %s', selected_streams)
    if not selected_streams:
        return

    stream_errors = {}

    with Transformer() as transformer:
        # ---- users_me + its children -----------------------------------
        user_children = _children_of('users_me', selected_streams)
        needs_default_creator_id = ('spaces_by_creator_ids' in selected_streams
                                     and not config.get('creator_ids'))
        needs_users_me = 'users_me' in selected_streams or bool(user_children) or needs_default_creator_id
        me_id = None
        if needs_users_me:
            singer.set_currently_syncing(state, 'users_me')
            try:
                me_id = _sync_users_me(client, config, catalog, selected_streams, transformer)
            except STREAM_FAILURE_EXCEPTIONS as err:
                LOGGER.error('Stream: users_me - FAILED: %s', err)
                stream_errors['users_me'] = str(err)
            singer.set_currently_syncing(state, None)
            singer.write_state(state)

        if user_children:
            if me_id is None:
                for name in user_children:
                    stream_errors[name] = 'Skipped - parent stream users_me failed, no id available'
                    LOGGER.error('Stream: %s - SKIPPED: %s', name, stream_errors[name])
            else:
                for name in user_children:
                    singer.set_currently_syncing(state, name)
                    try:
                        sync_child_stream(client, config, catalog, state, name, me_id, transformer)
                    except STREAM_FAILURE_EXCEPTIONS as err:
                        LOGGER.error('Stream: %s - FAILED: %s', name, err)
                        stream_errors[name] = str(err)
                    singer.set_currently_syncing(state, None)
                    singer.write_state(state)

        # ---- config_ids_self_default: spaces_by_creator_ids ------------
        if 'spaces_by_creator_ids' in selected_streams and me_id and not config.get('creator_ids'):
            config = dict(config)
            config['_resolved_creator_ids'] = me_id

        # ---- standalone singleton / config_ids / config_loop streams --
        CONFIG_LOOP_PARENTS_WITH_CHILDREN = ('list_by_id', 'space_by_id')
        for name in selected_streams:
            stream = STREAMS[name]
            if name == 'users_me' or stream.parent:
                continue  # already handled above (or handled as config-loop-parent children below)
            if name in CONFIG_LOOP_PARENTS_WITH_CHILDREN:
                continue  # handled below, together with their children

            singer.set_currently_syncing(state, name)
            try:
                if stream.source_type == 'singleton':
                    sync_singleton_stream(client, config, catalog, name, transformer)
                elif stream.source_type == 'singleton_list':
                    sync_singleton_list_stream(client, config, catalog, name, transformer)
                elif stream.source_type in ('config_ids', 'config_ids_self_default'):
                    sync_config_ids_stream(client, config, catalog, name, transformer)
                elif stream.source_type == 'config_loop':
                    sync_config_loop_stream(client, config, catalog, name, transformer)
                elif stream.source_type == 'config_loop_multi':
                    sync_config_loop_multi_stream(client, config, catalog, name, transformer)
                elif stream.source_type == 'search':
                    sync_search_stream(client, config, catalog, state, name, transformer)
            except STREAM_FAILURE_EXCEPTIONS as err:
                LOGGER.error('Stream: %s - FAILED: %s', name, err)
                stream_errors[name] = str(err)
            singer.set_currently_syncing(state, None)
            singer.write_state(state)

        # ---- config_loop parents with children (list_by_id, space_by_id) --
        for parent_name in CONFIG_LOOP_PARENTS_WITH_CHILDREN:
            sync_config_loop_parent_group(client, config, catalog, state, parent_name,
                                           selected_streams, stream_errors, transformer)

    if stream_errors:
        summary = '; '.join('{}: {}'.format(name, msg) for name, msg in stream_errors.items())
        raise XApiClientError(
            'Sync completed with {} failed stream(s) (other streams were synced successfully): {}'.format(
                len(stream_errors), summary))


def _sync_users_me(client, config, catalog, selected_streams, transformer):
    """Fetch (and, if selected, emit) the `users_me` record. Always fetched
    when any users_me-child stream is selected, even if `users_me` itself is
    not, since its `id` is required as the children's parent id. Raises if
    the API returns no `data` (children cannot proceed without an id)."""
    stream = STREAMS['users_me']
    write_schema(catalog, 'users_me')
    record = fetch_record(client, config, stream)
    if not record:
        raise XApiClientError(
            'GET {} returned no `data` - cannot determine authenticated user id '
            'required by child streams'.format(stream.path))
    if 'users_me' in selected_streams:
        with singer.metrics.record_counter('users_me') as counter:
            write_one_record(catalog, 'users_me', record, transformer)
            counter.increment()
        LOGGER.info('Stream: users_me - FINISHED Syncing, Total Records: 1')
    return record['id']
