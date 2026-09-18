"""
Discovery and catalog generation for OAuth 2.0 X API v2 streams.

Streams are statically defined, but discovery probes each stream and
excludes those returning 402/403.

Parent streams are checked first, inaccessible parents prune their
children, while accessible children are checked independently.

Streams missing required config IDs or query parameters remain in the
catalog unchecked, matching the existing sync behavior.
"""
import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_twitter_ads.exceptions import (
    XApiForbiddenError,
    XApiNotFoundError,
    XApiPaymentRequiredError,
)
from tap_twitter_ads.schema import get_schemas
from tap_twitter_ads.streams import STREAMS
from tap_twitter_ads.sync import config_id_list, required_params_satisfied, resolve_params

LOGGER = singer.get_logger()

# 402/403 exclude only the affected stream. 401 and other errors
# propagate because they are not treated as stream-level access failures.
INACCESSIBLE_EXCEPTIONS = (XApiForbiddenError, XApiPaymentRequiredError)

def _probe(client, stream, path, params, on_success=None):
    """Probe a stream endpoint and return whether it is accessible."""
    try:
        response = client.get(path, params=params, auth=stream.auth)
        if on_success is not None:
            on_success(response)
        return True
    except XApiNotFoundError as exc:
        LOGGER.warning(
            "Stream '%s' not found (404), excluding from catalog. "
            "HTTP-Error-Message: '%s'",
            stream.tap_stream_id,
            str(exc),
        )
        return False
    except INACCESSIBLE_EXCEPTIONS as exc:
        LOGGER.warning(
            "Stream '%s' is not accessible. Error: %s",
            stream.tap_stream_id,
            str(exc)
        )
        return False

def check_access(stream, client, config, parent_id=None):
    """Check whether a stream can be included in discovery."""
    source_type = stream.source_type

    if source_type in ('singleton', 'singleton_list'):
        if not required_params_satisfied(config, stream):
            return True  # not configured - unknown/unverified, not "inaccessible"
        params = resolve_params(config, stream)
        if 'max_results' in params:
            params = dict(params, max_results=1)
        return _probe(client, stream, stream.path, params)

    if source_type == 'parent':
        if parent_id is None:
            return True  # no resolved parent id given - nothing to probe with
        path = stream.path.replace('{id}', str(parent_id))
        params = dict(stream.params)
        if 'max_results' in params:
            params = dict(params, max_results=1)
        return _probe(client, stream, path, params)

    if source_type in ('config_ids', 'config_ids_self_default'):
        ids = (config_id_list(config, stream.source_key)
               or config_id_list(config, '_resolved_' + stream.source_key))
        if not ids:
            return True  # no id(s) configured/resolvable yet
        params = dict(stream.params)
        params[stream.id_query_param] = ids[0]
        return _probe(client, stream, stream.path, params)

    if source_type == 'config_loop':
        ids = (config_id_list(config, stream.source_key)
               or config_id_list(config, '_resolved_' + stream.source_key))
        if not ids:
            return True
        path = stream.path.replace('{id}', str(ids[0]))
        return _probe(client, stream, path, dict(stream.params))

    if source_type == 'config_loop_multi':
        ids = (config_id_list(config, stream.source_key)
               or config_id_list(config, '_resolved_' + stream.source_key))
        if not ids:
            return True
        path = stream.path.replace('{id}', str(ids[0]))
        params = dict(stream.params)
        if 'max_results' in params:
            params = dict(params, max_results=1)
        return _probe(client, stream, path, params)

    if source_type == 'search':
        if not required_params_satisfied(config, stream):
            return True
        params = resolve_params(config, stream)
        if 'max_results' in params:
            params = dict(params, max_results=1)
        return _probe(client, stream, stream.path, params)

    return True

def _prune_inaccessible_children(child_stream_ids, parent_name):
    """Exclude children when their parent is inaccessible."""
    for child in child_stream_ids:
        LOGGER.warning(
            "Stream: '%s' excluded from catalog because its parent"
            "stream '%s' is not accessible.",
            child,
            parent_name
        )
    return set(child_stream_ids)

def _children_by_parent():
    """Build a mapping of parent stream IDs to child stream IDs."""
    children_of = {}
    for name, stream in STREAMS.items():
        if stream.parent:
            children_of.setdefault(stream.parent, []).append(name)
    return children_of

def _resolved_config_for_independent_streams(config, me_id, me_username):
    """Add the self-defaulted values used by sync."""
    resolved = dict(config)
    if me_id and not resolved.get('creator_ids'):
        resolved.setdefault('_resolved_creator_ids', me_id)
    if not resolved.get('woeids'):
        resolved.setdefault('_resolved_woeids', '1')
    if me_username and not resolved.get('post_search_query'):
        resolved.setdefault('_resolved_post_search_query', 'from:{}'.format(me_username))
    return resolved

def _apply_access_checks(client, config):
    """Check stream access and return the accessible stream IDs."""
    children_of = _children_by_parent()

    accessible = set()
    inaccessible = set()

    # ---- users_me: also the source of me_id/me_username used to resolve
    # self-default params for several independent streams below ----------
    me_id = me_username = None
    users_me_accessible = True
    if 'users_me' in STREAMS:
        users_me_stream = STREAMS['users_me']
        captured = {}

        def _capture_me(response):
            data = (response or {}).get('data') or {}
            captured['id'] = data.get('id')
            captured['username'] = data.get('username')

        users_me_accessible = _probe(client, users_me_stream, users_me_stream.path,
                                      dict(users_me_stream.params), on_success=_capture_me)
        if users_me_accessible:
            accessible.add('users_me')
            me_id, me_username = captured.get('id'), captured.get('username')
        else:
            inaccessible.add('users_me')

    users_me_children = children_of.get('users_me', [])
    if not users_me_accessible:
        inaccessible |= _prune_inaccessible_children(users_me_children, 'users_me')
    else:
        for child in users_me_children:
            if check_access(STREAMS[child], client, config, parent_id=me_id):
                accessible.add(child)
            else:
                inaccessible.add(child)

    # ---- config_loop parents with children (list_by_id, space_by_id) -
    # only actively probed when their id(s) are explicitly configured;
    # otherwise left accessible/unverified along with their children,
    # exactly like any other not-yet-configured stream (see check_access) --
    for parent_name in ('list_by_id', 'space_by_id'):
        if parent_name not in STREAMS:
            continue
        parent_stream = STREAMS[parent_name]
        children = children_of.get(parent_name, [])
        ids = config_id_list(config, parent_stream.source_key)

        if not ids:
            accessible.add(parent_name)
            accessible.update(children)
            continue

        if check_access(parent_stream, client, config):
            accessible.add(parent_name)
            resolved_parent_id = ids[0]
            for child in children:
                if check_access(STREAMS[child], client, config, parent_id=resolved_parent_id):
                    accessible.add(child)
                else:
                    inaccessible.add(child)
        else:
            inaccessible.add(parent_name)
            inaccessible |= _prune_inaccessible_children(children, parent_name)

    # ---- every other (independent - no parent/child relationship) stream --
    handled = {'users_me', 'list_by_id', 'space_by_id'}
    handled.update(users_me_children)
    handled.update(children_of.get('list_by_id', []))
    handled.update(children_of.get('space_by_id', []))

    resolved_config = _resolved_config_for_independent_streams(config, me_id, me_username)
    for name, stream in STREAMS.items():
        if name in handled:
            continue
        if check_access(stream, client, resolved_config):
            accessible.add(name)
        else:
            inaccessible.add(name)

    if inaccessible:
        LOGGER.warning(
            "Unauthorised streams excluded from catalog: %s",
            ", ".join(inaccessible)
        )

    if not accessible:
        raise XApiForbiddenError(
            'HTTP-error-code: 403, Error: The credentials do not have access to any supported streams.')

    return accessible

def discover(client, config):
    """Build a Singer `Catalog` with one `CatalogEntry` per stream in
    `streams.STREAMS` that `_apply_access_checks()` determined the
    connection can actually use.
    """

    accessible = _apply_access_checks(client, config or {})

    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        if stream_name not in accessible:
            continue

        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        table_metadata = {}
        for entry in mdata:
            if entry.get('breadcrumb') == ():
                table_metadata = entry.get('metadata', {})
        key_properties = table_metadata.get('table-key-properties')

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=key_properties,
            schema=schema,
            metadata=mdata,
        ))

    return catalog
