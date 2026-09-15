"""
Schema + metadata loader for the OAuth 2.0 X API v2 streams (streams.py).
Every stream owns its own schema JSON file (`schemas/<tap_stream_id>.json`,
via `Stream.schema_file`), so no cross-stream sharing/aliasing is involved.
"""
import json
import os

import singer
from singer import metadata

from tap_twitter_ads.streams import STREAMS

LOGGER = singer.get_logger()


def get_abs_path(path):
    """Resolve `path` relative to this package's directory (so it works
    regardless of the caller's current working directory)."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    """Load every stream's schema JSON and build its standard Singer metadata
    (key_properties, replication method/key, `parent-tap-stream-id` for
    'parent' streams, and automatic-inclusion for the replication key field).
    Returns `(schemas, field_metadata)` dicts keyed by `tap_stream_id`."""
    schemas = {}
    field_metadata = {}

    for stream_name, stream in STREAMS.items():
        schema_path = get_abs_path('schemas/{}.json'.format(stream.schema_file))
        with open(schema_path, encoding='utf-8') as file:
            schema = json.load(file)

        schemas[stream_name] = schema

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream.key_properties,
            valid_replication_keys=[stream.replication_key] if stream.replication_key else None,
            replication_method=stream.replication_method,
        )
        mdata = metadata.to_map(mdata)

        if stream.parent:
            mdata = metadata.write(mdata, (), 'parent-tap-stream-id', stream.parent)

        # Replication key fields must be automatic so they are always emitted,
        # even when not explicitly selected by the user.
        if stream.replication_key and stream.replication_key in schema.get('properties', {}):
            mdata = metadata.write(mdata, ('properties', stream.replication_key), 'inclusion', 'automatic')

        field_metadata[stream_name] = metadata.to_list(mdata)

    return schemas, field_metadata
