"""
Discovery (catalog generation) for the OAuth 2.0 X API v2 streams. Does not
make any live API call - every stream is statically defined in streams.py -
so discovery works even with an expired/rotated access_token (only sync
needs a valid token).
"""
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_twitter_ads.schema import get_schemas


def discover():
    """Build a Singer `Catalog` with one `CatalogEntry` per stream in
    `streams.STREAMS`, using the schema + metadata from `schema.get_schemas()`."""
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
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
