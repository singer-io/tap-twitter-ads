#!/usr/bin/env python3
import sys
import json

import singer

from tap_twitter_ads.client import XApiClient
from tap_twitter_ads.discover import discover
from tap_twitter_ads.sync import sync as _sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'client_id',
    'client_secret',
    'access_token',
    'refresh_token',
]


def do_discover():
    """Discover mode does not make a live API call - streams are statically
    defined in streams.py - so an expired/rotated access token does not
    block discovery (only sync requires a valid token)."""
    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():
    """Tap entrypoint: parses CLI args/config, builds the OAuth2 client, and
    dispatches to discover or sync mode."""
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    config = parsed_args.config
    state = parsed_args.state or {}
    catalog = parsed_args.catalog
    config_path = getattr(parsed_args, 'config_path', None)

    client = XApiClient(config, config_path=config_path)
    client.check_credentials()

    if parsed_args.discover:
        do_discover()
    elif parsed_args.catalog:
        _sync(client=client, config=config, catalog=catalog, state=state)


if __name__ == '__main__':
    main()
