#!/usr/bin/env python3

import sys
import json
import argparse
from twitter_ads.client import Client
import singer
from singer import metadata, utils
from tap_twitter_ads.discover import discover
from tap_twitter_ads.sync import sync


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'consumer_key',
    'consumer_secret',
    'access_token',
    'access_token_secret',
    'account_ids'
]

def do_discover(reports):
    LOGGER.info('Starting discover')
    catalog = discover(reports)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    config = parsed_args.config

    # Twitter Ads SDK Reference: https://github.com/twitterdev/twitter-python-ads-sdk
    client = Client(
        consumer_key=config.get('consumer_key'),
        consumer_secret=config.get('consumer_secret'),
        access_token=config.get('access_token'),
        access_token_secret=config.get('access_token_secret'),
        options={
            'handle_rate_limit': True,
            'retry_max': 3,
            'retry_delay': 5000,
            'retry_on_status': [404, 500, 503],
            'retry_on_timeouts': True,
            'timeout': (1.0, 3.0)})

    state = {}
    if parsed_args.state:
        state = parsed_args.state

    catalog = parsed_args.catalog

    reports = config.get('reports', {})

    if parsed_args.discover:
        do_discover(reports)
    elif parsed_args.catalog:
        sync(client=client,
             config=config,
             catalog=catalog,
             state=state)

if __name__ == '__main__':
    main()
