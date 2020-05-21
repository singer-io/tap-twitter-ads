# pylint: disable=too-many-lines
from datetime import datetime, timedelta
import time
from urllib.parse import urlparse
import pytz
import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc

from twitter_ads import API_VERSION
from twitter_ads.cursor import Cursor
from twitter_ads.http import Request
from twitter_ads.error import Error
from twitter_ads.utils import split_list

from tap_twitter_ads.transform import transform_record, transform_report
from tap_twitter_ads.streams import flatten_streams


LOGGER = singer.get_logger()

ADS_API_URL = 'https://ads-api.twitter.com'

# Reference: https://developer.twitter.com/en/docs/ads/campaign-management/overview/placements#placements
PLACEMENTS = [
    'ALL_ON_TWITTER', # All possible placement types on Twitter
    'PUBLISHER_NETWORK' # On the Twitter Audience Platform
]


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    LOGGER.info('Stream: {} - Writing schema'.format(stream_name))
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.error('Stream: {} - OS Error writing schema'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(
            stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error('Stream: {} - OS Error writing record'.format(stream_name))
        LOGGER.error('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, default):
    # default only populated on initial sync
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Stream: {} - Write state, bookmark value: {}'.format(stream, value))
    singer.write_state(state)


# Converts cursor object to dictionary
def obj_to_dict(obj):
    if not hasattr(obj, "__dict__"):
        return obj
    result = {}
    for key, val in obj.__dict__.items():
        if key.startswith("_"):
            continue
        element = []
        if isinstance(val, list):
            for item in val:
                element.append(obj_to_dict(item))
        else:
            element = obj_to_dict(val)
        result[key] = element
    return result


# pylint: disable=line-too-long
# API SDK Requests: https://github.com/twitterdev/twitter-python-ads-sdk/blob/master/examples/manual_request.py
# pylint: enable=line-too-long
def get_resource(stream_name, client, path, params=None):
    resource = '/{}/{}'.format(API_VERSION, path)
    try:
        request = Request(client, 'get', resource, params=params) #, stream=True)
    except Error as err:
        # see twitter_ads.error for more details
        LOGGER.error('Stream: {} - ERROR: {}'.format(stream_name, err.details))
        raise err
    cursor = Cursor(None, request)
    return cursor


def post_resource(report_name, client, path, params=None, body=None):
    resource = '/{}/{}'.format(API_VERSION, path)
    try:
        response = Request(client, 'post', resource, params=params, body=body).perform()
    except Error as err:
        # see twitter_ads.error for more details
        LOGGER.error('Report: {} - ERROR: {}'.format(report_name, err.details))
        raise err
    response_body = response.body # Dictionary response of POST request
    return response_body


def get_async_data(report_name, client, url):
    resource = urlparse(url)
    domain = '{0}://{1}'.format(resource.scheme, resource.netloc)
    try:
        response = Request(
            client, 'get', resource.path, domain=domain, raw_body=True, stream=True).perform()
        response_body = response.body
    except Error as err:
        # see twitter_ads.error for more details
        LOGGER.error('Report: {} - ERROR: {}'.format(report_name, err.details))
        raise err
    return response_body


# List selected fields from stream catalog
def get_selected_fields(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    mdata = metadata.to_map(stream.metadata)
    mdata_list = singer.metadata.to_list(mdata)
    selected_fields = []
    for entry in mdata_list:
        field = None
        try:
            field = entry['breadcrumb'][1]
            if entry.get('metadata', {}).get('selected', False):
                selected_fields.append(field)
        except IndexError:
            pass
    return selected_fields


def remove_minutes_local(dttm, tzone):
    new_dttm = dttm.astimezone(tzone).replace(
        minute=0, second=0, microsecond=0)
    return new_dttm


def remove_hours_local(dttm, timezone):
    new_dttm = dttm.astimezone(timezone).replace(
        hour=0, minute=0, second=0, microsecond=0)
    return new_dttm


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)
    LOGGER.info('Stream: {} - Currently Syncing'.format(stream_name))


# from sync.py
def sync_endpoint(client,
                  catalog,
                  state,
                  start_date,
                  stream_name,
                  endpoint_config,
                  tap_config,
                  account_id=None,
                  parent_ids=None,
                  child_streams=None):

    # endpoint_config variables
    path = endpoint_config.get('path')
    LOGGER.info('Stream: {} - endpoint_config: {}'.format(stream_name, endpoint_config))
    id_fields = endpoint_config.get('key_properties', [])
    parent_id_field = next(iter(id_fields), None) # first ID field
    params = endpoint_config.get('params', {})
    bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
    datetime_format = endpoint_config.get('datetime_format')
    sub_types = endpoint_config.get('sub_types', ['none'])
    children = endpoint_config.get('children')

    if parent_ids is None:
        parent_ids = []
    if child_streams is None:
        child_streams = []

    # tap config variabless
    # Twitter Ads does not accept True/False as boolean, must be true/false
    with_deleted = tap_config.get('with_deleted', 'true')
    country_codes = tap_config.get('country_codes', '').replace(' ', '')
    country_code_list = country_codes.split(',')
    LOGGER.info('country_code_list = {}'.format(country_code_list)) # COMMENT OUT
    if sub_types == ['{country_code_list}']:
        sub_types = country_code_list
    LOGGER.info('sub_types = {}'.format(sub_types)) # COMMENT OUT

    # Bookmark datetimes
    last_datetime = get_bookmark(state, stream_name, start_date)
    last_dttm = strptime_to_utc(last_datetime)

    # NOTE: Risk of syncing indefinitely and never getting bookmark
    max_bookmark_value = None

    total_records = 0
    # Loop through sub_types (for tweets endpoint), all other endpoints loop once
    for sub_type in sub_types:
        LOGGER.info('sub_type = {}'.format(sub_type)) # COMMENT OUT

        # Reset params and path for each sub_type
        params = {}
        new_params = {}
        path = None
        params = endpoint_config.get('params', {})
        path = endpoint_config.get('path')

        # Replace keys/ids in path and params
        add_account_id = False # Initial default
        if '{account_id}' in path:
            add_account_id = True
            path = path.replace('{account_id}', account_id)

        if parent_ids:
            parent_id_list = ','.join(map(str, parent_ids))
            path = path.replace('{parent_ids}', parent_id_list)
        key = None
        val = None
        for key, val in list(params.items()):
            new_val = val
            if isinstance(val, str):
                if key == 'with_deleted':
                    new_val = val.replace('{with_deleted}', with_deleted)
                if '{account_ids}' in val:
                    new_val = val.replace('{account_ids}', account_id)
                if '{parent_ids}' in val:
                    new_val = val.replace('{parent_ids}', parent_id_list)
                if '{start_date}' in val:
                    new_val = val.replace('{start_date}', start_date)
                if '{country_codes}' in val:
                    new_val = val.replace('{country_codes}', country_codes)
                if '{sub_type}' in val:
                    new_val = val.replace('{sub_type}', sub_type)
            new_params[key] = new_val
        LOGGER.info('Stream: {} - Request URL: {}/{}/{}'.format(
            stream_name, ADS_API_URL, API_VERSION, path))
        LOGGER.info('Stream: {} - Request params: {}'.format(stream_name, new_params))

        # API Call
        cursor = get_resource(stream_name, client, path, new_params)

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()

        # Get stream metadata from catalog (for masking and validation)
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)

        i = 0
        with metrics.record_counter(stream_name) as counter:
            # Loop thru cursor records, break out if no more data or bookmark_value < last_dttm
            for record in cursor:
                # Get dictionary for record
                record_dict = obj_to_dict(record)
                if not record_dict:
                    # Finish looping
                    LOGGER.info('Stream: {} - Finished Looping, no more data'.format(stream_name))
                    break

                # Get record's bookmark_value
                # All bookmarked requests are sorted by updated_at descending
                #   'sort_by': ['updated_at-desc']
                # The first record is the max_bookmark_value
                if bookmark_field:
                    bookmark_value_str = record_dict.get(bookmark_field)
                    if bookmark_value_str:
                        # Tweets use a different datetime format: '%a %b %d %H:%M:%S %z %Y'
                        if datetime_format:
                            bookmark_value = datetime.strptime(
                                record_dict.get(bookmark_field), datetime_format)
                        # Other bookmarked endpoints use normal UTC format
                        else:
                            bookmark_value = strptime_to_utc(record_dict.get(bookmark_field))
                        # If first record, set max_bookmark_value
                        if i == 0:
                            max_bookmark_dttm = bookmark_value
                            max_bookmark_value = max_bookmark_dttm.strftime('%Y-%m-%dT%H:%M:%S%z')
                            LOGGER.info('Stream: {} - max_bookmark_value: {}'.format(
                                stream_name, max_bookmark_value))
                    else:
                        # pylint: disable=line-too-long
                        LOGGER.info('Stream: {} - NO BOOKMARK, bookmark_field: {}, record: {}'.format(
                            stream_name, bookmark_field, record_dict))
                        # pylint: enable=line-too-long
                        bookmark_value = last_dttm
                    if bookmark_value < last_dttm:
                        # Finish looping
                        LOGGER.info('Stream: {} - Finished, bookmark value < last datetime'.format(
                            stream_name))
                        break
                else:
                    bookmark_value = last_dttm

                # Check for PK fields
                for key in id_fields:
                    if not record_dict.get(key):
                        LOGGER.info('Stream: {} - Missing key {} in record: {}'.format(
                            stream_name, key, record))

                # Transform record from transform.py
                prepared_record = transform_record(stream_name, record_dict)

                # Add account_id to record
                if add_account_id:
                    prepared_record['account_id'] = account_id

                # Transform record with Singer Transformer
                with Transformer() as transformer:
                    transformed_record = transformer.transform(
                        prepared_record,
                        schema,
                        stream_metadata)

                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

                # Append parent_id to parent_ids
                parent_id = record_dict.get(parent_id_field)
                parent_ids.append(parent_id)

                # Increment counters
                i = i + 1
                total_records = total_records + 1

                # End: for record in cursor
            # End: with metrics as counter

        # Loop through children and chunks of parent_ids
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                if child_stream_name in child_streams:
                    update_currently_syncing(state, child_stream_name)
                    # pylint: disable=line-too-long
                    LOGGER.info('Child Stream: {} - START Syncing, parent_stream: {}, account_id: {}'.format(
                        child_stream_name, stream_name, account_id))
                    # pylint: enable=line-too-long
                    # Write schema and log selected fields for stream
                    write_schema(catalog, child_stream_name)
                    selected_fields = get_selected_fields(catalog, child_stream_name)
                    LOGGER.info('Child Stream: {} - selected_fields: {}'.format(
                        child_stream_name, selected_fields))

                    total_child_records = 0
                    child_total_records = 0
                    # parent_id_limit: max list size for parent_ids
                    parent_id_limit = child_endpoint_config.get('parent_ids_limit', 1)
                    chunk = 0 # chunk number
                    # Make chunks of parent_ids
                    for chunk_ids in split_list(parent_ids, parent_id_limit):
                        # pylint: disable=line-too-long
                        LOGGER.info('Child Stream: {} - Syncing, chunk#: {}, parent_stream: {}, parent chunk_ids: {}'.format(
                            child_stream_name, chunk, stream_name, chunk_ids))
                        # pylint: enable=line-too-long

                        child_total_records = sync_endpoint(
                            client=client,
                            catalog=catalog,
                            state=state,
                            start_date=start_date,
                            stream_name=child_stream_name,
                            endpoint_config=child_endpoint_config,
                            tap_config=tap_config,
                            account_id=account_id,
                            parent_ids=chunk_ids,
                            child_streams=child_streams)

                        # pylint: disable=line-too-long
                        LOGGER.info('Child Stream: {} - Finished chunk#: {}, parent_stream: {}'.format(
                            child_stream_name, chunk, stream_name))
                        # pylint: enable=line-too-long
                        total_child_records = total_child_records + child_total_records
                        chunk = chunk + 1
                        # End: for chunk in parent_id_chunks

                    # pylint: disable=line-too-long
                    LOGGER.info('Child Stream: {} - FINISHED Syncing, parent_stream: {}, account_id: {}'.format(
                        child_stream_name, stream_name, account_id))
                    # pylint: enable=line-too-long
                    LOGGER.info('Child Stream: {} - total_records: {}'.format(
                        child_stream_name, total_child_records))
                    update_currently_syncing(state, stream_name)
                    # End: if child_stream_name in child_streams
                # End: for child_stream_name in children.items()
            # End: if children

        # pylint: disable=line-too-long
        LOGGER.info('Stream: {}, Account ID: {} - FINISHED Sub Type: {}, Total Sub Type Records: {}'.format(
            stream_name, account_id, sub_type, i))
        # pylint: enable=line-too-long
        # End: for sub_type in sub_types

    # Update the state with the max_bookmark_value for the stream
    if bookmark_field:
        write_bookmark(state, stream_name, max_bookmark_value)

    return total_records
    # End sync_endpoint


# GET Metric Groups allowed for each Entity, w/ Segment constraints
# Metrics & Segmentation: https://developer.twitter.com/en/docs/ads/analytics/overview/metrics-and-segmentation
# Google Sheet summary: https://docs.google.com/spreadsheets/d/1Cn3B1TPZOjg9QhnnF44Myrs3W8hNOSyFRH6qn8SCc7E/edit?usp=sharing
def get_entity_metric_groups(report_entity, report_segment):
    # Entity type: Set metric_groups, instantiate object
    all_metric_groups = [
        'ENGAGEMENT',
        'BILLING',
        'VIDEO',
        'MEDIA',
        'WEB_CONVERSION',
        'MOBILE_CONVERSION',
        'LIFE_TIME_VALUE_MOBILE_CONVERSION'
    ]
    # Undocumented rule: CONVERSION_TAGS report segment only allows WEB_CONVERSION metric group
    if report_segment == 'CONVERSION_TAGS' and report_entity in \
        ['ACCOUNT', 'CAMPAIGN', 'LINE_ITEM', 'PROMOTED_TWEET']:
        metric_groups = ['WEB_CONVERSION']

    elif report_entity == 'ACCOUNT':
        metric_groups = ['ENGAGEMENT']

    elif report_entity == 'FUNDING_INSTRUMENT':
        metric_groups = ['ENGAGEMENT', 'BILLING']

    elif report_entity == 'CAMPAIGN':
        metric_groups = all_metric_groups

    elif report_entity == 'LINE_ITEM':
        metric_groups = all_metric_groups

    elif report_entity == 'PROMOTED_TWEET':
        metric_groups = all_metric_groups

    elif report_entity == 'PROMOTED_ACCOUNT':
        metric_groups = all_metric_groups

    elif report_entity == 'MEDIA_CREATIVE':
        metric_groups = all_metric_groups

    elif report_entity == 'ORGANIC_TWEET':
        metric_groups = ['ENGAGEMENT', 'VIDEO']
    
    return metric_groups


# Round start and end times based on granularity and timezone
def round_times(report_granularity, timezone, start=None, end=None):
    start_rounded = None
    end_rounded = None
    # Round min_start, max_end to hours or dates
    if report_granularity == 'HOUR': # Round min_start/end to hour
        if start:
            start_rounded = remove_minutes_local(start, timezone) - timedelta(hours=1)
        if end:
            end_rounded = remove_minutes_local(end, timezone) + timedelta(hours=1)
    else: # DAY, TOTAL, Round min_start, max_end to date
        if start:
            start_rounded = remove_hours_local(start, timezone) - timedelta(days=1)
        if end:
            end_rounded = remove_hours_local(end, timezone) + timedelta(days=1)
    return start_rounded, end_rounded


# Determine absolute start and end times w/ attribution_window constraint
# abs_start/end and window_start/end must be rounded to nearest hour or day (granularity)
def get_absolute_start_end_time(report_granularity, timezone, last_dttm, attribution_window):
    now_dttm = utils.now().astimezone(timezone)
    delta_days = (now_dttm - last_dttm).days
    if delta_days < attribution_window:
        start = now_dttm - timedelta(days=attribution_window)
    else:
        start = last_dttm
    abs_start, abs_end = round_times(report_granularity, timezone, start, now_dttm)
    return abs_start, abs_end


# Organic Tweets may not use Active Entities endpoint
# GET Published Organic Tweet Entity IDs within date window
def get_tweet_entity_ids(client, account_id, window_start, window_end):
    entity_ids = []
    flat_streams = flatten_streams()
    tweet_config = flat_streams.get('tweets')
    tweet_path = tweet_config.get('path').replace('{account_id}', account_id)
    datetime_format = tweet_config.get('datetime_format')

    # Set params for PUBLISHED ORGANIC_TWEETs
    tweet_params = tweet_config.get('params')
    tweet_params['tweet_type'] = 'PUBLISHED'
    tweet_params['timeline_type'] = 'ORGANIC'
    tweet_params['with_deleted'] = 'false'
    tweet_params['trim_user'] = 'true'

    tweet_cursor = get_resource('tweets', client, tweet_path, tweet_params)
    # Loop thru organic tweets to get entity_ids (if in date range)
    for tweet in tweet_cursor:
        entity_id = tweet['id']
        created_at = tweet['created_at']
        created_dttm = datetime.strptime(created_at, datetime_format)
        if created_dttm <= window_end and created_dttm >= window_start:
            entity_ids.append(entity_id)
        elif created_dttm < window_start:
            break

    return entity_ids


# GET Active Entity IDs w/in date window (rounded for granularity) for an entity type
def get_active_entity_sets(active_entities, report_name, account_id, report_entity, \
    report_granularity, timezone, window_start, window_end):

    entity_id_sets = []
    entity_id_set = {}
    # Abstract out the Get Entity IDs and start/end date
    # PLACEMENT LOOP
    # Get entity_ids for each placement type
    for placement in PLACEMENTS: # ALL_ON_TWITTER, PUBLISHER_NETWORK
        # LOGGER.info('placement = {}'.format(placement)) # COMMENT OUT
        entity_ids = []
        min_start = None
        max_end = None
        min_start_rounded = window_start
        max_end_rounded = window_end
        ent = 0
        for active_entity in active_entities:
            active_entity_dict = obj_to_dict(active_entity)
            LOGGER.info('active_entity_dict = {}'.format(active_entity_dict)) # COMMENT OUT
            entity_id = active_entity_dict.get('entity_id')
            entity_placements = active_entity_dict.get('placements', [])
            entity_start = strptime_to_utc(active_entity_dict.get(
                'activity_start_time')).astimezone(timezone)
            entity_end = strptime_to_utc(active_entity_dict.get(
                'activity_end_time')).astimezone(timezone)

            # If active_entity in placement, append; and determine min/max dates
            if placement in entity_placements:
                entity_ids.append(entity_id)
                if ent == 0:
                    min_start = entity_start
                    max_end = entity_end
                if entity_start < min_start:
                    min_start = entity_start
                if entity_end > max_end:
                    max_end = entity_end
                ent = ent + 1
                # End: if placement in entity_placements
            # End: for active_entity in active_entities

        # Round min_start, max_end to hours or dates
        min_start_rounded, max_end_rounded = round_times(
            report_granularity, timezone, min_start, max_end)

        if entity_ids != []:
            entity_id_set = {
                'placement': placement,
                'entity_ids': entity_ids,
                'start_time': min_start_rounded.strftime('%Y-%m-%dT%H:%M:%S%z'),
                'end_time': max_end_rounded.strftime('%Y-%m-%dT%H:%M:%S%z')
            }
            LOGGER.info('entity_id_set = {}'.format(entity_id_set)) # COMMENT OUT
            entity_id_sets.append(entity_id_set)

        # End: for placement in PLACEMENTS

    return entity_id_sets


# POST QUEUED ASYNC JOB
# pylint: disable=line-too-long
# ASYNC Analytics Reference: https://developer.twitter.com/en/docs/ads/analytics/guides/asynchronous-analytics
# ASYNC POST Reference: https://developer.twitter.com/en/docs/ads/analytics/api-reference/asynchronous#post-stats-jobs-accounts-account-id
# Report Parameters:
#  entity_type: required, 1 and only 1 per request
#  entity_ids: required, 1 to 20 per request
#  metric_groups: required, 1 or more per request (valid combinations based on entity_type)
#  segments: optional, 0 or 1 allowed, based on entity_type
#    NO segmentation allowed for: MEDIA_CREATIVE and ORGANIC_TWEETS
#    country: country targeting value, required for segment = LOCATIONS, POSTAL_CODES, REGIONS, METROS?
#    platform: platform targeting value, required for segment = DEVICES, PLATFORM_VERSIONS
#  placements: required, 1 and only 1 per request; but loop thru 2: ALL_ON_TWITTER, PUBLISHER_NETWORK
#  granularity: required; HOUR, DAY, or TOTAL; 1 and only 1 per request
#  start_date - end_date: required, have to be rounded to the hour
#      limited to 45 day windows (for SEGMENT queries), 90 days (for non-SEGMENTED)
# pylint: enable=line-too-long
def post_queued_async_jobs(client, account_id, report_name, report_entity, entity_ids, report_granularity, \
    report_segment, metric_groups, placement, start_time, end_time, country_id, platform_id):
    queued_job_ids = []
    # CHUNK ENTITY_IDS LOOP
    chunk = 0 # chunk number
    # Make chunks of 20 of entity_ids
    for chunk_ids in split_list(entity_ids, 20):
        # POST async_queued_job for report entity chunk_ids
        # Reference: https://developer.twitter.com/en/docs/ads/analytics/api-reference/asynchronous#post-stats-jobs-accounts-account-id
        LOGGER.info('Report: {} - POST ASYNC queued_job, chunk#: {}'.format(
            report_name, chunk))
        queued_job_path = 'stats/jobs/accounts/{account_id}'.replace(
            '{account_id}', account_id)
        queued_job_params = {
            # Required params
            'entity': report_entity,
            'entity_ids': ','.join(map(str, chunk_ids)),
            'metric_groups': ','.join(map(str, metric_groups)),
            'placement': placement,
            'granularity': report_granularity,
            'start_time': start_time,
            'end_time': end_time,
            # Optional params
            'segmentation_type': report_segment,
            'country': country_id,
            'platform': platform_id
        }
        LOGGER.info('Report: {} - queued_job POST URL: {}/{}/{}'.format(
            report_name, ADS_API_URL, API_VERSION, queued_job_path))
        LOGGER.info('Report: {} - queued_job params: {}'.format(
            report_name, queued_job_params))

        # POST queued_job: asynchronous job
        queued_job = post_resource('queued_job', client, queued_job_path, \
            queued_job_params)

        queued_job_data = queued_job.get('data')
        queued_job_id = queued_job_data.get('id_str')
        queued_job_ids.append(queued_job_id)
        LOGGER.info('queued_job_ids = {}'.format(queued_job_ids)) # COMMENT OUT
        # End: for chunk_ids in entity_ids
    return queued_job_ids


def get_async_results_urls(client, account_id, report_name, queued_job_ids):
    # WHILE JOBS STILL RUNNING LOOP, GET ASYNC JOB STATUS
    # GET ASYNC Status Reference: https://developer.twitter.com/en/docs/ads/analytics/api-reference/asynchronous#get-stats-jobs-accounts-account-id 
    jobs_still_running = True # initialize
    j = 1 # job status check counter
    async_results_urls = []
    while len(queued_job_ids) > 0 and jobs_still_running and j <= 20:
        # Wait 15 sec for async reports to finish
        wait_sec = 15
        LOGGER.info('Report: {} - Waiting {} sec for async job to finish'.format(
            report_name, wait_sec))
        time.sleep(wait_sec)

        # GET async_job_status
        LOGGER.info('Report: {} - GET async_job_statuses'.format(report_name))
        async_job_statuses_path = 'stats/jobs/accounts/{account_id}'.replace(
            '{account_id}', account_id)
        async_job_statuses_params = {
            # What is the concurrent job_id limit?
            'job_ids': ','.join(map(str, queued_job_ids)),
            'count': 1000,
            'cursor': None
        }
        LOGGER.info('Report: {} - async_job_statuses GET URL: {}/{}/{}'.format(
            report_name, ADS_API_URL, API_VERSION, async_job_statuses_path))
        LOGGER.info('Report: {} - async_job_statuses params: {}'.format(
            report_name, async_job_statuses_params))
        async_job_statuses = get_resource('async_job_statuses', client, async_job_statuses_path, \
            async_job_statuses_params)

        jobs_still_running = False
        for async_job_status in async_job_statuses:
            job_status_dict = obj_to_dict(async_job_status)
            job_id = job_status_dict.get('id_str')
            job_status = job_status_dict.get('status')
            if job_status == 'PROCESSING':
                jobs_still_running = True
            elif job_status == 'SUCCESS':
                LOGGER.info('Report: {} - job_id: {}, finished running (SUCCESS)'.format(
                    report_name, job_id))
                job_results_url = job_status_dict.get('url')
                async_results_urls.append(job_results_url)
                # LOGGER.info('job_results_url = {}'.format(job_results_url)) # COMMENT OUT
                # Remove job_id from queued_job_ids
                queued_job_ids.remove(job_id)
            # End: async_job_status in async_job_statuses
        j = j + 1 # increment job status check counter
        # End: async_job_status in async_job_statuses
    return async_results_urls


def sync_report(client,
                catalog,
                state,
                start_date,
                report_name,
                report_config,
                tap_config,
                account_id=None,
                country_ids=None,
                platform_ids=None):

    # PROCESS:
    # Outer-outer loop (in sync): loop through accounts
    # Outer loop (in sync): loop through reports selected in catalog
    #   Each report definition: name, entity, segment, granularity
    #
    # For each Report:
    # 1. Determine start/end dates and date windows (rounded, limited, timezone);
    #     Loop through date windows from bookmark datetime to current datetime.
    # 2. Based on Entity Type, Get active entity ids for date window and placement.
    # 3. POST ASYNC Job to Queue to get queued_job_id with the following Loops:
    #     A. For each Sub Type Loop (Country or Platform)
    #     B. For each Placement w/ Entity ID Set Loop
    #     C. For each Chunk of 20 Entity IDs
    # 4. GET ASYNC Job Statuses and Download URLs (when complete)
    # 5. Download Data from URLs and Sync data to target

    # report parameters
    report_entity = report_config.get('entity')
    report_segment = report_config.get('segment', 'NO_SEGMENT')
    report_granularity = report_config.get('granularity', 'DAY')

    LOGGER.info('Report: {}, Entity: {}, Segment: {}, Granularity: {}'.format(
        report_name, report_entity, report_segment, report_granularity))

    # Set report_segment NO_SEGMENT to None
    if report_segment == 'NO_SEGMENT':
        report_segment = None
    # MEDIA_CREATIVE and ORGANIC_TWEET don't allow Segmentation
    if report_entity in ['MEDIA_CREATIVE', 'ORGANIC_TWEET']:
        report_segment = None

    # Initialize account and get account timezone
    account = client.accounts(account_id)
    tzone = account.timezone
    timezone = pytz.timezone(tzone)
    LOGGER.info('Account ID: {} - timezone: {}'.format(account_id, tzone))

    # Bookmark datetimes
    last_datetime = get_bookmark(state, report_name, start_date)
    last_dttm = strptime_to_utc(last_datetime).astimezone(timezone)
    max_bookmark_value = last_datetime

    # Get absolute start and end times
    attribution_window = int(tap_config.get('attribution_window', '14'))
    abs_start, abs_end = get_absolute_start_end_time(
        report_granularity, timezone, last_dttm, attribution_window)

    # Initialize date window
    if report_segment:
        # Max is 45 days (segmented), set lower to avoid date/hour rounding issues
        date_window_size = 42 # is the Answer
    else:
        # Max is 90 days, set lower to avoid date/hour rounding issues
        date_window_size = 85
    window_start = abs_start
    window_end = (abs_start + timedelta(days=date_window_size))
    window_start_rounded = None
    window_end_rounded = None
    if window_end > abs_end:
        window_end = abs_end

    entity_id_sets = []
    entity_ids = []
    # DATE WINDOW LOOP
    while window_start != abs_end:
        window_entity_id_sets = []
        window_start_rounded, window_end_rounded = round_times(
            report_granularity, timezone, window_start, window_end)
        window_start_str = window_start_rounded.strftime('%Y-%m-%dT%H:%M:%S%z')
        window_end_str = window_end_rounded.strftime('%Y-%m-%dT%H:%M:%S%z')

        LOGGER.info('Report: {} - Date window: {} to {}'.format(
            report_name, window_start_str, window_end_str))

        # ACCOUNT cannot use active_entities endpoint; but single Account ID
        if report_entity == 'ACCOUNT':
            entity_ids.append(account_id)
            window_entity_id_sets = [
                {
                    'placement': 'ALL_ON_TWITTER',
                    'entity_ids': entity_ids,
                    'start_time': window_start_str,
                    'end_time': window_end_str
                },
                {
                    'placement': 'PUBLISHER_NETWORK',
                    'entity_ids': entity_ids,
                    'start_time': window_start_str,
                    'end_time': window_end_str
                }
            ]

        # ORGANIC_TWEET cannot use active_entities endpoint
        elif report_entity == 'ORGANIC_TWEET':
            LOGGER.info('Report: {} - GET ORGANINC_TWEET entity_ids'.format(report_name))
            entity_ids = get_tweet_entity_ids(client, account_id, window_start, window_end)
            entity_id_set = { # PUBLISHER_NETWORK is invalid placement for ORGANIC_TWEET
                'placement': 'ALL_ON_TWITTER',
                'entity_ids': entity_ids,
                'start_time': window_start_str,
                'end_time': window_end_str
            }
            window_entity_id_sets.append(entity_id_set)

        # ALL OTHER entity types use active_entities endpoint
        else:
            # Reference: https://developer.twitter.com/en/docs/ads/analytics/api-reference/active-entities
            # GET active_entities for entity
            LOGGER.info('Report: {} - GET {} active_entities entity_ids'.format(
                report_name, report_entity))
            active_entities_path = 'stats/accounts/{account_id}/active_entities'.replace(
                '{account_id}', account_id)
            active_entities_params = {
                'entity': report_entity,
                'start_time': window_start_str,
                'end_time': window_end_str
            }
            LOGGER.info('Report: {} - active_entities GET URL: {}/{}/{}'.format(
                report_name, ADS_API_URL, API_VERSION, active_entities_path))
            LOGGER.info('Report: {} - active_entities params: {}'.format(
                report_name, active_entities_params))
            active_entities = get_resource('active_entities', client, active_entities_path, \
                active_entities_params)

            # Get active entity_ids, start, end for each placement type for date window
            window_entity_id_sets = []
            window_entity_id_sets = get_active_entity_sets(active_entities,
                                                           report_name,
                                                           account_id,
                                                           report_entity,
                                                           report_granularity,
                                                           timezone,
                                                           window_start,
                                                           window_end)
            # End: else (active_entities)

        # Append window_entity_id_sets to entity_id_sets
        entity_id_sets = entity_id_sets + window_entity_id_sets

        # Increment date window
        window_start = window_end
        window_end = window_start + timedelta(days=date_window_size)
        if window_end > abs_end:
            window_end = abs_end
        # End: date window

    LOGGER.info('entity_id_sets = {}'.format(entity_id_sets)) # COMMENT OUT

    # ASYNC report POST requests
    # Get metric_groups for report_entity and report_egment
    metric_groups = get_entity_metric_groups(report_entity, report_segment)

    # Set sub_type and sub_type_ids for sub_type loop
    if report_segment in ('LOCATIONS', 'METROS', 'POSTAL_CODES', 'REGIONS'):
        sub_type = 'countries'
        sub_type_ids = country_ids
    elif report_segment in ('DEVICES', 'PLATFORM_VERSIONS'):
        sub_type = 'platforms'
        sub_type_ids = platform_ids
    else: # NO sub_type (loop once thru sub_type loop)
        sub_type = 'none'
        sub_type_ids = ['none']

    # POST ALL Queued ASYNC Jobs for Report
    queued_job_ids = []
    # SUB_TYPE LOOP
    # Countries or Platforms loop (or single loop for sub_types = ['none'])
    for sub_type_id in sub_type_ids:
        sub_type_queued_job_ids = []
        if sub_type == 'platforms':
            country_id = None
            platform_id = sub_type_id
        elif sub_type == 'countries':
            country_id = sub_type_id
            platform_id = None
        else:
            country_id = None
            platform_id = None

        # ENTITY ID SET LOOP
        for entity_id_set in entity_id_sets:
            entity_id_set_queued_job_ids = []
            LOGGER.info('entity_id_set = {}'.format(entity_id_set)) # COMMENT OUT
            placement = entity_id_set.get('placement')
            entity_ids = entity_id_set.get('entity_ids', [])
            start_time = entity_id_set.get('start_time')
            end_time = entity_id_set.get('end_time')
            LOGGER.info('Report: {} - placement: {}, start_time: {}, end_time: {}'.format(
                report_name, placement, start_time, end_time))

            # POST ASYNC JOBS for ENTITY ID SET (possibly many chunks)
            entity_id_set_queued_job_ids = post_queued_async_jobs(client,
                                                                  account_id,
                                                                  report_name,
                                                                  report_entity,
                                                                  entity_ids,
                                                                  report_granularity,
                                                                  report_segment,
                                                                  metric_groups,
                                                                  placement,
                                                                  start_time,
                                                                  end_time,
                                                                  country_id,
                                                                  platform_id)
            sub_type_queued_job_ids = sub_type_queued_job_ids + entity_id_set_queued_job_ids
            # End: for entity_id_set in entity_id_sets
        
        queued_job_ids = queued_job_ids + sub_type_queued_job_ids
        # End: for sub_type_id in sub_type_ids

    # WHILE JOBS STILL RUNNING LOOP, GET ASYNC JOB STATUS
    # GET ASYNC Status Reference: https://developer.twitter.com/en/docs/ads/analytics/api-reference/asynchronous#get-stats-jobs-accounts-account-id 
    async_results_urls = []
    async_results_urls = get_async_results_urls(client, account_id, report_name, queued_job_ids)
    LOGGER.info('async_results_urls = {}'.format(async_results_urls)) # COMMENT OUT

    # Get stream_metadata from catalog (for Transformer masking and validation below)
    stream = catalog.get_stream(report_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)
    
    # ASYNC RESULTS DOWNLOAD / PROCESS LOOP
    # RISK: What if some reports error or don't finish?
    # Possibly move this code block withing ASYNC Status Check
    total_records = 0
    for async_results_url in async_results_urls:
        
        # GET DOWNLOAD DATA FROM URL
        LOGGER.info('Report: {} - GET async data from URL: {}'.format(
            report_name, async_results_url))
        async_data = get_async_data(report_name, client, async_results_url)
        # LOGGER.info('async_data = {}'.format(async_data)) # COMMENT OUT

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()

        # TRANSFORM REPORT DATA
        transformed_data = []
        transformed_data = transform_report(report_name, async_data, account_id)
        # LOGGER.info('transformed_data = {}'.format(transformed_data)) # COMMENT OUT
        if transformed_data is None or transformed_data == []:
            LOGGER.info('Report: {} - NO TRANSFORMED DATA for URL: {}'.format(
                report_name, async_results_url))

        # PROCESS RESULTS TO TARGET RECORDS
        with metrics.record_counter(report_name) as counter:
            for record in transformed_data:
                # Transform record with Singer Transformer

                # Evalueate max_bookmark_value
                end_time = record.get('end_time') # String
                end_dttm = strptime_to_utc(end_time) # Datetime
                max_bookmark_dttm = strptime_to_utc(max_bookmark_value) # Datetime
                if end_dttm > max_bookmark_dttm: # Datetime comparison
                    max_bookmark_value = end_time # String

                with Transformer() as transformer:
                    transformed_record = transformer.transform(
                        record,
                        schema,
                        stream_metadata)

                    write_record(report_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        # Increment total_records
        total_records = total_records + counter.value
        # End: for async_results_url in async_results_urls

    # Update the state with the max_bookmark_value for the stream
    write_bookmark(state, report_name, max_bookmark_value)

    return total_records
    # End sync_report


# Sync - main function to loop through select streams to sync_endpoints and sync_reports
def sync(client, config, catalog, state):
    # Get config parameters
    account_list = config.get('account_ids').replace(' ', '').split(',')
    country_code_list = config.get('country_codes', 'US').replace(' ', '').split(',')
    start_date = config.get('start_date')
    reports = config.get('reports', [])

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('Last/Currently Syncing Stream: {}'.format(last_stream))

    # Get ALL selected streams from catalog
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('Sync Selected Streams: {}'.format(selected_streams))
    if not selected_streams:
        return

    # Get lists of parent and child streams to sync (from streams.py and catalog)
    # For children, ensure that dependent parent_stream is included
    parent_streams = []
    child_streams = []
    # Get all streams (parent + child) from streams.py
    flat_streams = flatten_streams()
    # Loop thru all streams
    for stream_name, stream_metadata in flat_streams.items():
        # If stream has a parent_stream, then it is a child stream
        parent_stream = stream_metadata.get('parent_stream')
        # Append selected parent streams
        if not parent_stream and stream_name in selected_streams:
            parent_streams.append(stream_name)
        # Append selected child streams
        elif parent_stream and stream_name in selected_streams:
            child_streams.append(stream_name)
            # Append un-selected parent streams of selected children
            if parent_stream not in selected_streams:
                parent_streams.append(parent_stream)
    LOGGER.info('Sync Parent Streams: {}'.format(parent_streams))
    LOGGER.info('Sync Child Streams: {}'.format(child_streams))

    # Get list of report streams to sync (from config and catalog)
    report_streams = []
    for report in reports:
        report_name = report.get('name')
        if report_name in selected_streams:
            report_streams.append(report_name)
    LOGGER.info('Sync Report Streams: {}'.format(report_streams))

    # ACCOUNT_ID OUTER LOOP
    for account_id in account_list:
        LOGGER.info('Account ID: {} - START Syncing'.format(account_id))

        # PARENT STREAM LOOP
        for stream_name in parent_streams:
            update_currently_syncing(state, stream_name)
            endpoint_config = flat_streams.get(stream_name)

            LOGGER.info('Stream: {} - START Syncing, Account ID: {}'.format(
                stream_name, account_id))

            # Write schema and log selected fields for stream
            write_schema(catalog, stream_name)

            selected_fields = get_selected_fields(catalog, stream_name)
            LOGGER.info('Stream: {} - selected_fields: {}'.format(stream_name, selected_fields))

            total_records = sync_endpoint(client=client,
                                          catalog=catalog,
                                          state=state,
                                          start_date=start_date,
                                          stream_name=stream_name,
                                          endpoint_config=endpoint_config,
                                          tap_config=config,
                                          account_id=account_id,
                                          child_streams=child_streams)

            LOGGER.info('Stream: {} - FINISHED Syncing, Account ID: {}, Total Records: {}'.format(
                stream_name, account_id, total_records))

            update_currently_syncing(state, None)

        # GET country_ids and platform_ids (targeting values) - only if reports exist
        if report_streams != []:
            # GET country_ids (targeting_values) based on config country_codes
            country_ids = []
            country_path = 'targeting_criteria/locations'
            for country_code in country_code_list:
                country_params = {
                    'count': 1000,
                    'cursor': None,
                    'location_type': 'COUNTRIES',
                    'country_code': country_code
                }
                country_cursor = get_resource('countries', client, country_path, country_params)
                for country in country_cursor:
                    country_id = country['targeting_value']
                    country_ids.append(country_id)
            LOGGER.info('Countries - Country Codes: {}, Country Targeting IDs: {}'.format(
                country_code_list, country_ids))

            # GET platform_ids (targeting_values)
            platform_ids = []
            platforms_path = 'targeting_criteria/platforms'
            platforms_params = {
                'count': 1000,
                'cursor': None
            }
            platforms_cursor = get_resource('platforms', client, platforms_path, platforms_params)
            for platform in platforms_cursor:
                platform_id = platform['targeting_value']
                platform_ids.append(platform_id)
            LOGGER.info('Platforms - Platform Targeting IDs: {}'.format(platform_ids))

        # REPORT STREAMS LOOP
        for report in reports:
            report_name = report.get('name')
            if report_name in report_streams:
                update_currently_syncing(state, report_name)

                LOGGER.info('Report: {} - START Syncing for Account ID: {}'.format(
                    report_name, account_id))

                # Write schema and log selected fields for stream
                write_schema(catalog, report_name)

                selected_fields = get_selected_fields(catalog, report_name)
                LOGGER.info('Report: {} - selected_fields: {}'.format(
                    report_name, selected_fields))

                total_records = sync_report(client=client,
                                            catalog=catalog,
                                            state=state,
                                            start_date=start_date,
                                            report_name=report_name,
                                            report_config=report,
                                            tap_config=config,
                                            account_id=account_id,
                                            country_ids=country_ids,
                                            platform_ids=platform_ids)

                # pylint: disable=line-too-long
                LOGGER.info('Report: {} - FINISHED Syncing for Account ID: {}, Total Records: {}'.format(
                    report_name, account_id, total_records))
                # pylint: enable=line-too-long
                update_currently_syncing(state, None)

        LOGGER.info('Account ID: {} - FINISHED Syncing'.format(account_id))
