# tap-twitter-ads

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Twitter Ads API, version 7](https://developer.twitter.com/en/docs/ads/general/overview) using the [Twitter Ads Python SDK](https://github.com/twitterdev/twitter-python-ads-sdk).

[Twitter Ads Hierarchy and Terminology](https://developer.twitter.com/en/docs/tutorials/ads-api-hierarchy-terminology)
![Twitter Ads Hierarchy](https://cdn.cms-twdigitalassets.com/content/dam/developer-twitter/adsapi/adsapi-heirarchy.png.img.fullhd.medium.png)

- Extracts the following normal GET endpoints:
  - [account_media](https://developer.twitter.com/en/docs/ads/creatives/api-reference/account-media#account-media)
  - [accounts](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/accounts#accounts)
  - [advertiser_business_categories](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/advertiser-business-categories#advertiser-business-categories)
  - [bidding_rules](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/bidding-rules#bidding-rules)
  - [campaigns](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/campaigns#campaigns)
  - **cards**:
    - [image_app_download](https://developer.twitter.com/en/docs/ads/creatives/api-reference/image-app-download#image-app-download-cards)
    - [image_conversation](https://developer.twitter.com/en/docs/ads/creatives/api-reference/image-conversation#image-conversation-cards)
    - [image_direct_message](https://developer.twitter.com/en/docs/ads/creatives/api-reference/image-direct-message#image-direct-message-cards)
    - [poll](https://developer.twitter.com/en/docs/ads/creatives/api-reference/poll#poll-cards)
    - [video_app_download](https://developer.twitter.com/en/docs/ads/creatives/api-reference/video-app-download#video-app-download-cards)
    - [video_conversation](https://developer.twitter.com/en/docs/ads/creatives/api-reference/video-conversation#video-conversation-cards)
    - [video_direct_message](https://developer.twitter.com/en/docs/ads/creatives/api-reference/video-direct-message#video-direct-message-cards)
    - [video_website](https://developer.twitter.com/en/docs/ads/creatives/api-reference/video-website#video-website-cards)
    - [website](https://developer.twitter.com/en/docs/ads/creatives/api-reference/website#website-cards)
  - [content_categories](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/content-categories#content-categories)
  - [funding_instruments](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/funding-instruments#funding-instruments)
  - [iab_categories](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/iab-categories#iab-categories)
  - [line_item_apps](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/line-item-apps#line-item-apps)
  - [line_items](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/line-items#line-items) (aka **ad_groups**)
    - [targeting_criteria](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-criteria#targeting-criteria) (for each line_item)
  - [media_creatives](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/media-creatives#media-creatives)
  - [preroll_call_to_actions](https://developer.twitter.com/en/docs/ads/creatives/api-reference/preroll-call-to-actions#preroll-call-to-actions)
  - [promotable_users](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/promotable-users#promotable-users)
  - [promoted_accounts](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/promoted-accounts#promoted-accounts)
  - [promoted_tweets](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/promoted-tweets#promoted-tweets)
  - [scheduled_promoted_tweets](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/scheduled-promoted-tweets#scheduled-promoted-tweets)
  - [tailored_audiences](https://developer.twitter.com/en/docs/ads/audiences/api-reference/tailored-audiences#tailored-audiences)
  - [tweets](https://developer.twitter.com/en/docs/ads/creatives/api-reference/tweets#get-accounts-account-id-scoped-timeline) (scheduled and published, not draft)

- Extracts the following targeting GET endpoints:
  - [targeting_conversations](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-conversations)
  - [targeting_devices](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-devices)
  - [targeting_events](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-events) (for countries)
  - [targeting_interests](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-interests)
  - [targeting_languages](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-languages)
  - [targeting_locations](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-locations) (for countries)
  - [targeting_network_operators](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-network-operators) (for countries)
  - [targeting_platform_versions](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-platform-versions)
  - [targeting_platforms](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-platforms)
  - [targeting_tv_markets](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-tv-markets)
    - [targeting_tv_shows](https://developer.twitter.com/en/docs/ads/campaign-management/api-reference/targeting-options#get-targeting-criteria-tv-shows) (for each)

- Extracts Asyncronous Reports using:
  - Supports many reports, each with Report Config Settings:
    - **Entity**: 7 Entity Types
    - **Segment**: 20 Segmentation Types
    - **Granularity**: DAY, HOUR, or TOTAL
  - [Metrics and Segmentation Rules](https://developer.twitter.com/en/docs/ads/analytics/overview/metrics-and-segmentation)
  - [active_entities](https://developer.twitter.com/en/docs/ads/analytics/api-reference/active-entities)
  - [async_queued_jobs](https://developer.twitter.com/en/docs/ads/analytics/api-reference/asynchronous#post-stats-jobs-accounts-account-id)
  - [async_job_status](https://developer.twitter.com/en/docs/ads/analytics/api-reference/asynchronous#get-stats-jobs-accounts-account-id)
  - async_results (download URL)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Authentication
Twitter Ads requires authentication headers using OAuth 1.0a with an access token obtained via 3-legged OAuth flow. The access token, once generated, is permanent, but request tokens are short-lived without a documented expiry.
The process is described in [Obtaining Ads Account Credential](https://developer.twitter.com/en/docs/ads/general/guides/obtaining-ads-account-access).

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-twitter-ads
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install twitter-ads (v7.0.0)
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)
    - [twitter-ads](https://github.com/twitterdev/twitter-python-ads-sdk)

3. Create your tap's `config.json` with the following parameters:
    - `start_date`: Absolute beginning date for bookmarked endpoints.
    - `user_agent`: Tap name and email address for API logging.
    - OAuth 1.0a credentials:
      - `consumer_key`
      - `consumer_secret`
      - `access_token`
      - `access_token_secret`
    - `account_ids`: Comma-delimited list of Twitter Ad Account IDs.
    - `attribution_window`: Number of days for latency look-back period to allow analytical reporting numbers to stabilize.
    - `with_deleted`: true or false; specifies whether to include logically deleted records in the results.
    - `country_codes`: Comma-delimited list of ISO 2-letter country codes for targeting and segmenttation.
    - `reports`: Object array of specified reports with name, entity, segment, and granularity.

    ```json
    {
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-twitter-ads <api_user_email@your_company.com>",
        "consumer_key": "YOUR_TWITTER_ADS_CONSUMER_KEY",
        "consumer_secret": "YOUR_TWITTER_ADS_CONSUMER_SECRET",
        "access_token": "YOUR_TWITTER_ADS_ACCESS_TOKEN",
        "access_token_secret": "YOUR_TWITTER_ADS_ACCESS_TOKEN_SECRET",
        "account_ids": "id1, id2, id3",
        "attribution_window": "14",
        "with_deleted": "true",
        "country_codes": "US, CA, MX, DE",
        "reports": [
            {
            "name": "campaigns_genders_hourly_report",
            "enitity": "CAMPAIGN",
            "segment": "GENDER",
            "granularity": "HOUR"
            },
            {
            "name": "line_items_regions_daily_report",
            "enitity": "LINE_ITEM",
            "segment": "REGIONS",
            "granularity": "DAY"
            }
        ]
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream.  The next run would begin where the last job left off.
    Each bookmarked endpoint that supports INCREMENTAL syncs will be listed with its max last processed record based on `updated_at`, `created_at`, or `end_time` (depending on the endpoint).

    ```json
    {
        "currently_syncing": "creatives",
        "bookmarks": {
            "accounts": "2019-06-11T13:37:55Z",
            "account_media": "2019-06-19T19:48:42Z",
            "..."
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    > tap-twitter-ads --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-twitter-ads --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-twitter-ads --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-twitter-ads --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the twitter Ads tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_twitter_ads -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.72/10.
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-twitter-ads --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
      The output is valid.
      It contained 872667 messages for 62 streams.

          63 schema messages
      872432 record messages
          172 state messages

      Details by stream:
      +--------------------------------------------+---------+---------+
      | stream                                     | records | schemas |
      +--------------------------------------------+---------+---------+
      | account_media                              | 0       | 1       |
      | accounts                                   | 1       | 1       |
      | accounts_conversion_tags_hourly_report     | 0       | 1       |
      | accounts_daily_report                      | 87      | 1       |
      | accounts_metros_hourly_report              | 226240  | 1       |
      | advertiser_business_categories             | 16      | 1       |
      | bidding_rules                              | 64      | 1       |
      | campaigns                                  | 2       | 1       |
      | campaigns_conversion_tags_daily_report     | 0       | 1       |
      | campaigns_daily_report                     | 10      | 1       |
      | campaigns_genders_hourly_report            | 504     | 1       |
      | cards_image_app_download                   | 0       | 1       |
      | cards_image_conversation                   | 0       | 1       |
      | cards_image_direct_message                 | 0       | 1       |
      | cards_poll                                 | 0       | 1       |
      | cards_video_app_download                   | 0       | 1       |
      | cards_video_conversation                   | 0       | 1       |
      | cards_video_direct_message                 | 0       | 1       |
      | cards_video_website                        | 0       | 1       |
      | cards_website                              | 0       | 1       |
      | content_categories                         | 392     | 1       |
      | funding_instruments                        | 1       | 1       |
      | funding_instruments_age_daily_report       | 175     | 1       |
      | funding_instruments_devices_hourly_report  | 15288   | 1       |
      | funding_instruments_hourly_report          | 84      | 1       |
      | iab_categories                             | 15      | 1       |
      | line_item_apps                             | 0       | 1       |
      | line_items                                 | 2       | 1       |
      | line_items_conversion_tags_hourly_report   | 0       | 1       |
      | line_items_daily_report                    | 10      | 1       |
      | line_items_platform_versions_hourly_report | 3108    | 1       |
      | line_items_platforms_hourly_report         | 588     | 1       |
      | line_items_regions_daily_report            | 630     | 1       |
      | media_creatives                            | 0       | 1       |
      | media_creatives_daily_report               | 0       | 1       |
      | organic_tweets_daily_report                | 82      | 2       |
      | organic_tweets_hourly_report               | 1344    | 1       |
      | preroll_call_to_actions                    | 0       | 1       |
      | promotable_users                           | 0       | 1       |
      | promoted_accounts                          | 0       | 1       |
      | promoted_accounts_daily_report             | 0       | 1       |
      | promoted_tweets                            | 2       | 1       |
      | promoted_tweets_daily_report               | 10      | 1       |
      | promoted_tweets_interests_daily_report     | 3210    | 1       |
      | promoted_tweets_keywords_hourly_report     | 420     | 1       |
      | promoted_tweets_languages_daily_report     | 205     | 1       |
      | scheduled_promoted_tweets                  | 0       | 1       |
      | tailored_audiences                         | 0       | 1       |
      | targeting_app_store_categories             | 84      | 1       |
      | targeting_conversations                    | 41136   | 1       |
      | targeting_criteria                         | 10      | 1       |
      | targeting_devices                          | 355     | 1       |
      | targeting_events                           | 355     | 1       |
      | targeting_interests                        | 361     | 1       |
      | targeting_languages                        | 21      | 1       |
      | targeting_locations                        | 56682   | 1       |
      | targeting_network_operators                | 161     | 1       |
      | targeting_platform_versions                | 38      | 1       |
      | targeting_platforms                        | 4       | 1       |
      | targeting_tv_markets                       | 39      | 1       |
      | targeting_tv_shows                         | 520694  | 1       |
      | tweets                                     | 2       | 1       |
      +--------------------------------------------+---------+---------+

    ```
---

Copyright &copy; 2019 Stitch
