# Changelog

## 1.1.0
  * Library version upgrades and updated metadata for streams [#46](https://github.com/singer-io/tap-twitter-ads/pull/46)

## 1.0.0
  * Upgraded SDK and API version [#19](https://github.com/singer-io/tap-twitter-ads/pull/19)
  * Added credentials check in discover mode [#15](https://github.com/singer-io/tap-twitter-ads/pull/15)
  * Added custom exception handling [#16](https://github.com/singer-io/tap-twitter-ads/pull/16)
  * Implemented request timeout [#17](https://github.com/singer-io/tap-twitter-ads/pull/17)
  * Made child streams independent of parent stream [#18](https://github.com/singer-io/tap-twitter-ads/pull/18)
  * Refactored Dict based implementation to class based implementation [#21](https://github.com/singer-io/tap-twitter-ads/pull/21)
  * Fixed invalid bookmarking for `tweets` stream [#22](https://github.com/singer-io/tap-twitter-ads/pull/22)
  * Implemented individual bookmarking for each account [#23](https://github.com/singer-io/tap-twitter-ads/pull/23)
  * Added new `cards` stream and removed deprecated streams [#25](https://github.com/singer-io/tap-twitter-ads/pull/25)
  * Added `null` type in the schema when the type is `object` [#26](https://github.com/singer-io/tap-twitter-ads/pull/26)
  * Updated params for `content_categories` and `iab_categories` [#27](https://github.com/singer-io/tap-twitter-ads/pull/27)
  * Added pagination parameter for `targeting devices` [#28](https://github.com/singer-io/tap-twitter-ads/pull/28)
  * Added missing tap-tester test cases [#20](https://github.com/singer-io/tap-twitter-ads/pull/20)
  * Resolved ConnectionResetError [#32](https://github.com/singer-io/tap-twitter-ads/pull/32)
  
## 0.0.7
  * Update twitter-ads SDK to v9.0.1 [#13](https://github.com/singer-io/tap-twitter-ads/pull/13)

## 0.0.6
  * Update twitter-ads SDK to v8.0.0 [#11](https://github.com/singer-io/tap-twitter-ads/pull/11)

## 0.0.5
  * Fix start/end datetime conversions to resolve daylight savings time error and entity start/end rounding errors.

## 0.0.4
  * Change report bookmarking to bookmark after each date window; Improve client rate limiting, error handling, and timeout handling.

## 0.0.3
  * Fix intermittent `None` bookmark issue.

## 0.0.2
  * Add `$ref` references to JSON schemas. Refactor `sync_reports` into several other functions.

## 0.0.1
  * Initial commit
