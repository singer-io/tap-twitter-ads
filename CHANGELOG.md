# Changelog

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
