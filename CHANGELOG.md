# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.2.0] - 2024-04-07

### Added

- BREAKING: Add support for single instance redis persistence (without Redis cluster mode).
  This is breaking, because you need to set `trinoLb.persistence.redis.clusterMode: true` in your config to keep using the cluster mode ([#15]).

[#15]: https://github.com/stackabletech/trino-lb/pull/15

## [0.1.0] - 2024-01-16
