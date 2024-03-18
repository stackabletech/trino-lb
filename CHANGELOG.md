# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- BREAKING: Add support for single instance redis persistence (without Redis cluster mode).
  This is breaking, because you need to set `trinoLb.persistence.redis.clusterMode: true` in your config to keep using the cluster mode ([#XXX]).

[#XXX]: https://github.com/stackabletech/trino-lb/pull/XXX
