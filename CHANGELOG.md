# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- Don't use the `aws-lc-rs` crate (introduced in [#45]), as it broke the Tilt build ([#46]).

[#46]: https://github.com/stackabletech/trino-lb/pull/46

## [0.3.1] - 2024-08-16

### Fixed

- Install default crypto provider, this prevent servers using https from starting ([#45]).

[#45]: https://github.com/stackabletech/trino-lb/pull/45

## [0.3.0] - 2024-08-15

### Added

- Added a configuration to specify the port numbers for `http`, `https` and `metrics` ([#43]).

### Changed

- BREAKING: Ensure no unknown config properties have been set. This is to make the user aware that what he tried to configure is not a valid configuration. You may need to adapt your configuration and remove any unknown properties ([#43]).
- Bump dependencies, such as `opentelemetry` 0.23 -> 0.24, `kube` 0.92 -> 0.93 and `redis` 0.25 -> 0.26 ([#41]).

[#41]: https://github.com/stackabletech/trino-lb/pull/41
[#43]: https://github.com/stackabletech/trino-lb/pull/43

## [0.2.3] - 2024-07-01

### Fixed

- URL-escape trino cluster credentials ([#40]).

[#40]: https://github.com/stackabletech/trino-lb/pull/40

## [0.2.2] - 2024-06-28

### Fixed

- Periodically set all clusters that are not scaled to `Ready`. Previously this was only done during startup, which
  caused problems when the persistence was wiped while trino-lb is running ([#37]).

[#37]: https://github.com/stackabletech/trino-lb/pull/37

## [0.2.1] - 2024-06-21

### Fixed

- Use redis [`ConnectionManager`](https://docs.rs/redis/latest/redis/aio/struct.ConnectionManager.html) to reconnect on
  Redis connection failures. Previously trino-lb would stop working once the Redis Pod restarted. This change only
  affects the single Redis instance connection, *not* the cluster mode connection, as a
  [ClusterConnection](https://docs.rs/redis/latest/redis/cluster/struct.ClusterConnection.html) does not seem to support
  a [`ConnectionManager`](https://docs.rs/redis/latest/redis/aio/struct.ConnectionManager.html) ([#34]).

[#34]: https://github.com/stackabletech/trino-lb/pull/34

## [0.2.0] - 2024-04-07

### Added

- BREAKING: Add support for single instance redis persistence (without Redis cluster mode).
  This is breaking, because you need to set `trinoLb.persistence.redis.clusterMode: true` in your config to keep using
  the cluster mode ([#15]).

[#15]: https://github.com/stackabletech/trino-lb/pull/15

## [0.1.0] - 2024-01-16
