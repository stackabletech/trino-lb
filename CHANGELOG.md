# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

- The Stackable scaler now ensures that a `TrinoCluster` has changed to `ready` more than 5 seconds
  ago before marking it as `ready` ([#68]).
- Support two different proxy modes: `ProxyAllCalls` (default) and `ProxyFirstCall`.
  Read the [proxy modes docs](./docs/proxy-modes.md) for details ([#82]).

[#68]: https://github.com/stackabletech/trino-lb/pull/68
[#82]: https://github.com/stackabletech/trino-lb/pull/82

## [0.5.0] - 2025-03-14

### Added

- Support configuring compression for OTEL ([#70]).
- Improve tracing details by adding a `tower_http::trace::TraceLayer` that creates spans for every HTTP request ([#71]).
- Support compressing HTTP contents, previously the content was always uncompressed.
  This consumes more CPU, but also reduces the data amount sent to Trino clients.
  E.g. `trino-cli` by default asks for `gzip` compressed content ([#74]).

### Changed

- Improve tracing for running queries on Trino, adding spans for the request to Trino and parsing ([#71]).
- Improve performance by using [`serde_json::value::RawValue`](https://docs.rs/serde_json/latest/serde_json/value/struct.RawValue.html) for the `data` and `columns` attributes to avoid unneeded deserialization and serialization of them ([#73]).
- Bumped to Rust 2024 edition ([#76]).

[#70]: https://github.com/stackabletech/trino-lb/pull/70
[#71]: https://github.com/stackabletech/trino-lb/pull/71
[#73]: https://github.com/stackabletech/trino-lb/pull/73
[#74]: https://github.com/stackabletech/trino-lb/pull/74
[#76]: https://github.com/stackabletech/trino-lb/pull/76

## [0.4.1] - 2025-03-03

### Fixed

- Add the `libpython3-dev` package to the container image. This prevented the startup of trino-lb ([#66]).

[#66]: https://github.com/stackabletech/trino-lb/pull/66

## [0.4.0] - 2025-02-28

### Added

- Support configuring the scaler reconcile interval ([#61]).
- Add simple web-based dashboard that shows the current state and query counts of all clusters.
  This makes it easier to debug state transitions of clusters ([#62]).
- Add `Unhealthy` cluster state.
  This state is entered once the readiness check of a cluster in the `Ready` state fails.
  The cluster will remain in the `Unhealthy` state until the scaler marks that cluster as `Ready` again.
  `Unhealthy` clusters won't get any new queries; if all clusters are unhealthy, new queries will be queued.
  The cluster health check interval can be configured using the scaler reconcile interval ([#63]).

### Changed

- Set defaults to oci ([#57]).

### Fixed

- Reduce max poll delay from 10s to 3s to have better client responsiveness

[#57]: https://github.com/stackabletech/trino-lb/pull/57
[#61]: https://github.com/stackabletech/trino-lb/pull/61
[#62]: https://github.com/stackabletech/trino-lb/pull/62
[#63]: https://github.com/stackabletech/trino-lb/pull/63

## [0.3.2] - 2024-08-20

### Changed

- Don't use the `aws-lc-rs` crate (introduced in [#45]), as it broke the Tilt build ([#46]).

### Fixed

- Fix division by zero when all clusters of a cluster group are not ready to accept queries ([#47]).

[#46]: https://github.com/stackabletech/trino-lb/pull/46
[#47]: https://github.com/stackabletech/trino-lb/pull/47

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
