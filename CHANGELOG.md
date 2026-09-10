# Changelog

All notable changes to Basalt are documented here.

This project follows [Semantic Versioning](https://semver.org/): incompatible public or durable-format changes require a major version, backward-compatible features require a minor version, and backward-compatible fixes require a patch version. Prerelease identifiers may be used before a stable release.

## [Unreleased]

### Added

- Storage-neutral managed API for Embedded and SQL Server providers.
- Typed jobs, durable retry and idempotent enqueue.
- Durable schedules, static workflow DAGs, management, recovery, leases, and fencing.
- Simplified managed API, SQL migration modes, and .NET Framework 4.7.2/WPF consumer support.

### Changed

- Managed management models expose enums and .NET-friendly time values.

### Fixed

- Requeue clears the previous terminal ledger entry atomically.
- Embedded execution listing tolerates concurrent retry transitions.
