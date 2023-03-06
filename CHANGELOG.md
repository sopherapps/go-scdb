# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [0.2.1] - 2023-03-06

### Added

### Changed

### Fixed

- Fixed Slice Out of bounds Error when Retrieving Value that is an empty string.

## [0.2.0] - 2023-01-16

### Added

### Changed

- Changed the `scdb.New()` signature, replacing `maxIndexKeyLen` option with `isSearchEnabled`.
- Permanently set the maximum index key length to 3
- Changed benchmarks to compare operations when search is enabled to when search is disabled.

### Fixed

## [0.1.0] - 2023-01-14

### Added

- Added full-text search for keys, with pagination using `store.Search(term, skip, limit)`

### Changed

- Changed the `scdb.New()` signature to include `maxIndexKeyLen` option.

### Fixed

## [0.0.7] - 2022-11-9

### Added

- More thorough documentation for the Store

### Changed

### Fixed

## [0.0.6] - 2022-11-9

### Added

### Changed

- Optimized the Compact operation.
    - Removed unnecessary internal type conversions e.g. to `internal.entries.Index`.
    - Got rid of message passing in iterating over the index blocks

### Fixed

## [0.0.5] - 2022-11-9

### Added

### Changed

- Changed to sync.Mutex instead of message passing via channels. This increased the speed to even beyond what is seen in
  the [rust scdb](https://github.com/sopherapps/scdb) except in compact

### Fixed

## [0.0.4] - 2022-11-8

### Added

### Changed

- Optimized the Get operation. Removed unnecessary internal type conversions e.g. to `internal.buffers.Value`

### Fixed

## [0.0.3] - 2022-11-8

### Added

### Changed

### Fixed

- Fix the `BufferPool.TryDeleteKvEntry` to return true when delete from file is successful

## [0.0.2] - 2022-11-8

### Added

### Changed

### Fixed

- Fixed typo in package name `github.com/sopherapps/go-scdb` (originally `github.com/sopherapps/go-scbd`)

## [0.0.1] - 2022-11-8

### Added

- Initial release

### Changed

### Fixed
