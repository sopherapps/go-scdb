# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

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
