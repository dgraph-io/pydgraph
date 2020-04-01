# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]
===================

## [v20.03.0] - 2020-03-31

Starting with this release, the release number has changed to match the Dgraph release
to make it easier to identify which version of Dgraph a client version supports.

### Added
- Use RunInBackground flag for computing indexes in background.

## [v2.0.3] - 2020-03-24

### Added
- Updated protobufs to latest version.

## [v2.0.2] - 2019-09-10

### Added
- Do not throw errors in build steps if pypandoc is not found.

## [v2.0.1] - 2019-09-06

### Added
- Fix unhandled ModuleNotFoundError.

## [v2.0.0] - 2019-09-05

### Added
- Update internal grpc API to talk to dgraph v1.1.0

## [v1.2.0] - 2019-06-24

### Added
- Added support for Upsert Block

## [v1.1.2] - 2019-06-07

### Added
- Updated requirements.txt to unpin protobuf version.
- Manually free Grpc resources on stub close.

## [v1.1.1] - 2019-04-26

### Added
- Bug fix

## [v1.1] - 2019-04-16

### Added
- Support for ACL (Access Control List).

### Removed
- The query method from the client class has been deprecated. This was done in
  order to match the rest of the clients and to make it explicit that creating a
  transaction is required to query Dgraph.

## [v1.0.3] - 2019-03-20

### Added
- Support for best-effort queries.

## [v1.0.2] - 2019-03-19

### Added
- During queries, passing a map with non-string keys or values as a variable
  map will result in an error instead of continuing silently.
- Fixed dependencies.

## [v1.0.1] - 2019-01-03

### Added
- Full compatibility with Dgraph v1.0.11
- Added support for read-only transactions.
- Fixed dependencies.
- Support for predicate tracking.
- Remove linread map and sequencing.

## [v1.0.0] - 2018-05-16

### Added
- Full compatibility with Dgraph v1.0.0

[Unreleased]: https://github.com/dgraph-io/pydgraph/compare/v1.0.0...HEAD
[v2.0.0] https://github.com/dgraph-io/pydgraph/compare/v1.2.0...v2.0.0
[v1.2.0] https://github.com/dgraph-io/pydgraph/compare/v1.1.2...v1.2.0
[v1.1.2]: https://github.com/dgraph-io/pydgraph/compare/v1.1.1...v1.1.2
[v1.1.1]: https://github.com/dgraph-io/pydgraph/compare/v1.1...v1.1.1
[v1.1]: https://github.com/dgraph-io/pydgraph/compare/v1.0.3...v1.1
[v1.0.3]: https://github.com/dgraph-io/pydgraph/compare/v1.0.2...v1.0.3
[v1.0.2]: https://github.com/dgraph-io/pydgraph/compare/v1.0.1...v1.0.2
[v1.0.1]: https://github.com/dgraph-io/pydgraph/compare/v1.0.0...v1.0.1
[v1.0.0]: https://github.com/dgraph-io/pydgraph/releases/tag/v1.0.0
