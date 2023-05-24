# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [v23.0.0] - 2023-05-15

### Breaking
- Minimum required Python now 3.7 (CI runs against 3.11)
- deprecated from_slash_endpoint function (#190)

### Added
- accept grpc endpoint or /graphql endpoint in from_cloud function
- `pyproject.toml` as the source of truth for dependency requirements
- `.python-version` file to keep track of supported Python version

### Chore
- Github Actions for CI/CD pipelines
  - chore(ci): test against latest dgraph on main (#196)
  - chore(ci): fix test script & use latest docker image (#195)
  - chore(ci/cd): add pipelines + modernize repo (#193)
  - chore(ci): add ci to pydgraph (#192)
- Add example for datetime parsing (#170)
- cleanup README, compose files and doc links (#194)
- fix the TLS examples in examples/tls (#198)
- update docs and fix flaky tests (#199)

## [v21.3.2] - 2021-08-05

### Added
- Missing variable in from_cloud (#174)

## [v21.3.1] - 2021-08-04

### Added
- Add from_cloud method (#169)
- Refresh examples (#171)
- test: Update test setup to use randomized Docker ports (picking up the port from `TEST_SERVER_ADDR`)
- test: Use `--guardian-creds` superflags in ACL tests

## [v21.03.0] - 2020-04-09

### Added
-  Login to a namespace
-  Add response type
-  Hash to response and txn context.
-  Deprecation messaged for Slash Endpoint [168]

## [v20.07.0] - 2020-09-18

### Added
- Support for Slash GraphQl
- Fixed missing import in the client.

## [v20.03.1] - 2020-06-03

### Added
- Added more exception classes for specific types of errors (e.g retriable
errors). Existing applications might want to update their error handling code to
take advantage of these new error classes.
- Added async versions of alter, query, and mutate functions.

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

[Unreleased]:https://github.com/dgraph-io/pydgraph/compare/v1.0.0...HEAD
[v2.0.0]:https://github.com/dgraph-io/pydgraph/compare/v1.2.0...v2.0.0
[v1.2.0]:https://github.com/dgraph-io/pydgraph/compare/v1.1.2...v1.2.0
[v1.1.2]:https://github.com/dgraph-io/pydgraph/compare/v1.1.1...v1.1.2
[v1.1.1]:https://github.com/dgraph-io/pydgraph/compare/v1.1...v1.1.1
[v1.1]:https://github.com/dgraph-io/pydgraph/compare/v1.0.3...v1.1
[v1.0.3]:https://github.com/dgraph-io/pydgraph/compare/v1.0.2...v1.0.3
[v1.0.2]:https://github.com/dgraph-io/pydgraph/compare/v1.0.1...v1.0.2
[v1.0.1]:https://github.com/dgraph-io/pydgraph/compare/v1.0.0...v1.0.1
[v1.0.0]:https://github.com/dgraph-io/pydgraph/releases/tag/v1.0.0
