# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project
adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [v25.0.0] - 2025-12-18

**Added**

- Native async/await client support with `AsyncDgraphClient`, `AsyncDgraphClientStub`, and
  `AsyncTxn` classes ([#280](https://github.com/dgraph-io/pydgraph/pull/280))
  - True asynchronous operations using Python's asyncio and grpc.aio
  - Async context manager support for client and transactions
  - Connection string support via `async_open()` function
  - Automatic JWT refresh handling
- Dgraph v25 API support:
  - `run_dql()` method for executing DQL queries and mutations directly
  - `run_dql_with_vars()` method for DQL queries with variables
  - `allocate_uids()` method for allocating unique identifiers
  - `allocate_timestamps()` method for allocating timestamps
  - `allocate_namespaces()` method for allocating namespace IDs
  - Namespace management methods: `create_namespace()`, `drop_namespace()`, `list_namespaces()`
- Convenience methods for schema and data management:
  - `drop_all()` - Drops all data and schema
  - `drop_data()` - Drops all data while preserving schema
  - `drop_predicate()` - Drops a specific predicate
  - `drop_type()` - Drops a specific type
  - `set_schema()` - Sets the DQL schema
- Updated proto definitions to support Dgraph v25 API

**Deprecated**

- `DgraphClientStub.from_cloud()` and `AsyncDgraphClientStub.from_cloud()` methods (deprecated in
  25.1.0, removal planned for 26.0.0)
  - Dgraph Cloud service has been discontinued
  - Use standard `DgraphClientStub` constructor with `grpc.ssl_channel_credentials()` instead
  - Docstrings include migration examples
- `DgraphClientStub.parse_host()` and `AsyncDgraphClientStub.parse_host()` methods (deprecated in
  25.1.0, removal planned for 26.0.0)
  - Use standard gRPC hostname handling instead

**Chore**

- Updated GitHub Actions workflows to v5/v6
- Added comprehensive test coverage for new v25 API methods

## [v24.3.0] - 2025-07-29

**Chore**

- Bumped minimum grpcio version to 1.65.0
- Pinned the grpcio-tools version to 1.65.x
- Updated generated modules following a grpc deps update

## [v24.2.1] 2025-04-02

**Chore**

- Updated generated modules following a grpc deps update

## [v24.2.0] - 2025-04-01

**Added**

- Add new `Open` function to support Dgraph Connection Strings

## [v24.1.0] - 2024-11-29

**Added**

- add RAG notebooks by @rderbier in https://github.com/dgraph-io/pydgraph/pull/240
- Bump grpcio-tools from 1.65.2 to 1.68.0 in the minor-and-patch group by @dependabot in #242 #243
  #244 #245 #247 #251
- Bump the minor-and-patch group with 2 updates by @dependabot in
  https://github.com/dgraph-io/pydgraph/pull/246
- Bump build from 1.2.2 to 1.2.2.post1 by @dependabot in
  https://github.com/dgraph-io/pydgraph/pull/248
- Bump setuptools from 67.7.2 to 75.6.0 by @dependabot in
  https://github.com/dgraph-io/pydgraph/pull/252
- update proto file to support bigfloat data type by @mangalaman93 in
  https://github.com/dgraph-io/pydgraph/pull/255

## [v24.0.2] - 2024-07-24

**Added**

- Create example computeEmbeddings.py by @rderbier in https://github.com/dgraph-io/pydgraph/pull/221
- Allow flexible dependency versions by @gautambhat in
  https://github.com/dgraph-io/pydgraph/pull/233
- update proto to make it consistent with dgraph-io/dgo by @mangalaman93 in
  https://github.com/dgraph-io/pydgraph/pull/237
- Bump protobuf from 4.22.3 to 5.27.2 by @dependabot in
  https://github.com/dgraph-io/pydgraph/pull/231
- Minor and patch dependency upgrades

## [v23.0.2] - 2023-11-08

**Added**

- accept custom grpc options in from_cloud function (#215)
- return commit_ts in the function 'commit()' in txn.py (#213)
- add ai-classification notebook (#207)
- Add Jupyter Notebook example (#206)

**Chore**

- chore(docs): Remove invalid emails and non-maintainers. (#208)

## [v23.0.1] - 2023-05-29

**Added**

- chore(cd): fix protobuf import issue (#204)

## [v23.0.0] - 2023-05-15

**Breaking**

- Minimum required Python now 3.7 (CI runs against 3.11)
- deprecated from_slash_endpoint function (#190)

**Added**

- accept grpc endpoint or /graphql endpoint in from_cloud function
- `pyproject.toml` as the source of truth for dependency requirements
- `.python-version` file to keep track of supported Python version

**Chore**

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

**Added**

- Missing variable in from_cloud (#174)

## [v21.3.1] - 2021-08-04

**Added**

- Add from_cloud method (#169)
- Refresh examples (#171)
- test: Update test setup to use randomized Docker ports (picking up the port from
  `TEST_SERVER_ADDR`)
- test: Use `--guardian-creds` superflags in ACL tests

## [v21.03.0] - 2020-04-09

**Added**

- Login to a namespace
- Add response type
- Hash to response and txn context.
- Deprecation messaged for Slash Endpoint [168]

## [v20.07.0] - 2020-09-18

**Added**

- Support for Slash GraphQl
- Fixed missing import in the client.

## [v20.03.1] - 2020-06-03

**Added**

- Added more exception classes for specific types of errors (e.g retriable errors). Existing
  applications might want to update their error handling code to take advantage of these new error
  classes.
- Added async versions of alter, query, and mutate functions.

## [v20.03.0] - 2020-03-31

Starting with this release, the release number has changed to match the Dgraph release to make it
easier to identify which version of Dgraph a client version supports.

**Added**

- Use RunInBackground flag for computing indexes in background.

## [v2.0.3] - 2020-03-24

**Added**

- Updated protobufs to latest version.

## [v2.0.2] - 2019-09-10

**Added**

- Do not throw errors in build steps if pypandoc is not found.

## [v2.0.1] - 2019-09-06

**Added**

- Fix unhandled ModuleNotFoundError.

## [v2.0.0] - 2019-09-05

**Added**

- Update internal grpc API to talk to dgraph v1.1.0

## [v1.2.0] - 2019-06-24

**Added**

- Added support for Upsert Block

## [v1.1.2] - 2019-06-07

**Added**

- Updated requirements.txt to unpin protobuf version.
- Manually free Grpc resources on stub close.

## [v1.1.1] - 2019-04-26

**Added**

- Bug fix

## [v1.1] - 2019-04-16

**Added**

- Support for ACL (Access Control List).

**Removed**

- The query method from the client class has been deprecated. This was done in order to match the
  rest of the clients and to make it explicit that creating a transaction is required to query
  Dgraph.

## [v1.0.3] - 2019-03-20

**Added**

- Support for best-effort queries.

## [v1.0.2] - 2019-03-19

**Added**

- During queries, passing a map with non-string keys or values as a variable map will result in an
  error instead of continuing silently.
- Fixed dependencies.

## [v1.0.1] - 2019-01-03

**Added**

- Full compatibility with Dgraph v1.0.11
- Added support for read-only transactions.
- Fixed dependencies.
- Support for predicate tracking.
- Remove linread map and sequencing.

## [v1.0.0] - 2018-05-16

**Added**

- Full compatibility with Dgraph v1.0.0

[v2.0.0]: https://github.com/dgraph-io/pydgraph/compare/v1.2.0...v2.0.0
[v1.2.0]: https://github.com/dgraph-io/pydgraph/compare/v1.1.2...v1.2.0
[v1.1.2]: https://github.com/dgraph-io/pydgraph/compare/v1.1.1...v1.1.2
[v1.1.1]: https://github.com/dgraph-io/pydgraph/compare/v1.1...v1.1.1
[v1.1]: https://github.com/dgraph-io/pydgraph/compare/v1.0.3...v1.1
[v1.0.3]: https://github.com/dgraph-io/pydgraph/compare/v1.0.2...v1.0.3
[v1.0.2]: https://github.com/dgraph-io/pydgraph/compare/v1.0.1...v1.0.2
[v1.0.1]: https://github.com/dgraph-io/pydgraph/compare/v1.0.0...v1.0.1
[v1.0.0]: https://github.com/dgraph-io/pydgraph/releases/tag/v1.0.0
