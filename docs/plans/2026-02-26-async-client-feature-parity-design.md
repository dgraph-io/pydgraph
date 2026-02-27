---
date: 2026-02-26
topic: async-client-feature-parity
---

# Async Client Feature Parity

## What We're Building

Add 13 missing public methods (plus 1 private helper) to `AsyncDgraphClient` and 5 gRPC wrapper
methods to `AsyncDgraphClientStub`, achieving 1:1 feature parity with the sync client. Every method
on `DgraphClient` will have a corresponding `async def` on `AsyncDgraphClient` with the same
signature, parameters, return types, and behavior — differing only by `async`/`await`.

## Why This Approach

We evaluated three approaches:

1. **Direct port** (chosen) — mirror each sync method as an async counterpart
2. **Shared base class** — extract common logic into a mixin; rejected because the sync/async call
   patterns are inherently different and refactoring existing working code adds risk
3. **Code generation** — auto-generate async from sync; rejected as overengineering for 13 methods

Direct port matches the existing codebase pattern (e.g., `alter`, `login`, `check_version` already
exist as parallel sync/async implementations).

## Key Decisions

- **All methods on `AsyncDgraphClient`**: matches sync client layout exactly
- **Same signatures**: parameters, defaults, return types identical to sync counterparts
- **Same JWT retry pattern**: `is_jwt_expired` → `await self.retry_login()` → retry
- **Same error mapping**: `is_retriable_error`, `is_connection_error` where applicable

## Changes

### `pydgraph/async_client_stub.py` — 5 new gRPC wrappers

| Method             | gRPC call                    | Returns                       |
| ------------------ | ---------------------------- | ----------------------------- |
| `run_dql()`        | `self._stub.RunDQL()`        | `api.Response`                |
| `allocate_ids()`   | `self._stub.AllocateIDs()`   | `api.AllocateIDsResponse`     |
| `create_namespace` | `self._stub.CreateNamespace` | `api.CreateNamespaceResponse` |
| `drop_namespace`   | `self._stub.DropNamespace`   | `api.DropNamespaceResponse`   |
| `list_namespaces`  | `self._stub.ListNamespaces`  | `api.ListNamespacesResponse`  |

### `pydgraph/async_client.py` — 13 public + 1 private method

**Group 1: Alter wrappers** (delegate to `self.alter()`):

- `drop_all()`, `drop_data()`, `drop_predicate(predicate)`, `drop_type(type_name)`,
  `set_schema(schema)`

**Group 2: Direct gRPC with JWT retry**:

- `run_dql()`, `run_dql_with_vars()`, `create_namespace()`, `drop_namespace(namespace)`,
  `list_namespaces()`, `_allocate_ids()` (private)

**Group 3: Allocation convenience** (delegate to `_allocate_ids()`):

- `allocate_uids(how_many)`, `allocate_timestamps(how_many)`, `allocate_namespaces(how_many)`

### `tests/test_async_client.py` — 4 new test classes

- `TestAsyncConvenienceMethods` — drop_all, drop_data, drop_predicate, drop_type, set_schema
- `TestAsyncDQL` — run_dql, run_dql_with_vars
- `TestAsyncAllocations` — allocate_uids, allocate_timestamps, allocate_namespaces
- `TestAsyncNamespaces` — create_namespace, drop_namespace, list_namespaces

### No changes needed

- `async_txn.py` — all methods live on the client
- `__init__.py` — no new public symbols (methods added to existing class)
- `errors.py`, `util.py` — existing helpers are sufficient

## Open Questions

None — design is straightforward port of existing sync patterns.

## Next Steps

Implement on `feat/async-client-feature-parity` branch.
