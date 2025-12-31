# pydgraph

This is the official Dgraph database client implementation for Python (Python >= v3.9), using
[gRPC][grpc].

[grpc]: https://grpc.io/

This client follows the [Dgraph Go client][goclient] closely.

[goclient]: https://github.com/dgraph-io/dgo

Before using this client, we highly recommend that you read the the product documentation at
[dgraph.io/docs].

[dgraph.io/docs]: https://dgraph.io/docs

## Table of contents

- [pydgraph](#pydgraph)
  - [Table of contents](#table-of-contents)
  - [Install](#install)
  - [Supported Versions](#supported-versions)
  - [Quickstart](#quickstart)
  - [Using a client](#using-a-client)
    - [Creating a Client](#creating-a-client)
    - [Login into a Namespace](#login-into-a-namespace)
    - [Connecting To Dgraph Cloud](#connecting-to-dgraph-cloud)
    - [Altering the Database](#altering-the-database)
    - [Creating a Transaction](#creating-a-transaction)
    - [Running a Mutation](#running-a-mutation)
    - [Running a Query](#running-a-query)
    - [Query with RDF response](#query-with-rdf-response)
    - [Running an Upsert: Query + Mutation](#running-an-upsert-query--mutation)
    - [Running a Conditional Upsert](#running-a-conditional-upsert)
    - [Committing a Transaction](#committing-a-transaction)
    - [Cleaning Up Resources](#cleaning-up-resources)
    - [Setting Metadata Headers](#setting-metadata-headers)
    - [Setting a timeout](#setting-a-timeout)
    - [Async methods](#async-methods)
  - [Examples](#examples)
  - [Development](#development)
    - [Setting up environment](#setting-up-environment)
    - [Build from source](#build-from-source)
    - [Running tests](#running-tests)

## Install

Install using pip:

```sh
pip install pydgraph
```

### Protobuf Version Compatibility

pydgraph supports protobuf versions 4.23.0 through 6.x. The specific version installed depends on
your environment:

- **Modern environments**: protobuf 6.x is recommended and will be installed by default on Python
  3.13+
- **Legacy environments**: If you need to use protobuf 4.x or 5.x (e.g., for compatibility with
  other packages), you can pin the version:

```sh
# For protobuf 4.x compatibility
pip install pydgraph "protobuf>=4.23.0,<5.0.0"

# For protobuf 5.x compatibility
pip install pydgraph "protobuf>=5.0.0,<6.0.0"
```

## Supported Versions

Depending on the version of Dgraph that you are connecting to, you should use a different version of
this client. Using an incompatible version may lead to unexpected behavior or errors.

| Dgraph version | pydgraph version |
| :------------: | :--------------: |
|    21.03.x     |    _21.03.x_     |
|    23.0.x+     |     _23.0.x_     |
|    24.0.x+     |     _24.0.x_     |
|    25.0.x+     |     _25.0.x_     |

## Quickstart

Build and run the [simple project][simple] in the `examples` folder, which contains an end-to-end
example of using the Dgraph python client. For additional details, follow the instructions in the
project's [README](./examples/simple/README.md).

[simple]: ./examples/simple

## Using a client

### Creating a Client

You can initialize a `DgraphClient` object by passing it a list of `DgraphClientStub` clients as
variadic arguments. Connecting to multiple Dgraph servers in the same cluster allows for better
distribution of workload.

The following code snippet shows just one connection.

```python3
import pydgraph

client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)
```

### Using Dgraph Connection Strings

The pydgraph package supports connecting to a Dgraph cluster using connection strings. Dgraph
connections strings take the form `dgraph://{username:password@}host:port?args`.

`username` and `password` are optional. If username is provided, a password must also be present. If
supplied, these credentials are used to log into a Dgraph cluster through the ACL mechanism.

Valid connection string args:

| Arg         | Value                           | Description                                                                                                                                                   |
| ----------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| apikey      | \<key\>                         | a Dgraph Cloud API Key                                                                                                                                        |
| bearertoken | \<token\>                       | an access token                                                                                                                                               |
| sslmode     | disable \| require \| verify-ca | TLS option, the default is `disable`. If `verify-ca` is set, the TLS certificate configured in the Dgraph cluster must be from a valid certificate authority. |
| namespace   | \<namespace\>                   | a previously created integer-based namespace, username and password must be supplied                                                                          |

Note the `sslmode=require` pair is not supported and will throw an Exception if used. Python grpc
does not support traffic over TLS that does not fully verify the certificate and domain. Developers
should use the existing stub/client initialization steps for self-signed certs as demonstrated in
/examples/tls/tls_example.py

Some example connection strings:

| Value                                                                                                        | Explanation                                                                         |
| ------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------- |
| dgraph://localhost:9080                                                                                      | Connect to localhost, no ACL, no TLS                                                |
| dgraph://sally:supersecret@dg.example.com:443?sslmode=verify-ca                                              | Connect to remote server, use ACL and require TLS and a valid certificate from a CA |
| dgraph://foo-bar.grpc.us-west-2.aws.cloud.dgraph.io:443?sslmode=verify-ca&apikey=\<your-api-connection-key\> | Connect to a Dgraph Cloud cluster                                                   |
| dgraph://foo-bar.grpc.dgraph-io.com:443?sslmode=verify-ca&bearertoken=\<some access token\>                  | Connect to a Dgraph cluster protected by a secure gateway                           |
| dgraph://sally:supersecret@dg.example.com:443?namespace=2                                                    | Connect to a ACL enabled Dgraph cluster in namespace 2                              |

Using the `Open` function with a connection string:

```go
// open a connection to an ACL-enabled, non-TLS cluster and login as groot
client = pydgraph.open("dgraph://groot:password@localhost:8090")

// Use the client
...

client.close()
```

### Login into a Namespace

If your server has Access Control Lists enabled (Dgraph v1.1 or above), the client must be logged in
for accessing data. If you didn't use the `open` function with credentials, use the `login`
endpoint.

Calling `login` will obtain and remember the access and refresh JWT tokens. All subsequent
operations via the logged in client will send along the stored access token.

```python3
client.login("groot", "password")
```

If your server additionally has namespaces (Dgraph v21.03 or above), use the `login_into_namespace`
API.

```python3
client.login_into_namespace("groot", "password", "123")
```

### Connecting To Dgraph Cloud

If you want to connect to Dgraph running on [Dgraph Cloud](https://cloud.dgraph.io) instance, then
get the gRPC endpoint of your cluster that you can find in the
[Settings section](https://cloud.dgraph.io/_/settings) of Dgraph Cloud console and obtain a Client
or Admin API key (created in the [API key tab](https://cloud.dgraph.io/_/settings?tab=api-keys) of
the Setting section). Create the `client_stub` using the gRPC endpoint and the API key:

```python3
client_stub = pydgraph.DgraphClientStub.from_cloud(
    "https://morning-glade.grpc.us-east-1.aws.cloud.dgraph.io:443", "<api-key>")
client = pydgraph.DgraphClient(client_stub)
```

Alternatively, you can simply use a Dgraph connection string with the `open` function. For example:

```python
conn_str = "dgraph://foo-bar.grpc.us-west-2.aws.cloud.dgraph.io:443?sslmode=verify-ca&apikey=<your-api-connection-key>"
client = pydgraph.open(conn_str)

# some time later...
client.close()
```

The `DgraphClientStub.from_slash_endpoint()` method has been removed v23.0. Please use
`DgraphClientStub.from_cloud()` instead.

### Altering the Database

#### Set the Dgraph types schema

To set the Dgraph types schema (aka DQL schema), create an `Operation` object, set the schema and
pass it to `DgraphClient#alter(Operation)` method.

```python3
schema = 'name: string @index(exact) .'
op = pydgraph.Operation(schema=schema)
client.alter(op)
```

Indexes can be computed in the background. You can set the `run_in_background` field of
`pydgraph.Operation` to `True` before passing it to the `Alter` function. You can find more details
in the
[Dgraph documentation on background indexes](https://docs.dgraph.io/master/query-language/#indexes-in-background).

**Note** To deploy the GraphQL schema in python you have to use GraphQL client such as
[python-graphql-client](https://github.com/prodigyeducation/python-graphql-client) to invoke the
GraphQL admin mutation
[updateGQLSchema](https://dgraph.io/docs/graphql/admin/#using-updategqlschema-to-add-or-modify-a-schema)

```python3
schema = 'name: string @index(exact) .'
op = pydgraph.Operation(schema=schema, run_in_background=True)
client.alter(op)
```

#### Drop data

To drop all data and schema:

```python3
# Drop all data including schema from the Dgraph instance. This is a useful
# for small examples such as this since it puts Dgraph into a clean state.
op = pydgraph.Operation(drop_all=True)
client.alter(op)
```

**Note** If the Dgraph cluster contains a GraphQL Schema, it will also be deleted by this operation.

To drop all data and preserve the DQL schema:

```python3
# Drop all data from the Dgraph instance. Keep the DQL Schema.
op = pydgraph.Operation(drop_op="DATA")
client.alter(op)
```

To drop a predicate:

```python3
# Drop the data associated to a predicate and the predicate from the schema.
op = pydgraph.Operation(drop_op="ATTR", drop_value="<predicate_name>")
client.alter(op)
```

the same result is obtained using

```python3
# Drop the data associated to a predicate and the predicate from the schema.
op = pydgraph.Operation(drop_attr="<predicate_name>")
client.alter(op)
```

To drop a type definition from DQL Schema:

```python3
# Drop a type from the schema.
op = pydgraph.Operation(drop_op="TYPE", drop_value="<predicate_name>")
client.alter(op)
```

**Note** `drop_op="TYPE"` just removes a type definition from the DQL schema. No data is removed
from the cluster. The operation does not drop the predicates associated with the type.

### Creating a Transaction

To create a transaction, call the `DgraphClient#txn()` method, which returns a new `Txn` object.
This operation incurs no network overhead.

It is good practice to call `Txn#discard()` in a `finally` block after running the transaction.
Calling `Txn#discard()` after `Txn#commit()` is a no-op and you can call `Txn#discard()` multiple
times with no additional side-effects.

```python3
txn = client.txn()
try:
  # Do something here
  # ...
finally:
  txn.discard()
  # ...
```

To create a read-only transaction, call `DgraphClient#txn(read_only=True)`. Read-only transactions
are ideal for transactions which only involve queries. Mutations and commits are not allowed.

```python3
txn = client.txn(read_only=True)
try:
  # Do some queries here
  # ...
finally:
  txn.discard()
  # ...
```

To create a read-only transaction that executes best-effort queries, call
`DgraphClient#txn(read_only=True, best_effort=True)`. Best-effort queries are faster than normal
queries because they bypass the normal consensus protocol. For this same reason, best-effort queries
cannot guarantee to return the latest data. Best-effort queries are only supported by read-only
transactions.

### Running a Mutation

`Txn#mutate(mu=Mutation)` runs a mutation. It takes in a `Mutation` object, which provides two main
ways to set data: JSON and RDF N-Quad. You can choose whichever way is convenient.

`Txn#mutate()` provides convenience keyword arguments `set_obj` and `del_obj` for setting JSON
values and `set_nquads` and `del_nquads` for setting N-Quad values. See examples below for usage.

We define a person object to represent a person and use it in a transaction.

```python3
# Create data.
p = { 'name': 'Alice' }

# Run mutation.
txn.mutate(set_obj=p)

# If you want to use a mutation object, use this instead:
# mu = pydgraph.Mutation(set_json=json.dumps(p).encode('utf8'))
# txn.mutate(mu)

# If you want to use N-Quads, use this instead:
# txn.mutate(set_nquads='_:alice <name> "Alice" .')
```

```python3
# Delete data

query = """query all($a: string)
 {
   all(func: eq(name, $a))
    {
      uid
    }
  }"""
variables = {'$a': 'Bob'}

res = txn.query(query, variables=variables)
ppl = json.loads(res.json)

# For a mutation to delete a node, use this:
txn.mutate(del_obj=person)
```

For a complete example with multiple fields and relationships, look at the [simple project][simple]
in the `examples` folder.

Sometimes, you only want to commit a mutation, without querying anything further. In such cases, you
can set the keyword argument `commit_now=True` to indicate that the mutation must be immediately
committed.

A mutation can be executed using `txn.do_request` as well.

```python3
mutation = txn.create_mutation(set_nquads='_:alice <name> "Alice" .')
request = txn.create_request(mutations=[mutation], commit_now=True)
txn.do_request(request)
```

### Committing a Transaction

A transaction can be committed using the `Txn#commit()` method. If your transaction consist solely
of `Txn#query` or `Txn#queryWithVars` calls, and no calls to `Txn#mutate`, then calling
`Txn#commit()` is not necessary.

An error is raised if another transaction(s) modify the same data concurrently that was modified in
the current transaction. It is up to the user to retry transactions when they fail.

```python
txn = client.txn()
try:
  # ...
  # Perform any number of queries and mutations
  # ...
  # and finally...
  txn.commit()
except pydgraph.AbortedError:
  # Retry or handle exception.
finally:
  # Clean up. Calling this after txn.commit() is a no-op
  # and hence safe.
  txn.discard()
```

#### Using Transaction with Context Manager

The Python context manager will automatically perform the "`commit`" action after all queries and
mutations have been done, and perform "`discard`" action to clean the transaction. When something
goes wrong in the scope of context manager, "`commit`" will not be called,and the "`discard`" action
will be called to drop any potential changes.

```python
with client.begin(read_only=False, best_effort=False) as txn:
  # Do some queries or mutations here
```

or you can directly create a transaction from the `Txn` class.

```python
with pydgraph.Txn(client, read_only=False, best_effort=False) as txn:
  # Do some queries or mutations here
```

> `client.begin()` can only be used with "`with-as`" blocks, while `pydgraph.Txn` class can be
> directly called to instantiate a transaction object.

### Running a Query

You can run a query by calling `Txn#query(string)`. You will need to pass in a
[DQL](https://dgraph.io/docs/query-language/) query string. If you want to pass an additional
dictionary of any variables that you might want to set in the query, call
`Txn#query(string, variables=d)` with the variables dictionary `d`.

The query response contains the `json` field, which returns the JSON response. Let’s run a query
with a variable `$a`, deserialize the result from JSON and print it out:

```python3
# Run query.
query = """query all($a: string) {
  all(func: eq(name, $a))
  {
    name
  }
}"""
variables = {'$a': 'Alice'}

res = txn.query(query, variables=variables)

# If not doing a mutation in the same transaction, simply use:
# res = client.txn(read_only=True).query(query, variables=variables)

ppl = json.loads(res.json)

# Print results.
print('Number of people named "Alice": {}'.format(len(ppl['all'])))
for person in ppl['all']:
  print(person)
```

This should print:

```console
Number of people named "Alice": 1
Alice
```

You can also use `txn.do_request` function to run the query.

```python3
request = txn.create_request(query=query)
txn.do_request(request)
```

### Query with RDF response

You can get query result as a RDF response by calling `Txn#query(string)` with `resp_format` set to
`RDF`. The response would contain a `rdf` field, which has the RDF encoded result.

**Note:** If you are querying only for `uid` values, use a JSON format response.

```python3
res = txn.query(query, variables=variables, resp_format="RDF")
print(res.rdf)
```

### Running an Upsert: Query + Mutation

The `txn.do_request` function allows you to use upsert blocks. An upsert block contains one query
block and one or more mutation blocks, so it lets you perform queries and mutations in a single
request. Variables defined in the query block can be used in the mutation blocks using the `uid` and
`val` functions implemented by DQL.

To learn more about upsert blocks, see the
[Upsert Block documentation](https://dgraph.io/docs/mutations/upsert-block/).

```python3
query = """{
  u as var(func: eq(name, "Alice"))
}"""

nquad = """
  uid(u) <name> "Alice" .
  uid(u) <age> "25" .
"""

mutation = txn.create_mutation(set_nquads=nquad)
request = txn.create_request(query=query, mutations=[mutation], commit_now=True)
txn.do_request(request)
```

### Running a Conditional Upsert

The upsert block also allows specifying a conditional mutation block using an `@if` directive. The
mutation is executed only when the specified condition is true. If the condition is false, the
mutation is silently ignored.

See more about
[conditional upserts in the Dgraph documentation](https://docs.dgraph.io/mutations/#conditional-upsert).

```python3
query = """
  {
    user as var(func: eq(email, "wrong_email@dgraph.io"))
  }
"""

cond = "@if(eq(len(user), 1))"
nquads = """
  uid(user) <email> "correct_email@dgraph.io" .
"""

mutation = txn.create_mutation(cond=cond, set_nquads=nquads)
request = txn.create_request(mutations=[mutation], query=query, commit_now=True)
txn.do_request(request)
```

### Cleaning Up Resources

To clean up resources, you have to call `DgraphClientStub#close()` individually for all the
instances of `DgraphClientStub`.

```python3
SERVER_ADDR1 = "localhost:9080"
SERVER_ADDR2 = "localhost:9080"

# Create instances of DgraphClientStub.
stub1 = pydgraph.DgraphClientStub(SERVER_ADDR1)
stub2 = pydgraph.DgraphClientStub(SERVER_ADDR2)

# Create an instance of DgraphClient.
client = pydgraph.DgraphClient(stub1, stub2)

# Use client
...

# Clean up resources by closing all client stubs.
stub1.close()
stub2.close()
```

#### Use context manager to automatically clean resources

Use function call:

```python
with pydgraph.client_stub(SERVER_ADDR) as stub1:
  with pydgraph.client_stub(SERVER_ADDR) as stub2:
    client = pydgraph.DgraphClient(stub1, stub2)
```

Use class constructor:

```python
with pydgraph.DgraphClientStub(SERVER_ADDR) as stub1:
  with pydgraph.DgraphClientStub(SERVER_ADDR) as stub2:
    client = pydgraph.DgraphClient(stub1, stub2)
```

Note: `client` should be used inside the "`with-as`" block. The resources related to `client` will
be automatically released outside the block and `client` is not usable any more.

### Setting Metadata Headers

Metadata headers such as authentication tokens can be set through the metadata of gRPC methods.
Below is an example of how to set a header named "auth-token".

```python3
# The following piece of code shows how one can set metadata with
# auth-token, to allow Alter operation, if the server requires it.
# metadata is a list of arbitrary key-value pairs.
metadata = [("auth-token", "the-auth-token-value")]
dg.alter(op, metadata=metadata)
```

### Setting a timeout

A timeout value representing the number of seconds can be passed to the `login`, `alter`, `query`,
and `mutate` methods using the `timeout` keyword argument.

For example, the following alters the schema with a timeout of ten seconds:
`dg.alter(op, timeout=10)`

### Async methods

The `alter` method in the client has an asynchronous version called `async_alter`. The async methods
return a future. You can directly call the `result` method on the future. However. The DgraphClient
class provides a static method `handle_alter_future` to handle any possible exception.

```python3
alter_future = self.client.async_alter(pydgraph.Operation(schema="name: string @index(term) ."))
response = pydgraph.DgraphClient.handle_alter_future(alter_future)
```

The `query` and `mutate` methods int the `Txn` class also have async versions called `async_query`
and `async_mutation` respectively. These functions work just like `async_alter`.

You can use the `handle_query_future` and `handle_mutate_future` static methods in the `Txn` class
to retrieve the result. A short example is given below:

```python3
txn = client.txn()
query = "query body here"
future = txn.async_query()
response = pydgraph.Txn.handle_query_future(future)
```

Keep in mind that due to the nature of async calls, the async functions cannot retry the request if
the login is invalid. You will have to check for this error and retry the login (with the function
`retry_login` in both the `Txn` and `Client` classes). A short example is given below:

```python3
client = DgraphClient(client_stubs) # client_stubs is a list of gRPC stubs.
alter_future = client.async_alter()
try:
    response = alter_future.result()
except Exception as e:
  # You can use this function in the util package to check for JWT
    # expired errors.
    if pydgraph.util.is_jwt_expired(e):
        # retry your request here.
```

### Native Async/Await Client

pydgraph provides a native async/await client using Python's `asyncio` library and `grpc.aio`. This
provides true asynchronous operations with better concurrency compared to the futures-based approach
above.

#### Basic Usage

```python
import asyncio
import pydgraph

async def main():
    # Create async client
    client_stub = pydgraph.AsyncDgraphClientStub('localhost:9080')
    client = pydgraph.AsyncDgraphClient(client_stub)

    try:
        # Login
        await client.login("groot", "password")

        # Alter schema
        await client.alter(pydgraph.Operation(
            schema="name: string @index(term) ."
        ))

        # Run mutation
        txn = client.txn()
        response = await txn.mutate(
            set_obj={"name": "Alice"},
            commit_now=True
        )

        # Run query
        query = '{ me(func: has(name)) { name } }'
        txn = client.txn(read_only=True)
        response = await txn.query(query)
        print(response.json)

    finally:
        await client.close()

asyncio.run(main())
```

#### Using Connection Strings

The async client supports the same connection string format as the sync client:

```python
import asyncio
import pydgraph

async def main():
    # Using async_open with connection string
    async with await pydgraph.async_open(
        "dgraph://groot:password@localhost:9080"
    ) as client:
        version = await client.check_version()
        print(f"Connected to Dgraph version: {version}")

asyncio.run(main())
```

#### Using Context Managers

Both the async client and transactions support async context managers for automatic resource
cleanup:

```python
import asyncio
import pydgraph

async def main():
    # Client auto-closes on exit
    async with await pydgraph.async_open("dgraph://localhost:9080") as client:
        await client.login("groot", "password")

        # Transaction auto-discards on exit
        async with client.txn() as txn:
            response = await txn.query('{ me(func: has(name)) { name } }')
            print(response.json)

asyncio.run(main())
```

#### Concurrent Operations

The async client excels at running many operations concurrently:

```python
import asyncio
import pydgraph

async def run_query(client, name):
    """Run a single query"""
    query = f'{{ me(func: eq(name, "{name}")) {{ name }} }}'
    txn = client.txn(read_only=True)
    return await txn.query(query)

async def main():
    async with await pydgraph.async_open("dgraph://localhost:9080") as client:
        await client.login("groot", "password")

        # Run 100 queries concurrently
        names = [f"User{i}" for i in range(100)]
        tasks = [run_query(client, name) for name in names]
        results = await asyncio.gather(*tasks)

        print(f"Completed {len(results)} queries concurrently")

asyncio.run(main())
```

#### JWT Refresh

The async client automatically handles JWT token refresh, just like the sync client:

```python
async with await pydgraph.async_open("dgraph://groot:password@localhost:9080") as client:
    # JWT will be automatically refreshed if it expires during operations
    response = await client.alter(pydgraph.Operation(schema="name: string ."))
```

#### Error Handling

Error handling works the same as the sync client:

```python
import pydgraph

async def main():
    async with await pydgraph.async_open("dgraph://localhost:9080") as client:
        try:
            await client.login("groot", "wrong_password")
        except Exception as e:
            print(f"Login failed: {e}")

        try:
            txn = client.txn(read_only=True)
            await txn.mutate(set_obj={"name": "Alice"})
        except pydgraph.errors.TransactionError as e:
            print(f"Cannot mutate in read-only transaction: {e}")

asyncio.run(main())
```

#### Differences from Sync Client

| Feature             | Sync Client                 | Async Client                      |
| ------------------- | --------------------------- | --------------------------------- |
| Import              | `pydgraph.DgraphClient`     | `pydgraph.AsyncDgraphClient`      |
| Connection function | `pydgraph.open()`           | `await pydgraph.async_open()`     |
| Method calls        | `client.query()`            | `await client.query()`            |
| Context manager     | `with client.txn() as txn:` | `async with client.txn() as txn:` |
| Concurrency         | Threading                   | Native asyncio                    |
| JWT refresh         | Automatic                   | Automatic                         |

## Examples

[tls]: ./examples/tls
[parse_datetime]: ./examples/parse_datetime

- [simple][]: Quickstart example of using pydgraph.
- [tls][]: Quickstart example that uses TLS.
- [parse_datetime]: Demonstration of converting Dgraph's DateTime strings to native python datetime.

## Development

### Setting up environment

There are many ways to set up your local Python environment. We suggest some sane defaults here.

- Most recent versions of Python (3.9+) should work for using pydgraph
- Python 3.13+ is required for project development
- The canonical Python version for development is located in `.python-version`
- Running `make setup` will automatically configure the project with uv and the correct Python
  version. It will install uv if not already available, create and configure a virtualenv, and sync
  all project dependencies.

### Build from source

To ensure the project is set up correctly, run:

```sh
make setup
```

To sync the project virtual environment Python dependencies after making changes:

```sh
make sync
```

#### Regenerating protobufs

If you have made changes to the `pydgraph/proto/api.proto` file, you need to regenerate the source
files generated by Protocol Buffer tools. Run:

```sh
make protogen
```

Or directly with uv:

```sh
uv run python scripts/protogen.py
```

**Important**: This project uses Python 3.13+ with grpcio-tools 1.66.2+ as the canonical development
environment. The generated proto files include mypy type stubs for better type checking. The script
will verify you have the correct Python and grpcio-tools versions before generating files.

#### grpcio 1.65.0 is the minimum version

Older grpcio versions have practical limitations:

- **Compilation failures**: grpcio versions older than ~1.60.0 fail to compile from source on modern
  systems (macOS with recent Xcode, newer Linux distributions) due to C++ compiler compatibility
  issues and outdated build configurations.
- **No pre-built wheels**: PyPI doesn't provide pre-built wheels for very old grpcio versions on
  modern Python versions (3.11+), forcing compilation from source.
- **Build tool incompatibility**: The build process for older grpcio versions uses deprecated
  compiler flags and build patterns that modern toolchains reject.

### Running tests

To run the tests in your local machine, run:

```bash
bash scripts/local-test.sh
```

You can run a specific test suite:

```bash
bash scripts/local-test.sh -v tests/test_connect.py::TestOpen
```

or an individual test:

```bash
bash scripts/local-test.sh -v tests/test_connect.py::TestOpen::test_connection_with_auth
```

The test script requires that `docker` and `docker compose` are installed on your machine.

The script will take care of bringing up a Dgraph cluster and bringing it down after the tests are
executed. The script connects to randomly selected ports for HTTP and gRPC requests to prevent
interference with clusters running on the default port. Docker and docker-compose need to be
installed before running the script. Refer to the official
[Docker documentation](https://docs.docker.com/) for instructions on how to install those packages.
