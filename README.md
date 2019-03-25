# pydgraph [![Build Status](https://img.shields.io/travis/dgraph-io/pydgraph/master.svg?style=flat)](https://travis-ci.org/dgraph-io/pydgraph) [![Coverage Status](https://img.shields.io/coveralls/github/dgraph-io/pydgraph/master.svg?style=flat)](https://coveralls.io/github/dgraph-io/pydgraph?branch=master)

Official Dgraph client implementation for Python (Python >= v2.7 and >= v3.5),
using [grpc].

[grpc]: https://grpc.io/

This client follows the [Dgraph Go client][goclient] closely.

[goclient]: https://github.com/dgraph-io/dgo

Before using this client, we highly recommend that you go through [docs.dgraph.io],
and understand how to run and work with Dgraph.

[docs.dgraph.io]:https://docs.dgraph.io

## Table of contents

- [Install](#install)
- [Quickstart](#quickstart)
- [Using a client](#using-a-client)
  - [Create a client](#create-a-client)
  - [Alter the database](#alter-the-database)
  - [Create a transaction](#create-a-transaction)
  - [Run a mutation](#run-a-mutation)
  - [Run a query](#run-a-query)
  - [Commit a transaction](#commit-a-transaction)
  - [Cleanup Resources](#cleanup-resources)
  - [Setting Metadata Headers](#setting-metadata-headers)
- [Examples](#examples)
- [Development](#development)
  - [Building the source](#building-the-source)
  - [Running tests](#running-tests)

## Install

Install using pip:

```sh
pip install pydgraph
```

## Quickstart

Build and run the [simple][] project in the `examples` folder, which
contains an end-to-end example of using the Dgraph python client. Follow the
instructions in the README of that project.

[simple]: ./examples/simple

## Using a client

### Create a client

A `DgraphClient` object can be initialised by passing it a list of
`DgraphClientStub` clients as variadic arguments. Connecting to multiple Dgraph
servers in the same cluster allows for better distribution of workload.

The following code snippet shows just one connection.

```python
import pydgraph

client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)
```

### Alter the database

To set the schema, create an `Operation` object, set the schema and pass it to
`DgraphClient#alter(Operation)` method.

```python
schema = 'name: string @index(exact) .'
op = pydgraph.Operation(schema=schema)
client.alter(op)
```

`Operation` contains other fields as well, including drop predicate and drop all.
Drop all is useful if you wish to discard all the data, and start from a clean
slate, without bringing the instance down.

```python
# Drop all data including schema from the Dgraph instance. This is useful
# for small examples such as this, since it puts Dgraph into a clean
# state.
op = pydgraph.Operation(drop_all=True)
client.alter(op)
```

### Create a transaction

To create a transaction, call `DgraphClient#txn()` method, which returns a
new `Txn` object. This operation incurs no network overhead.

It is good practice to call `Txn#discard()` in a `finally` block after running
the transaction. Calling `Txn#discard()` after `Txn#commit()` is a no-op
and you can call `Txn#discard()` multiple times with no additional side-effects.

```python
txn = client.txn()
try:
  # Do something here
  # ...
finally:
  txn.discard()
  # ...
```

To create a read-only transaction, call `DgraphClient#txn(read_only=True)`.
Read-only transactions are ideal for transactions which only involve queries.
Mutations and commits are not allowed.

```python
txn = client.txn(read_only=True)
try:
  # Do some queries here
  # ...
finally:
  txn.discard()
  # ...
```

`client.query()` uses a read-only transaction to execute the query.

To create a read-only transaction that executes best-effort queries, call
`DgraphClient#txn(read_only=True, best_effort=True)`. Best-effort queries are
faster than normal queries because they bypass the normal consensus protocol.
For this same reason, best-effort queries cannot guarantee to return the latest
data. Best-effort queries are only supported by read-only transactions.

### Run a mutation

`Txn#mutate(mu=Mutation)` runs a mutation. It takes in a `Mutation` object,
which provides two main ways to set data: JSON and RDF N-Quad. You can choose
whichever way is convenient. Most users won't need to create a `Mutation`
object themselves.

`Txn#mutate()` provides convenience keyword arguments `set_obj` and `del_obj`
for setting JSON values and `set_nquads` and `del_nquads` for setting N-Quad
values. See examples below for usage.

We define a person object to represent a person and use it in a transaction.

```python
# Create data.
p = {
    'name': 'Alice',
}

# Run mutation.
txn.mutate(set_obj=p)

# If you want to use a mutation object, use this instead:
# mu = pydgraph.Mutation(set_json=json.dumps(p).encode('utf8'))
# txn.mutate(mu)

# If you want to use N-Quads, use this instead:
# txn.mutate(set_nquads='_:alice <name> "Alice" .')
```

```python
# Delete data.

query1 = """query all($a: string)
 {
   all(func: eq(name, $a)) 
    {
      uid
    }   
  }"""
  
variables1 = {'$a': 'Bob'}

res1 = client.query(query1, variables=variables1)

ppl1 = json.loads(res1.json)

#For mutation to delete node, use this: 
txn.mutate(del_obj= person)
```

For a more complete example with multiple fields and relationships, look at the
[simple] project in the `examples` folder.

Sometimes, you only want to commit a mutation, without querying anything further.
In such cases, you can set the keyword argument `commit_now=True` to indicate
that the mutation must be immediately committed.

Keyword argument `ignore_index_conflict=True` can be used to not run conflict
detection over the index, which would decrease the number of transaction
conflicts and aborts. However, this would come at the cost of potentially
inconsistent upsert operations.

### Run a query

You can run a query by calling `Txn#query(string)`. You will need to pass in a
GraphQL+- query string. If you want to pass an additional dictionary of any
variables that you might want to set in the query, call
`Txn#query(string, variables=d)` with the variables dictionary `d`.

The response would contain the field `json`, which returns the response
JSON.

Let’s run the following query with a variable $a:

```console
query all($a: string) {
  all(func: eq(name, $a))
  {
    name
  }
}
```

Run the query, deserialize the result from JSON and print it out:

```python
# Run query.
query = """query all($a: string) {
  all(func: eq(name, $a))
  {
    name
  }
}"""
variables = {'$a': 'Alice'}

res = client.txn().query(query, variables=variables)
# If not doing a mutation in the same transaction, simply use:
# res = client.query(query, variables=variables)

ppl = json.loads(res.json);

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

### Commit a transaction

A transaction can be committed using the `Txn#commit()` method. If your transaction
consisted solely of calls to `Txn#query` or `Txn#queryWithVars`, and no calls to
`Txn#mutate`, then calling `Txn#commit()` is not necessary.

An error will be raised if other transactions running concurrently modify the same
data that was modified in this transaction. It is up to the user to retry
transactions when they fail.

```python
txn = client.txn();
try:
  # ...
  # Perform any number of queries and mutations
  # ...
  # and finally...
  txn.commit()
except Exception as e:
  if isinstance(e, pydgraph.AbortedError):
    # Retry or handle exception.
  else:
    raise e
finally:
  # Clean up. Calling this after txn.commit() is a no-op
  # and hence safe.
  txn.discard()
```

### Cleanup Resources

To cleanup resources, you have to call `DgraphClientStub#close()` individually for
all the instances of `DgraphClientStub`.

```python
SERVER_ADDR = "localhost:9080"

# Create instances of DgraphClientStub.
stub1 = pydgraph.DgraphClientStub(SERVER_ADDR)
stub2 = pydgraph.DgraphClientStub(SERVER_ADDR)

# Create an instance of DgraphClient.
client = pydgraph.DgraphClient(stub1, stub2)

# ...
# Use client
# ...

# Cleanup resources by closing all client stubs.
stub1.close()
stub2.close()
```

### Setting Metadata Headers
Metadata headers such as authentication tokens can be set through the metadata of gRPC methods. Below is an example of how to set a header named "auth-token".
```python
# The following piece of code shows how one can set metadata with
# auth-token, to allow Alter operation, if the server requires it.
# metadata is a list of arbritary key-value pairs.
metadata = [("auth-token", "the-auth-token-value")]
dg.alter(op, metadata=metadata)
```

## Examples

- [simple][]: Quickstart example of using pydgraph.
- [tls][]: Example of using pydgraph with a Dgraph cluster secured with TLS.

[tls]: ./examples/tls

## Development

### Building the source

```sh
python setup.py install
# To install for the current user, use this instead:
# python setup.py install --user
```

If you have made changes to the `pydgraph/proto/api.proto` file, you need need
to regenerate the source files generated by Protocol Buffer tools. To do that,
install the [grpcio-tools][grpcio-tools] library and then run the following
command:

[grpcio-tools]: https://pypi.python.org/pypi/grpcio-tools

```sh
python scripts/protogen.py
```

### Running tests

To run the tests in your local machine you can run the script
`scripts/local-tests.sh`. This script assumes Dgraph and dgo (Go client) are
already built on the local machine. The script will take care of bringing up a
Dgraph cluster and bringing it down after the tests are executed. The script
uses the port 9180 by default to prevent interference with clusters running on
the default port. Docker and docker-compose need to be installed before running
the script. Refer to the official Docker documentation for instructions on how
to install those packages.

The `test.sh` script downloads and installs Dgraph. It is meant for use by our
CI systems and using it for local development is not recommended.
