# Simple Example Project

Simple project demonstrating the use of [pydgraph], the official python client for Dgraph.

[pydgraph]:https://github.com/dgraph-io/pydgraph

## Running

### Start Dgraph

Start by spinning up a Dgraph cluster locally. Run `docker compose up`.
Note that the security flag uses a blanket whitelist. This is for convenience when testing locally.
Do not use this in a production environment.

## Install the Dependencies

```sh
pip install -r requirements.txt
```

## Run the Sample Code

```sh
python simple.py
```

You can explore the source code in the `simple.py` file. Run `docker compose down` to tear down the cluster.
