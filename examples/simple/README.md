# Simple Example Project

Simple project demonstrating the use of [pydgraph], the official python client for Dgraph.

[pydgraph]:https://github.com/dgraph-io/pydgraph

## Running

### Start Dgraph

Store the following content in `docker-compose.yml`. Then, run `docker-compose up` to
set up the Dgraph cluster:

```
version: "3.2"
services:
  zero:
    image: dgraph/dgraph:v1.1.0
    restart: on-failure
    command: dgraph zero --my=zero:5080
  server:
    image: dgraph/dgraph:v1.1.0
    ports:
      - 8080:8080
      - 9080:9080
    restart: on-failure
    command: dgraph alpha --my=server:7080 --lru_mb=2048 --zero=zero:5080
```

## Install dependencies

```sh
pip install -r requirements.txt
```

## Run the sample code

```sh
python simple.py
```

You can explore the source code in the `simple.py` file.
