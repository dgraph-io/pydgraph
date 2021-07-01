# Simple Example Project

Simple project demonstrating the use of [pydgraph], the official python client for Dgraph.

[pydgraph]:https://github.com/dgraph-io/pydgraph

## Running

### Start Dgraph

Store the following content in `docker-compose.yml`. Then, run `docker-compose up` to
set up the Dgraph cluster:

```yml
version: "3.2"
services:
  zero:
    image: dgraph/dgraph:latest
    restart: on-failure
    command: dgraph zero --my=zero:5080
  server:
    image: dgraph/dgraph:latest
    ports:
      - 8080:8080
      - 9080:9080
    restart: on-failure
    command: dgraph alpha --my=server:7080 --zero=zero:5080 --security "whitelist=${WHITELISTED}"
```
The "WHITELISTED" environment variable can be intiialized as described [in this post](https://discuss.dgraph.io/t/suggestion-for-how-to-add-docker-compose-network-to-whitelist/9600). We need to whitelist these IPs because the docker container runs with it's own IP address.

## Install the Dependencies

```sh
pip install -r requirements.txt
```

## Run the Sample Code

```sh
python simple.py
```

You can explore the source code in the `simple.py` file.
