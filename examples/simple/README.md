# Simple Example Project

Simple project demonstrating the use of [pydgraph], the official python client for Dgraph.

[pydgraph]: https://github.com/dgraph-io/pydgraph

## Setup

### Install uv

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Install it with:

**macOS/Linux:**

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**Using pip:**

```sh
pip install uv
```

For more installation options, visit: https://docs.astral.sh/uv/getting-started/installation/

### Install Dependencies

```sh
uv sync
```

This will create a virtual environment and install pydgraph and its dependencies.

## Running

### Start Dgraph

Start by spinning up a Dgraph cluster locally. Run `docker compose up`. Note that the security flag
uses a blanket whitelist. This is for convenience when testing locally. Do not use this in a
production environment.

### Run the Sample Code

```sh
uv run python simple.py
```

You can explore the source code in the `simple.py` file. Run `docker compose down` to tear down the
cluster.
