# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Shared pytest fixtures for pydgraph tests."""

from __future__ import annotations

import asyncio
import gzip
import logging
import os
import re
import shutil
import tempfile
import time
import urllib.request
from collections.abc import AsyncGenerator, Generator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

import pydgraph
from pydgraph import (
    AsyncDgraphClient,
    AsyncDgraphClientStub,
    DgraphClient,
    DgraphClientStub,
)

from .helpers import TEST_SERVER_ADDR

# =============================================================================
# Data Fixture Configuration (fetched on demand for stress/benchmark tests)
# =============================================================================

DATA_FIXTURE_DIR = Path(__file__).parent / "resources"
DATA_FIXTURE_BASE_URL = (
    "https://github.com/dgraph-io/dgraph-benchmarks/raw/refs/heads/main/data/"
)

logger = logging.getLogger(__name__)

# =============================================================================
# Stress Test Configuration
# =============================================================================


@pytest.fixture(scope="session")
def stress_config() -> dict[str, Any]:
    """Configuration for stress tests based on STRESS_TEST_MODE env var.

    Modes:
        quick: Fast sanity check (default) - 20 workers, 50 ops, 10 iterations
        moderate: Meaningful stress test - 200 workers, 500 ops, 100 iterations
        full: Maximum stress test - 2000 workers, 5000 ops, 1000 iterations
    """
    mode = os.environ.get("STRESS_TEST_MODE", "quick")

    if mode == "full":
        return {
            "mode": "full",
            "workers": 2000,
            "ops": 5000,
            "iterations": 1000,
            "load_movies": True,
        }
    if mode == "moderate":
        return {
            "mode": "moderate",
            "workers": 200,
            "ops": 500,
            "iterations": 100,
            "load_movies": True,
        }
    return {
        "mode": "quick",
        "workers": 20,
        "ops": 50,
        "iterations": 10,
        "load_movies": os.environ.get("STRESS_TEST_LOAD_MOVIES", "").lower()
        in ("1", "true"),
    }


# =============================================================================
# Movie Dataset Fixtures
# =============================================================================


def _downloaded_data_fixture_path(name: str) -> Path:
    """Download a data fixture file if it doesn't exist locally."""
    path = DATA_FIXTURE_DIR / name
    if not path.exists() or path.stat().st_size == 0:
        url = DATA_FIXTURE_BASE_URL + name
        path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading %s from %s", name, url)
        urllib.request.urlretrieve(url, path)  # noqa: S310
        logger.info("Downloaded %s (%.1f MB)", name, path.stat().st_size / 1024 / 1024)
    return path


@pytest.fixture(scope="session")
def movies_schema() -> Path:
    """Path to the 1million movie schema file.

    Downloads from dgraph-benchmarks repo if not present locally.
    """
    return _downloaded_data_fixture_path("1million.schema")


@pytest.fixture(scope="session")
def movies_rdf_gz() -> Path:
    """Path to the compressed 1million movie RDF data file.

    Downloads from dgraph-benchmarks repo if not present locally.
    """
    return _downloaded_data_fixture_path("1million.rdf.gz")


@pytest.fixture(scope="session")
def movies_rdf(movies_rdf_gz: Path) -> Generator[Path, None, None]:
    """Path to the uncompressed 1million movie RDF data file.

    Decompresses the gzipped RDF file to a temporary directory that is
    automatically cleaned up at the end of the test session.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        output_path = Path(tempdir) / "1million.rdf"
        logger.info("Decompressing %s to %s", movies_rdf_gz, output_path)
        with gzip.open(movies_rdf_gz, "rb") as f_in, open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        logger.info(
            "Decompressed RDF file: %.1f MB", output_path.stat().st_size / 1024 / 1024
        )
        yield output_path


@pytest.fixture(scope="session")
def session_sync_client() -> Generator[DgraphClient, None, None]:
    """Session-scoped sync client with login."""
    client_stub = DgraphClientStub(TEST_SERVER_ADDR)
    client = DgraphClient(client_stub)

    for _ in range(30):
        try:
            client.login("groot", "password")
            break
        except Exception as e:
            if "user not found" in str(e):
                raise
            time.sleep(0.1)

    yield client
    client.close()


@pytest.fixture(scope="session")
def movies_data_loaded(
    request: pytest.FixtureRequest,
    stress_config: dict[str, Any],
) -> bool:
    """Load the 1million movie dataset into Dgraph if load_movies is True.

    Uses lazy fixture evaluation - only requests movies_rdf and client fixtures
    when load_movies is True, avoiding unnecessary downloads in quick mode.

    Returns True if data was loaded, False otherwise.
    """
    if not stress_config["load_movies"]:
        logger.info("Skipping movie data loading (load_movies=False)")
        return False

    # Lazy evaluation: only instantiate session-scoped fixtures when actually needed
    client: DgraphClient = request.getfixturevalue("session_sync_client")
    movies_rdf_path: Path = request.getfixturevalue("movies_rdf")
    schema_content: str = request.getfixturevalue("movies_schema_content")

    # Apply schema before loading data
    client.alter(pydgraph.Operation(drop_all=True))
    client.alter(pydgraph.Operation(schema=schema_content))

    # Pattern to convert explicit UIDs and UUIDs to blank nodes
    # Matches: <12345> (numeric UIDs) and <24d9530f-553a-43fc-8eb6-14ac667b2387> (UUIDs)
    # These formats can't be directly used as Dgraph UIDs
    uid_pattern = re.compile(
        r"<(\d+|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})>",
        re.IGNORECASE,
    )

    def convert_uids_to_blank_nodes(line: str) -> str:
        """Convert <12345> or <uuid> to _:identifier so Dgraph assigns new UIDs."""
        return uid_pattern.sub(r"_:\1", line)

    # Load RDF data in batches
    batch_size = 10000
    batch: list[str] = []
    total_loaded = 0

    logger.info("Loading RDF data from %s", movies_rdf_path)
    with open(movies_rdf_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                # Convert UIDs to blank nodes
                line = convert_uids_to_blank_nodes(line)
                batch.append(line)

            if len(batch) >= batch_size:
                nquads = "\n".join(batch)
                txn = client.txn()
                txn.mutate(set_nquads=nquads, commit_now=True)
                total_loaded += len(batch)
                if total_loaded % 100000 == 0:
                    logger.info("Loaded %d RDF triples", total_loaded)
                batch = []

        # Load remaining batch
        if batch:
            nquads = "\n".join(batch)
            txn = client.txn()
            txn.mutate(set_nquads=nquads, commit_now=True)
            total_loaded += len(batch)

    logger.info("Finished loading %d RDF triples", total_loaded)
    return True


# =============================================================================
# Executor Fixture (for sync stress tests)
# =============================================================================


@pytest.fixture
def executor(
    stress_config: dict[str, Any],
) -> Generator[ThreadPoolExecutor, None, None]:
    """Create ThreadPoolExecutor for concurrent stress tests."""
    workers = stress_config["workers"]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        yield ex


# =============================================================================
# Sync Client Fixtures
# =============================================================================


@pytest.fixture
def sync_client() -> Generator[DgraphClient, None, None]:
    """Sync client with login."""
    client_stub = DgraphClientStub(TEST_SERVER_ADDR)
    client = DgraphClient(client_stub)

    for _ in range(30):
        try:
            client.login("groot", "password")
            break
        except Exception as e:
            if "user not found" in str(e):
                raise
            time.sleep(0.1)

    yield client
    client.close()


@pytest.fixture
def sync_client_clean(sync_client: DgraphClient) -> DgraphClient:
    """Sync client with clean database."""
    sync_client.alter(pydgraph.Operation(drop_all=True))
    return sync_client


@pytest.fixture(scope="session")
def movies_schema_content(movies_schema: Path) -> str:
    """Return the movies schema content as a string."""
    return movies_schema.read_text()


@pytest.fixture
def sync_client_with_movies_schema(
    sync_client_clean: DgraphClient, movies_schema_content: str
) -> DgraphClient:
    """Sync client with movies test schema."""
    sync_client_clean.alter(pydgraph.Operation(schema=movies_schema_content))
    return sync_client_clean


# =============================================================================
# Async Client Fixtures
# =============================================================================


@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncDgraphClient, None]:
    """Async client with login."""
    client_stub = AsyncDgraphClientStub(TEST_SERVER_ADDR)
    client = AsyncDgraphClient(client_stub)

    for _ in range(30):
        try:
            await client.login("groot", "password")
            break
        except Exception as e:
            if "user not found" in str(e):
                raise
            await asyncio.sleep(0.1)

    yield client
    await client.close()


@pytest.fixture
async def async_client_clean(async_client: AsyncDgraphClient) -> AsyncDgraphClient:
    """Async client with clean database."""
    await async_client.alter(pydgraph.Operation(drop_all=True))
    return async_client


@pytest.fixture
async def async_client_with_movies_schema(
    async_client_clean: AsyncDgraphClient,
    movies_schema_content: str,
) -> AsyncDgraphClient:
    """Async client with movies test schema."""
    await async_client_clean.alter(pydgraph.Operation(schema=movies_schema_content))
    return async_client_clean


# =============================================================================
# Sync-wrapped Async Client Fixtures (for benchmark tests)
# =============================================================================
# These fixtures create async clients using their own event loop, avoiding
# conflicts with pytest-asyncio's event loop when using benchmark fixtures.
# The event loop is stored alongside the client so it can be reused.


@pytest.fixture
def async_client_with_movies_schema_for_benchmark(
    movies_schema_content: str,
) -> Generator[tuple[AsyncDgraphClient, asyncio.AbstractEventLoop], None, None]:
    """Async client with schema and its event loop for benchmarking.

    Returns a tuple of (client, loop) so tests can run async operations
    using the same event loop the client was created with.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    schema_content = movies_schema_content  # Capture for closure

    async def setup() -> AsyncDgraphClient:
        client_stub = AsyncDgraphClientStub(TEST_SERVER_ADDR)
        client = AsyncDgraphClient(client_stub)
        for _ in range(30):
            try:
                await client.login("groot", "password")
                break
            except Exception as e:
                if "user not found" in str(e):
                    raise
                await asyncio.sleep(0.1)
        await client.alter(pydgraph.Operation(drop_all=True))
        await client.alter(pydgraph.Operation(schema=schema_content))
        return client

    client = loop.run_until_complete(setup())
    yield (client, loop)
    loop.run_until_complete(client.close())
    loop.close()
