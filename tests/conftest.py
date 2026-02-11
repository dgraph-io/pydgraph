# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Shared pytest fixtures for pydgraph tests."""

from __future__ import annotations

import asyncio
import gzip
import logging
import os
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

from .helpers import SYNTHETIC_SCHEMA, TEST_SERVER_ADDR

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
        with gzip.open(movies_rdf_gz, "rb") as f_in, open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        yield output_path


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


@pytest.fixture
def sync_client_with_schema(sync_client_clean: DgraphClient) -> DgraphClient:
    """Sync client with synthetic test schema."""
    sync_client_clean.alter(pydgraph.Operation(schema=SYNTHETIC_SCHEMA))
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
async def async_client_with_schema(
    async_client_clean: AsyncDgraphClient,
) -> AsyncDgraphClient:
    """Async client with synthetic test schema."""
    await async_client_clean.alter(pydgraph.Operation(schema=SYNTHETIC_SCHEMA))
    return async_client_clean


# =============================================================================
# Sync-wrapped Async Client Fixtures (for benchmark tests)
# =============================================================================
# These fixtures create async clients using their own event loop, avoiding
# conflicts with pytest-asyncio's event loop when using benchmark fixtures.
# The event loop is stored alongside the client so it can be reused.


@pytest.fixture
def async_client_with_schema_for_benchmark() -> Generator[
    tuple[AsyncDgraphClient, asyncio.AbstractEventLoop], None, None
]:
    """Async client with schema and its event loop for benchmarking.

    Returns a tuple of (client, loop) so tests can run async operations
    using the same event loop the client was created with.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

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
        await client.alter(pydgraph.Operation(schema=SYNTHETIC_SCHEMA))
        return client

    client = loop.run_until_complete(setup())
    yield (client, loop)
    loop.run_until_complete(client.close())
    loop.close()
