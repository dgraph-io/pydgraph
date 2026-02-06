# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Shared pytest fixtures for pydgraph tests."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import AsyncGenerator, Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
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
# Stress Test Configuration
# =============================================================================


@pytest.fixture(scope="session")
def stress_config() -> dict[str, Any]:
    """Configuration for stress tests based on STRESS_TEST_MODE env var."""
    mode = os.environ.get("STRESS_TEST_MODE", "quick")

    if mode == "full":
        return {
            "mode": "full",
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
# Executor Fixtures (for sync stress tests)
# =============================================================================


@pytest.fixture(params=["thread", "process"])
def executor_type(request: pytest.FixtureRequest) -> str:
    """Parametrize tests to run with both executor types."""
    return request.param


@pytest.fixture
def executor(
    executor_type: str, stress_config: dict[str, Any]
) -> Generator[ThreadPoolExecutor | ProcessPoolExecutor, None, None]:
    """Create executor based on parametrization."""
    workers = stress_config["workers"]
    if executor_type == "thread":
        with ThreadPoolExecutor(max_workers=workers) as ex:
            yield ex
    else:
        with ProcessPoolExecutor(max_workers=workers) as ex:
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
