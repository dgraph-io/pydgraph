# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Utilities used by tests."""

from __future__ import annotations

import os
import re
import time
import unittest

from packaging import version

import pydgraph

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

SERVER_ADDR = "localhost:9180"


def create_client(
    addr: str = SERVER_ADDR,
    username: str | None = None,
    password: str | None = None,
    namespace: int | None = None,
) -> pydgraph.DgraphClient:
    """Creates a new client object using the given address.

    If username/password are provided, uses pydgraph.open() with login.
    If namespace is also provided, logs into that namespace.
    """
    if username and password:
        # Build connection string for pydgraph.open()
        host, port = addr.split(":")
        conn_str = f"dgraph://{username}:{password}@{host}:{port}"
        if namespace is not None:
            conn_str += f"?namespace={namespace}"
        return pydgraph.open(conn_str)
    return pydgraph.DgraphClient(pydgraph.DgraphClientStub(addr))


def set_schema(
    client: pydgraph.DgraphClient, schema: str
) -> pydgraph.proto.api_pb2.Payload:
    """Sets the schema in the given client."""
    return client.alter(pydgraph.Operation(schema=schema))


def drop_all(client: pydgraph.DgraphClient) -> pydgraph.proto.api_pb2.Payload:
    """Drops all data in the given client."""
    return client.alter(pydgraph.Operation(drop_all=True))


def setup() -> pydgraph.DgraphClient:
    """Creates a new client and drops all existing data."""
    client = create_client()
    drop_all(client)
    return client


def check_dgraph_version(
    client: pydgraph.DgraphClient, min_version: str
) -> tuple[bool, str | None, str | None]:
    """Check if Dgraph version meets minimum requirement.

    Args:
        client: Dgraph client instance
        min_version: Minimum required version string (e.g., "25.0.0")

    Returns:
        tuple: (is_compatible, actual_version, error_message)
               is_compatible: bool indicating if version requirement is met
               actual_version: string of actual Dgraph version or None
               error_message: string error message or None
    """
    try:
        dgraph_version = client.check_version()
        version_str = dgraph_version.lstrip("v")
        # Extract just the semantic version part (major.minor.patch)
        match = re.match(r"^(\d+\.\d+\.\d+)", version_str)
        if match:
            clean_version = match.group(1)
        else:
            # Fallback: try to parse as-is in case it's already clean
            clean_version = version_str

        is_compatible = version.parse(clean_version) >= version.parse(min_version)
    except Exception as e:
        return False, None, str(e)
    else:
        return is_compatible, dgraph_version, None


def skip_if_dgraph_version_below(
    client: pydgraph.DgraphClient, min_version: str, test_case: unittest.TestCase
) -> None:
    """Skip test if Dgraph version is below minimum requirement.

    Args:
        client: Dgraph client instance
        min_version: Minimum required version string (e.g., "25.0.0")
        test_case: Test case instance to call skipTest on
    """
    is_compatible, actual_version, error_msg = check_dgraph_version(client, min_version)

    if not is_compatible:
        if error_msg:
            test_case.skipTest(f"Could not determine Dgraph version: {error_msg}")
        else:
            test_case.skipTest(
                f"Test requires Dgraph v{min_version}+, found {actual_version}"
            )


class ClientIntegrationTestCase(unittest.TestCase):
    """Base class for other integration test cases. Provides a client object
    with a connection to the dgraph server.
    """

    TEST_SERVER_ADDR = os.getenv("TEST_SERVER_ADDR", SERVER_ADDR)

    def setUp(self) -> None:
        """Sets up the client."""

        self.client = create_client(self.TEST_SERVER_ADDR)
        while True:
            try:
                self.client.login("groot", "password")
                break
            except Exception as e:
                if "user not found" not in str(e):
                    raise
            time.sleep(0.1)
