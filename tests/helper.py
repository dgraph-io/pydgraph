# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Utilities used by tests."""

__author__ = "Garvit Pahal"
__maintainer__ = "Dgraph Labs <contact@dgraph.io>"

import os
import time
import unittest

import pydgraph

SERVER_ADDR = "localhost:9180"


def create_client(addr=SERVER_ADDR):
    """Creates a new client object using the given address."""
    return pydgraph.DgraphClient(pydgraph.DgraphClientStub(addr))


def set_schema(client, schema):
    """Sets the schema in the given client."""
    return client.alter(pydgraph.Operation(schema=schema))


def drop_all(client):
    """Drops all data in the given client."""
    return client.alter(pydgraph.Operation(drop_all=True))


def setup():
    """Creates a new client and drops all existing data."""
    client = create_client()
    drop_all(client)
    return client


class ClientIntegrationTestCase(unittest.TestCase):
    """Base class for other integration test cases. Provides a client object
    with a connection to the dgraph server.
    """

    TEST_SERVER_ADDR = os.getenv("TEST_SERVER_ADDR", SERVER_ADDR)

    def setUp(self):
        """Sets up the client."""

        self.client = create_client(self.TEST_SERVER_ADDR)
        while True:
            try:
                self.client.login("groot", "password")
                break
            except Exception as e:
                if "user not found" not in str(e):
                    raise e
            time.sleep(0.1)
