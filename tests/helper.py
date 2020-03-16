# Copyright 2018 Dgraph Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities used by tests."""

__author__ = 'Garvit Pahal <garvit@dgraph.io>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'

import time
import unittest

import pydgraph


SERVER_ADDR = 'localhost:9180'


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

    TEST_SERVER_ADDR = SERVER_ADDR

    def setUp(self):
        """Sets up the client."""

        self.client = create_client(self.TEST_SERVER_ADDR)
        while True:
            try:
                self.client.login("groot", "password")
                break
            except Exception as e:
                if not "user not found" in str(e):
                    raise e
            time.sleep(0.1)
