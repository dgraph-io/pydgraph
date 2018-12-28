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

import unittest

import pydgraph


def create_lin_read(src_ids):
    """Creates a linread map using src_ids."""
    lin_read = pydgraph.LinRead()
    ids = lin_read.ids
    for key, value in src_ids.items():
        ids[key] = value

    return lin_read


def are_lin_reads_equal(lin_read1, lin_read2):
    """Returns True if both linread maps are equal."""
    ids1 = lin_read1.ids
    ids2 = lin_read2.ids

    if len(ids1) != len(ids2):
        return False

    for (key, value) in ids1.items():
        if key not in ids2 or lin_read2.ids[key] != value:
            return False

    return True


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
