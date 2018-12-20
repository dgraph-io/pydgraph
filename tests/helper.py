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

__author__ = 'Garvit Pahal <garvit@dgraph.io>'
__maintainer__ = 'Garvit Pahal <garvit@dgraph.io>'

import unittest

import pydgraph


def create_lin_read(src_ids):
    lr = pydgraph.LinRead()
    ids = lr.ids
    for key, value in src_ids.items():
        ids[key] = value

    return lr


def are_lin_reads_equal(a, b):
    a_ids = a.ids
    b_ids = b.ids

    if len(a_ids) != len(b_ids):
        return False

    for (key, value) in a_ids.items():
        if key not in b_ids or b.ids[key] != value:
            return False

    return True


SERVER_ADDR = 'localhost:9080'


def create_client(addr=SERVER_ADDR):
    return pydgraph.DgraphClient(pydgraph.DgraphClientStub(addr))


def set_schema(c, schema):
    return c.alter(pydgraph.Operation(schema=schema))


def drop_all(c):
    return c.alter(pydgraph.Operation(drop_all=True))


def setup():
    c = create_client()
    drop_all(c)
    return c


class ClientIntegrationTestCase(unittest.TestCase):
    """Base class for other integration test cases. Provides a client object
    with a connection to the dgraph server and ensures that the server is
    v1.0 or greater.
    """

    TEST_SERVER_ADDR = 'localhost:9180'

    def setUp(self):
        """Sets up the client and verifies the version is compatible."""

        self.client = create_client(self.TEST_SERVER_ADDR)
