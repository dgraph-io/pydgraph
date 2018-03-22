# Copyright 2018 DGraph Labs, Inc.
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
import grpc

from pydgraph.client_stub import DgraphClientStub
from pydgraph.client import DgraphClient
from pydgraph.proto import api_pb2 as api

def create_lin_read(ids):
    lr = api.LinRead()
    ids = lr.ids
    for key, value in ids:
        ids[key] = value
    
    return lr

def are_lin_reads_equal(a, b):
    aIds = a.ids
    bIds = b.ids

    if len(aIds) != len(bIds):
        return False
    
    for key in aIds.items():
        if key not in bIds:
            return False
    
    return True

SERVER_ADDR = 'localhost:9080'

def createClient(addr = SERVER_ADDR):
    return DgraphClient(DgraphClientStub(addr))

def setSchema(client, schema):
    return client.alter(api.Operation(schema=schema))

def dropAll(client):
    return client.alter(api.Operation(drop_all=True))

def setup():
    client = createClient()
    dropAll(client)
    return client


class ClientIntegrationTestCase(unittest.TestCase):
    """Base class for other integration test cases. Provides a client object
    with a connection to the dgraph server and ensures that the server is
    v1.0 or greater.
    """

    TEST_SERVER_ADDR = SERVER_ADDR

    def setUp(self):
        """Sets up the client and verifies the version is compatible."""

        self.client = createClient(self.TEST_SERVER_ADDR)
        version = self.client.any_client().check_version(api.Check());

        # version.tag string format is v<MAJOR>.<MINOR>.<PATCH>
        # version_tup = [MAJOR, MINOR, PATCH]
        version_tup = version.tag[1:].split('.')

        version_supported = int(version_tup[0]) > 0
        self.assertTrue(
            version_supported,
            'Dgraph server version must be >= v1.0.0, got %s' % version.tag)
