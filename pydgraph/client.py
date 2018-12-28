# Copyright 2016 Dgraph Labs, Inc.
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

"""Dgraph python client."""

import random

from pydgraph import txn, util
from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api

__author__ = 'Mohit Ranka <mohitranka@gmail.com>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'
__version__ = VERSION
__status__ = 'development'


class DgraphClient(object):
    """Creates a new Client for interacting with the Dgraph store.

    The client can be backed by multiple connections (to the same server, or
    multiple servers in a cluster).
    """

    def __init__(self, *clients):
        if not clients:
            raise ValueError('No clients provided in DgraphClient constructor')

        self._clients = clients[:]
        self._lin_read = api.LinRead()

    def alter(self, operation, timeout=None, metadata=None, credentials=None):
        """Runs a modification via this client."""
        return self.any_client().alter(operation, timeout=timeout,
                                       metadata=metadata,
                                       credentials=credentials)

    def query(self, query, variables=None, timeout=None, metadata=None,
              credentials=None):
        """Runs a query via this client."""
        return self.txn().query(query, variables=variables, timeout=timeout,
                                metadata=metadata, credentials=credentials)

    def txn(self):
        """Creates a transaction."""
        return txn.Txn(self)

    def set_lin_read(self, ctx):
        """Sets linread map in ctx to the one from this instace."""
        ctx.lin_read.MergeFrom(self._lin_read)

    def merge_lin_reads(self, src):
        """Merges linread map in this instance with src."""
        util.merge_lin_reads(self._lin_read, src)

    def any_client(self):
        """Returns a random client."""
        return random.choice(self._clients)
