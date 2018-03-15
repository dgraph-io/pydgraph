# Copyright 2016 DGraph Labs, Inc.
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
"""This module contains the main user-facing methods for interacting with the
Dgraph server over gRPC.
"""

import grpc
import random
from pydgraph import txn
from pydgraph import util
from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api
from pydgraph.proto import api_pb2_grpc as api_grpc

__author__ = 'Mohit Ranka <mohitranka@gmail.com>'
__maintainer__ = 'Mohit Ranka <mohitranka@gmail.com>'
__version__ = VERSION
__status__ = 'development'


class DgraphClient(object):
    def __init__(self, *clients):
        if len(clients) == 0:
            raise ValueError('no clients provided in DgraphClient constructor')

        self._clients = [*clients]
        self._lin_read = api.LinRead()

    def alter(self, schema, timeout=None):
        """Alter schema at the other end of the connection."""
        operation = api.Operation(schema=schema)
        return self.any_client().alter(operation, timeout=timeout)

    async def aalter(self, schema, timeout=None):
        operation = api.Operation(schema=schema)
        return await self.any_client().alter_future(operation, timeout=timeout)

    def txn(self):
        return txn.DgraphTxn(self)

    def merge_context(self, context):
        """Merges txn_context into client's state."""
        util.merge_lin_reads(self._lin_read, context.lin_read)

    def any_client(self):
        return random.choice(self._clients)
