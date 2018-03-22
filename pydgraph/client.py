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

import grpc
import random
import json

from pydgraph import txn, util
from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api, api_pb2_grpc as api_grpc

__author__ = 'Mohit Ranka <mohitranka@gmail.com>'
__maintainer__ = 'Garvit Pahal <garvit@dgraph.io>'
__version__ = VERSION
__status__ = 'development'


class DgraphClient(object):
    """Creates a new Client for interacting with the Dgraph store.
    
    The client can be backed by multiple connections (to the same server, or
    multiple servers in a cluster).
    """

    def __init__(self, *clients):
        if len(clients) == 0:
            raise ValueError('No clients provided in DgraphClient constructor')

        self._clients = clients[:]
        self._lin_read = api.LinRead()

    def alter(self, op, timeout=None, metadata=None, credentials=None):
        return self.any_client().alter(op, timeout=timeout, metadata=metadata, credentials=credentials)

    async def async_alter(self, op, timeout=None, metadata=None, credentials=None):
        return await self.any_client().async_alter(op, timeout=timeout, metadata=metadata, credentials=credentials)

    def txn(self):
        return txn.Txn(self)

    def get_lin_read(self):
        lr = api.LinRead()
        ids = lr.ids
        for key, value in ids.items():
            ids[key] = value
        
        return lr

    def merge_lin_reads(self, src):
        util.merge_lin_reads(self._lin_read, src)

    def any_client(self):
        return random.choice(self._clients)
