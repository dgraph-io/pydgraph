#
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

"""
This module contains the main user-facing methods for interacting with the
Dgraph server over gRPC.
"""
import grpc
from grpc.beta import implementations
from pydgraph.utils import loader
from pydgraph.utils.meta import VERSION

__author__ = 'Mohit Ranka <mohitranka@gmail.com>'
__maintainer__ = 'Mohit Ranka <mohitranka@gmail.com>'
__version__ = VERSION
__status__ = 'development'


class DgraphClient(object):
    def __init__(self, host, port):
        self.channel = grpc.insecure_channel("{host}:{port}".format(host=host, port=port))
        self.stub = loader.BetaDgraphStub(self.channel)

    def query(self, q, timeout=None):
        request = loader.Request(query=q)
        response = self.stub.Query(request, timeout)
        return response

    async def aQuery(self, q, timeout=None):
        request = loader.Request(query=q)
        return await self.stub.aQuery(request, timeout)
