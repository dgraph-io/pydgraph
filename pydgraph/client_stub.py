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

"""Stub for RPC request."""

import grpc

from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2_grpc as api_grpc

__author__ = 'Garvit Pahal <garvit@dgraph.io>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'
__version__ = VERSION
__status__ = 'development'


class DgraphClientStub(object):
    """Stub for the Dgraph grpc client."""

    def __init__(self, addr='localhost:9080', credentials=None, options=None):
        if credentials is None:
            self.channel = grpc.insecure_channel(addr, options)
        else:
            self.channel = grpc.secure_channel(addr, credentials, options)

        self.stub = api_grpc.DgraphStub(self.channel)

    def login(self, login_req, timeout=None, metadata=None, credentials=None):
        return self.stub.Login(login_req, timeout=timeout, metadata=metadata,
                               credentials=credentials)

    def alter(self, operation, timeout=None, metadata=None, credentials=None):
        """Runs alter operation."""
        return self.stub.Alter(operation, timeout=timeout, metadata=metadata,
                               credentials=credentials)

    def async_alter(self, operation, timeout=None, metadata=None, credentials=None):
        """Async version of alter."""
        return self.stub.Alter.future(operation, timeout=timeout, metadata=metadata,
                                      credentials=credentials)


    def query(self, req, timeout=None, metadata=None, credentials=None):
        """Runs query or mutate operation."""
        return self.stub.Query(req, timeout=timeout, metadata=metadata,
                               credentials=credentials)

    def async_query(self, req, timeout=None, metadata=None, credentials=None):
        """Async version of query."""
        return self.stub.Query.future(req, timeout=timeout, metadata=metadata,
                                      credentials=credentials)

    def commit_or_abort(self, ctx, timeout=None, metadata=None,
                        credentials=None):
        """Runs commit or abort operation."""
        return self.stub.CommitOrAbort(ctx, timeout=timeout, metadata=metadata,
                                       credentials=credentials)

    def check_version(self, check, timeout=None, metadata=None,
                      credentials=None):
        """Returns the version of the Dgraph instance."""
        return self.stub.CheckVersion(check, timeout=timeout,
                                      metadata=metadata,
                                      credentials=credentials)

    def close(self):
        """Deletes channel and stub."""
        try:
            self.channel.close()
        except:
            pass
        del self.channel
        del self.stub
