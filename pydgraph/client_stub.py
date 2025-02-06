# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Stub for RPC request."""

import grpc

from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2_grpc as api_grpc

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

__author__ = "Garvit Pahal"
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"
__version__ = VERSION
__status__ = "development"


class DgraphClientStub(object):
    """Stub for the Dgraph grpc client."""

    def __init__(self, addr="localhost:9080", credentials=None, options=None):
        if credentials is None:
            self.channel = grpc.insecure_channel(addr, options)
        else:
            self.channel = grpc.secure_channel(addr, credentials, options)

        self.stub = api_grpc.DgraphStub(self.channel)

    def login(self, login_req, timeout=None, metadata=None, credentials=None):
        return self.stub.Login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def alter(self, operation, timeout=None, metadata=None, credentials=None):
        """Runs alter operation."""
        return self.stub.Alter(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def async_alter(self, operation, timeout=None, metadata=None, credentials=None):
        """Async version of alter."""
        return self.stub.Alter.future(
            operation, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def query(self, req, timeout=None, metadata=None, credentials=None):
        """Runs query or mutate operation."""
        return self.stub.Query(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def async_query(self, req, timeout=None, metadata=None, credentials=None):
        """Async version of query."""
        return self.stub.Query.future(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def commit_or_abort(self, ctx, timeout=None, metadata=None, credentials=None):
        """Runs commit or abort operation."""
        return self.stub.CommitOrAbort(
            ctx, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def check_version(self, check, timeout=None, metadata=None, credentials=None):
        """Returns the version of the Dgraph instance."""
        return self.stub.CheckVersion(
            check, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def close(self):
        """Deletes channel and stub."""
        try:
            self.channel.close()
        except:
            pass
        del self.channel
        del self.stub

    @staticmethod
    def parse_host(cloud_endpoint):
        """Converts any cloud endpoint to grpc endpoint"""
        host = cloud_endpoint
        if cloud_endpoint.startswith("http"):  # catch http:// and https://
            host = urlparse(cloud_endpoint).netloc
        host = host.split(":", 1)[0]  # remove port if any
        if ".grpc." not in host:
            url_parts = host.split(".", 1)
            host = url_parts[0] + ".grpc." + url_parts[1]
        return host

    # accepts grpc endpoint as copied in cloud console as well as graphql endpoint
    # Usage:
    # import pydgraph
    # client_stub = pydgraph.DgraphClientStub.from_cloud("cloud_endpoint", "api-key")
    # client = pydgraph.DgraphClient(client_stub)
    @staticmethod
    def from_cloud(cloud_endpoint, api_key, options=None):
        """Returns Dgraph Client stub for the Dgraph Cloud endpoint"""
        host = DgraphClientStub.parse_host(cloud_endpoint)
        creds = grpc.ssl_channel_credentials()
        call_credentials = grpc.metadata_call_credentials(
            lambda context, callback: callback((("authorization", api_key),), None)
        )
        composite_credentials = grpc.composite_channel_credentials(
            creds, call_credentials
        )
        if options is None:
            options = [("grpc.enable_http_proxy", 0)]
        else:
            options.append(("grpc.enable_http_proxy", 0))
        client_stub = DgraphClientStub(
            "{host}:{port}".format(host=host, port="443"),
            composite_credentials,
            options=options,
        )
        return client_stub
