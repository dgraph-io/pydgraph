# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Dgraph python client."""

import random
import urllib.parse

import grpc

from pydgraph import errors, txn, util
from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api
from pydgraph.proto import api_v2_pb2 as api_v2
from pydgraph.proto.api_v2_pb2 import (
    AllocateIDsRequest,
)
from pydgraph.proto.api_v2_pb2 import AlterRequest as AlterRequestV2
from pydgraph.proto.api_v2_pb2 import (
    CreateNamespaceRequest,
    DropNamespaceRequest,
    ListNamespacesRequest,
    RunDQLRequest,
    SignInUserRequest,
    UpdateExtSnapshotStreamingStateRequest,
    UpdateNamespaceRequest,
)

__author__ = "Mohit Ranka <mohitranka@gmail.com>"
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"
__version__ = VERSION
__status__ = "development"


class DgraphClient(object):
    """Creates a new Client for interacting with the Dgraph store.

    The client can be backed by multiple connections (to the same server, or
    multiple servers in a cluster).
    """

    def __init__(self, *clients):
        if not clients:
            raise ValueError("No clients provided in DgraphClient constructor")

        self._clients = clients[:]
        self._jwt = api.Jwt()
        self._login_metadata = []

    def check_version(self, timeout=None, metadata=None, credentials=None):
        """Returns the version of Dgraph if the server is ready to accept requests."""

        new_metadata = self.add_login_metadata(metadata)
        check_req = api.Check()

        try:
            response = self.any_client().check_version(
                check_req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
            return response.tag
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = self.any_client().check_version(
                    check_req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
                return response.tag
            else:
                raise error

    def login(self, userid, password, timeout=None, metadata=None, credentials=None):
        login_req = api.LoginRequest()
        login_req.userid = userid
        login_req.password = password
        login_req.namespace = 0

        response = self.any_client().login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    def login_into_namespace(
        self, userid, password, namespace, timeout=None, metadata=None, credentials=None
    ):
        login_req = api.LoginRequest()
        login_req.userid = userid
        login_req.password = password
        login_req.namespace = namespace

        response = self.any_client().login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    def retry_login(self, timeout=None, metadata=None, credentials=None):
        if len(self._jwt.refresh_jwt) == 0:
            raise ValueError("refresh jwt should not be empty")

        login_req = api.LoginRequest()
        login_req.refresh_token = self._jwt.refresh_jwt

        response = self.any_client().login(
            login_req, timeout=timeout, metadata=metadata, credentials=credentials
        )
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    def alter(self, operation, timeout=None, metadata=None, credentials=None):
        """Runs a modification via this client."""
        new_metadata = self.add_login_metadata(metadata)

        try:
            return self.any_client().alter(
                operation,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                try:
                    return self.any_client().alter(
                        operation,
                        timeout=timeout,
                        metadata=new_metadata,
                        credentials=credentials,
                    )
                except Exception as error:
                    self._common_except_alter(error)
            else:
                self._common_except_alter(error)

    def ping(self, timeout=None, metadata=None, credentials=None):
        """Runs a ping via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = api_v2.PingRequest()
        try:
            return self.any_client().ping(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().ping(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def allocate_ids(
        self, how_many, lease_type, timeout=None, metadata=None, credentials=None
    ):
        """Runs an AllocateIDs via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = AllocateIDsRequest(how_many=how_many, lease_type=lease_type)
        try:
            return self.any_client().allocate_ids(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().allocate_ids(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def sign_in_user(
        self,
        user_id,
        password,
        refresh_token=None,
        timeout=None,
        metadata=None,
        credentials=None,
    ):
        """Runs a SignInUser via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = SignInUserRequest(
            user_id=user_id, password=password, refresh_token=refresh_token
        )
        try:
            return self.any_client().sign_in_user(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().sign_in_user(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def alter_v2(
        self,
        op,
        ns_name=None,
        schema=None,
        run_in_background=False,
        predicate_to_drop=None,
        type_to_drop=None,
        timeout=None,
        metadata=None,
        credentials=None,
    ):
        """Runs an Alter (v2) via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = AlterRequestV2(
            op=op,
            ns_name=ns_name,
            schema=schema,
            run_in_background=run_in_background,
            predicate_to_drop=predicate_to_drop,
            type_to_drop=type_to_drop,
        )
        try:
            return self.any_client().alter_v2(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().alter_v2(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def run_dql(
        self,
        dql_query,
        ns_name=None,
        vars=None,
        read_only=False,
        best_effort=False,
        resp_format=api_v2.RespFormat.JSON,
        timeout=None,
        metadata=None,
        credentials=None,
    ):
        """Runs a RunDQL via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = RunDQLRequest(
            dql_query=dql_query,
            ns_name=ns_name,
            vars=vars,
            read_only=read_only,
            best_effort=best_effort,
            resp_format=resp_format,
        )
        try:
            return self.any_client().run_dql(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().run_dql(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def create_namespace(self, ns_name, timeout=None, metadata=None, credentials=None):
        """Runs a CreateNamespace via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = CreateNamespaceRequest(ns_name=ns_name)
        try:
            return self.any_client().create_namespace(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().create_namespace(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def drop_namespace(self, ns_name, timeout=None, metadata=None, credentials=None):
        """Runs a DropNamespace via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = DropNamespaceRequest(ns_name=ns_name)
        try:
            return self.any_client().drop_namespace(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().drop_namespace(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def update_namespace(
        self, ns_name, rename_to_ns, timeout=None, metadata=None, credentials=None
    ):
        """Runs an UpdateNamespace via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = UpdateNamespaceRequest(ns_name=ns_name, rename_to_ns=rename_to_ns)
        try:
            return self.any_client().update_namespace(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().update_namespace(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def list_namespaces(self, timeout=None, metadata=None, credentials=None):
        """Runs a ListNamespaces via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = ListNamespacesRequest()
        try:
            return self.any_client().list_namespaces(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().list_namespaces(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def update_ext_snapshot_streaming_state(
        self,
        start=False,
        finish=False,
        drop_data=False,
        timeout=None,
        metadata=None,
        credentials=None,
    ):
        """Runs an UpdateExtSnapshotStreamingState via this client."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        req = UpdateExtSnapshotStreamingStateRequest(
            start=start, finish=finish, drop_data=drop_data
        )
        try:
            return self.any_client().update_ext_snapshot_streaming_state(
                req,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                return self.any_client().update_ext_snapshot_streaming_state(
                    req,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise error

    def stream_ext_snapshot(
        self, request_iterator, timeout=None, metadata=None, credentials=None
    ):
        """Runs a StreamExtSnapshot via this client. This is a client-streaming RPC.
        The caller is responsible for handling JWT expiry and retries for this streaming call.
        """
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        try:
            # The underlying gRPC stub expects an iterator for client-streaming RPCs.
            return self.any_client().stream_ext_snapshot(
                request_iterator,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            # JWT retry logic is not implemented for streaming calls as it's non-trivial.
            # The stream would be broken on error anyway.
            self._common_except_alter(error)

    @staticmethod
    def _common_except_alter(error):
        if util.is_retriable_error(error):
            raise errors.RetriableError(error)

        if util.is_connection_error(error):
            raise errors.ConnectionError(error)

        raise error

    def async_alter(self, operation, timeout=None, metadata=None, credentials=None):
        """The async version of alter."""
        """Note this is only supported on Dgraph v25.0.0 and above."""
        new_metadata = self.add_login_metadata(metadata)
        return self.any_client().async_alter(
            operation, timeout=timeout, metadata=new_metadata, credentials=credentials
        )

    @staticmethod
    def handle_alter_future(future):
        try:
            return future.result()
        except Exception as error:
            DgraphClient._common_except_alter(error)

    def txn(self, read_only=False, best_effort=False):
        """Creates a transaction."""
        return txn.Txn(self, read_only=read_only, best_effort=best_effort)

    def any_client(self):
        """Returns a random gRPC client so that requests are distributed evenly among them."""
        return random.choice(self._clients)  # nosec # pylint: disable=insecure-random

    def add_login_metadata(self, metadata):
        new_metadata = list(self._login_metadata)
        if not metadata:
            return new_metadata
        new_metadata.extend(metadata)
        return new_metadata

    def close(self):
        for client in self._clients:
            client.close()


def open(connection_string: str) -> DgraphClient:
    """Open a new Dgraph client. Use client.close() to close the client.

    Args:
        connection_string: A connection string in the format of "dgraph://<username:password>@<host>:<port>?<params>"

    Returns:
        A new Dgraph client.

    Raises:
        ValueError: If the connection string is invalid.
    """
    try:
        parsed_url = urllib.parse.urlparse(connection_string)
        if not parsed_url.scheme == "dgraph":
            raise ValueError("Invalid connection string: scheme must be 'dgraph'")
        if not parsed_url.hostname:
            raise ValueError("Invalid connection string: hostname required")
        if not parsed_url.port:
            raise ValueError("Invalid connection string: port required")
    except Exception as e:
        raise ValueError(f"Failed to parse connection string: {e}") from e

    host = parsed_url.hostname
    port = parsed_url.port
    username = parsed_url.username
    password = parsed_url.password
    if username and not password:
        raise ValueError(
            "Invalid connection string: password required when username is provided"
        )

    params = dict(urllib.parse.parse_qsl(parsed_url.query))
    credentials = None
    options = None
    auth_header = None

    if "sslmode" in params:
        sslmode = params["sslmode"]
        if sslmode == "disable":
            credentials = None
        elif sslmode == "require":
            raise ValueError(
                "sslmode=require is not supported in pydgraph, use verify-ca"
            )
        elif sslmode == "verify-ca":
            credentials = grpc.ssl_channel_credentials()
        else:
            raise ValueError(f"Invalid sslmode: {sslmode}")

    if "apikey" in params and "bearertoken" in params:
        raise ValueError("apikey and bearertoken cannot both be provided")

    if "apikey" in params:
        auth_header = params["apikey"]
    elif "bearertoken" in params:
        auth_header = f"Bearer {params['bearertoken']}"

    if auth_header:
        options = [("grpc.enable_http_proxy", 0)]
        call_credentials = grpc.metadata_call_credentials(
            lambda context, callback: callback((("authorization", auth_header),), None)
        )
        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(), call_credentials
        )

    from pydgraph.client_stub import DgraphClientStub

    client_stub = DgraphClientStub(f"{host}:{port}", credentials, options)
    client = DgraphClient(client_stub)

    if username:
        thirty_seconds = 30
        client.login(username, password, timeout=thirty_seconds)

    return client
