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

from pydgraph import errors, txn, util
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
        self._jwt = api.Jwt()
        self._login_metadata = []

    def check_version(self, timeout=None, metadata=None, credentials=None):
        """Returns the version of Dgraph if the server is ready to accept requests."""

        new_metadata = self.add_login_metadata(metadata)
        check_req = api.Check()

        try:
            response = self.any_client().check_version(check_req, timeout=timeout,
                                                       metadata=new_metadata,
                                                       credentials=credentials)
            return response.tag
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                response = self.any_client().check_version(check_req, timeout=timeout,
                                                   metadata=new_metadata,
                                                   credentials=credentials)
                return response.tag
            else:
                raise error

    def login(self, userid, password, timeout=None, metadata=None,
              credentials=None):
        login_req = api.LoginRequest()
        login_req.userid = userid
        login_req.password = password
        login_req.namespace = 0

        response = self.any_client().login(login_req, timeout=timeout,
                                           metadata=metadata,
                                           credentials=credentials)
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    def login_into_namespace(self, userid, password, namespace, timeout=None, metadata=None,
              credentials=None):
        login_req = api.LoginRequest()
        login_req.userid = userid
        login_req.password = password
        login_req.namespace = namespace

        response = self.any_client().login(login_req, timeout=timeout,
                                           metadata=metadata,
                                           credentials=credentials)
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    def retry_login(self, timeout=None, metadata=None, credentials=None):
        if len(self._jwt.refresh_jwt) == 0:
            raise ValueError('refresh jwt should not be empty')

        login_req = api.LoginRequest()
        login_req.refresh_token = self._jwt.refresh_jwt

        response = self.any_client().login(login_req, timeout=timeout,
                                           metadata=metadata,
                                           credentials=credentials)
        self._jwt = api.Jwt()
        self._jwt.ParseFromString(response.json)
        self._login_metadata = [("accessjwt", self._jwt.access_jwt)]

    def alter(self, operation, timeout=None, metadata=None, credentials=None):
        """Runs a modification via this client."""
        new_metadata = self.add_login_metadata(metadata)

        try:
            return self.any_client().alter(operation, timeout=timeout,
                                           metadata=new_metadata,
                                           credentials=credentials)
        except Exception as error:
            if util.is_jwt_expired(error):
                self.retry_login()
                new_metadata = self.add_login_metadata(metadata)
                try:
                    return self.any_client().alter(operation, timeout=timeout,
                                                   metadata=new_metadata,
                                                   credentials=credentials)
                except Exception as error:
                    self._common_except_alter(error)
            else:
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
        new_metadata = self.add_login_metadata(metadata)
        return self.any_client().async_alter(operation, timeout=timeout,
                                             metadata=new_metadata,
                                             credentials=credentials)

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
        return random.choice(self._clients)

    def add_login_metadata(self, metadata):
        new_metadata = list(self._login_metadata)
        if not metadata:
            return new_metadata
        new_metadata.extend(metadata)
        return new_metadata
