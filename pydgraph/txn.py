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

"""Dgraph atomic transaction support."""

import json
import grpc

from pydgraph import errors, util
from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api

__author__ = 'Shailesh Kochhar <shailesh.kochhar@gmail.com>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'
__version__ = VERSION
__status__ = 'development'


class Txn(object):
    """Txn is a single atomic transaction.

    A transaction lifecycle is as follows:

    1. Created using Client.newTxn.

    2. Modified via calls to query and mutate.

    3. Committed or discarded. If any mutations have been made, it's important
    that at least one of these methods is called to clean up resources. Discard
    is a no-op if commit has already been called, so it's safe to call discard
    after calling commit.
    """

    def __init__(self, client, read_only=False, best_effort=False):
        if not read_only and best_effort:
            raise Exception('Best effort transactions are only compatible with '
                            'read-only transactions')

        self._dg = client
        self._dc = client.any_client()
        self._ctx = api.TxnContext()

        self._finished = False
        self._mutated = False
        self._read_only = read_only
        self._best_effort = best_effort

    def query(self, query, variables=None, timeout=None, metadata=None, credentials=None):
        """Executes a query operation."""
        req = self.create_request(query=query, variables=variables)
        return self.do_request(req, timeout=timeout, metadata=metadata, credentials=credentials)

    def async_query(self, query, variables=None, timeout=None, metadata=None, credentials=None):
        """Async version of query."""
        req = self.create_request(query=query, variables=variables)
        return self.async_do_request(req, timeout=timeout, metadata=metadata, credentials=credentials)

    def mutate(self, mutation=None, set_obj=None, del_obj=None,
               set_nquads=None, del_nquads=None, cond=None, commit_now=None,
               timeout=None, metadata=None, credentials=None):
        """Executes a mutate operation."""
        mutation = self.create_mutation(mutation, set_obj, del_obj, set_nquads, del_nquads, cond)
        commit_now = commit_now or mutation.commit_now
        req = self.create_request(mutations=[mutation], commit_now=commit_now)
        return self.do_request(req, timeout=timeout, metadata=metadata, credentials=credentials)

    def async_mutate(self, mutation=None, set_obj=None, del_obj=None,
                     set_nquads=None, del_nquads=None, cond=None, commit_now=None,
                     timeout=None, metadata=None, credentials=None):
        """Async version of mutate."""
        mutation = self.create_mutation(mutation, set_obj, del_obj, set_nquads, del_nquads, cond)
        commit_now = commit_now or mutation.commit_now
        req = self.create_request(mutations=[mutation], commit_now=commit_now)
        return self.async_do_request(req, timeout=timeout, metadata=metadata,
                                     credentials=credentials)

    def do_request(self, request, timeout=None, metadata=None, credentials=None):
        """Executes a query/mutate operation on the server."""
        if self._finished:
            raise errors.TransactionError('Transaction has already been committed or discarded')

        if len(request.mutations) > 0:
            if self._read_only:
                raise errors.TransactionError('Readonly transaction cannot run mutations')
            self._mutated = True

        new_metadata = self._dg.add_login_metadata(metadata)
        query_error = None
        try:
            response = self._dc.query(request, timeout=timeout,
                                      metadata=new_metadata,
                                      credentials=credentials)
        except Exception as error:
            if util.is_jwt_expired(error):
                self._dg.retry_login()
                new_metadata = self._dg.add_login_metadata(metadata)
                try:
                    response = self._dc.query(request, timeout=timeout,
                                              metadata=new_metadata,
                                              credentials=credentials)
                except Exception as error:
                    query_error = error
            else:
                query_error = error

        if query_error is not None:
            try:
                self.discard(timeout=timeout, metadata=metadata,
                             credentials=credentials)
            except:
                # Ignore error - user should see the original error.
                pass

            self._common_except_mutate(query_error)

        if request.commit_now:
            self._finished = True

        self.merge_context(response.txn)
        return response

    def async_do_request(self, request, timeout=None, metadata=None, credentials=None):
        """Async version of do_request."""
        if self._finished:
            raise Exception('Transaction has already been committed or discarded')

        if len(request.mutations) > 0:
            if self._read_only:
                raise Exception('Readonly transaction cannot run mutations')
            self._mutated = True

        new_metadata = self._dg.add_login_metadata(metadata)
        return self._dc.async_query(request, timeout=timeout,
                                    metadata=new_metadata,
                                    credentials=credentials)

    @staticmethod
    def handle_query_future(future):
        """Method to call when getting the result of a future returned by async_query"""
        try:
            response = future.result()
        except Exception as error:
            txn._common_except_mutate(error)

        return response


    @staticmethod
    def handle_mutate_future(txn, future, commit_now):
        """Method to call when getting the result of a future returned by async_mutate"""
        try:
            response = future.result()
        except Exception as error:
            try:
                txn.discard(timeout=timeout, metadata=metadata, credentials=credentials)
            except:
                # Ignore error - user should see the original error.
                pass
            txn._common_except_mutate(error)

        if commit_now:
            txn._finished = True

        txn.merge_context(response.txn)
        return response

    def create_mutation(self, mutation=None, set_obj=None, del_obj=None,
                        set_nquads=None, del_nquads=None, cond=None):
        if not mutation:
            mutation = api.Mutation()
        if set_obj:
            mutation.set_json = json.dumps(set_obj).encode('utf8')
        if del_obj:
            mutation.delete_json = json.dumps(del_obj).encode('utf8')
        if set_nquads:
            mutation.set_nquads = set_nquads.encode('utf8')
        if del_nquads:
            mutation.del_nquads = del_nquads.encode('utf8')
        if cond:
            mutation.cond = cond.encode('utf8')
        return mutation

    def create_request(self, query=None, variables=None, mutations=None, commit_now=None):
        """Creates a request object"""
        request = api.Request(start_ts = self._ctx.start_ts, commit_now=commit_now,
                              read_only=self._read_only, best_effort=self._best_effort)

        if variables is not None:
            for key, value in variables.items():
                if util.is_string(key) and util.is_string(value):
                    request.vars[key] = value
                else:
                    raise errors.TransactionError('Values and keys in variable map must be strings')
        if query:
            request.query = query.encode('utf8')
        if mutations:
            request.mutations.extend(mutations)
        return request

    @staticmethod
    def _common_except_mutate(error):
        if util.is_aborted_error(error):
            raise errors.AbortedError()

        if util.is_retriable_error(error):
            raise errors.RetriableError(error)

        if util.is_connection_error(error):
            raise errors.ConnectionError(error)

        raise error

    def commit(self, timeout=None, metadata=None, credentials=None):
        """Commits the transaction."""
        if not self._common_commit():
            return

        new_metadata = self._dg.add_login_metadata(metadata)
        try:
            self._dc.commit_or_abort(self._ctx, timeout=timeout,
                                     metadata=new_metadata,
                                     credentials=credentials)
        except Exception as error:
            if util.is_jwt_expired(error):
                self._dg.retry_login()
                new_metadata = self._dg.add_login_metadata(metadata)
                try:
                    self._dc.commit_or_abort(self._ctx, timeout=timeout,
                                             metadata=new_metadata,
                                             credentials=credentials)
                except Exception as error:
                    return self._common_except_commit(error)

            self._common_except_commit(error)

    def _common_commit(self):
        if self._read_only:
            raise errors.TransactionError(
                'Readonly transaction cannot run mutations or be committed')
        if self._finished:
            raise errors.TransactionError(
                'Transaction has already been committed or discarded')

        self._finished = True
        return self._mutated

    @staticmethod
    def _common_except_commit(error):
        if util.is_aborted_error(error):
            raise errors.AbortedError()

        raise error

    def discard(self, timeout=None, metadata=None, credentials=None):
        """Discards the transaction."""
        if not self._common_discard():
            return

        new_metadata = self._dg.add_login_metadata(metadata)
        try:
            self._dc.commit_or_abort(self._ctx, timeout=timeout,
                                     metadata=new_metadata,
                                     credentials=credentials)
        except Exception as error:
            if util.is_jwt_expired(error):
                self._dg.retry_login()
                new_metadata = self._dg.add_login_metadata(metadata)
                self._dc.commit_or_abort(self._ctx, timeout=timeout,
                                         metadata=new_metadata,
                                         credentials=credentials)
            else:
                raise error

    def _common_discard(self):
        if self._finished:
            return False

        self._finished = True
        if not self._mutated:
            return False

        self._ctx.aborted = True
        return True

    def merge_context(self, src=None):
        """Merges context from this instance with src."""
        if src is None:
            # This condition will be true only if the server doesn't return a
            # txn context after a query or mutation.
            return

        if self._ctx.start_ts == 0:
            self._ctx.start_ts = src.start_ts
        elif self._ctx.start_ts != src.start_ts:
            # This condition should never be true.
            raise errors.TransactionError('StartTs mismatch')

        self._ctx.keys.extend(src.keys)
        self._ctx.preds.extend(src.preds)

    def retry_login(self):
        self._dg.retry_login()
