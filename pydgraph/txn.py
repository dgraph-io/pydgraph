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

    def __init__(self, client):
        self._dc = client
        self._ctx = api.TxnContext()
        self._dc.set_lin_read(self._ctx)

        self._finished = False
        self._mutated = False
        self._sequencing = api.LinRead.CLIENT_SIDE

    def sequencing(self, sequencing):
        """Sets sequencing."""
        self._sequencing = sequencing

    def query(self, query, variables=None, timeout=None, metadata=None,
              credentials=None):
        """Adds a query operation to the transaction."""
        req = self._common_query(query, variables=variables)
        res = self._dc.any_client().query(req, timeout=timeout,
                                          metadata=metadata,
                                          credentials=credentials)
        self.merge_context(res.txn)
        return res

    def _common_query(self, query, variables=None):
        if self._finished:
            raise Exception(
                'Transaction has already been committed or discarded')

        lin_read = self._ctx.lin_read
        lin_read.sequencing = self._sequencing
        req = api.Request(query=query, start_ts=self._ctx.start_ts,
                          lin_read=lin_read)
        if variables is not None:
            for key, value in variables.items():
                if util.is_string(key) and util.is_string(value):
                    req.vars[key] = value

        return req

    def mutate(self, mutation=None, set_obj=None, del_obj=None, set_nquads=None,
               del_nquads=None, commit_now=None, ignore_index_conflict=None,
               timeout=None, metadata=None, credentials=None):
        """Adds a mutate operation to the transaction."""
        mutation = self._common_mutate(
            mutation=mutation, set_obj=set_obj, del_obj=del_obj,
            set_nquads=set_nquads, del_nquads=del_nquads,
            commit_now=commit_now, ignore_index_conflict=ignore_index_conflict)

        try:
            assigned = self._dc.any_client().mutate(mutation, timeout=timeout,
                                                    metadata=metadata,
                                                    credentials=credentials)
        except Exception as error:
            try:
                self.discard(timeout=timeout, metadata=metadata,
                             credentials=credentials)
            except:
                # Ignore error - user should see the original error.
                pass

            self._common_except_mutate(error)

        if mutation.commit_now:
            self._finished = True

        self.merge_context(assigned.context)
        return assigned

    def _common_mutate(self, mutation=None, set_obj=None, del_obj=None,
                       set_nquads=None, del_nquads=None,
                       commit_now=None, ignore_index_conflict=None):
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
        if commit_now:
            mutation.commit_now = True
        if ignore_index_conflict:
            mutation.ignore_index_conflict = True

        if self._finished:
            raise Exception(
                'Transaction has already been committed or discarded')

        self._mutated = True
        mutation.start_ts = self._ctx.start_ts
        return mutation

    @staticmethod
    def _common_except_mutate(error):
        if isinstance(error, grpc._channel._Rendezvous):
            error.details()
            status_code = error.code()
            if (status_code == grpc.StatusCode.ABORTED or
                    status_code == grpc.StatusCode.FAILED_PRECONDITION):
                raise errors.AbortedError()

        raise error

    def commit(self, timeout=None, metadata=None, credentials=None):
        """Commits the transaction."""
        if not self._common_commit():
            return

        try:
            self._dc.any_client().commit_or_abort(self._ctx, timeout=timeout,
                                                  metadata=metadata,
                                                  credentials=credentials)
        except Exception as error:
            self._common_except_commit(error)

    def _common_commit(self):
        if self._finished:
            raise Exception(
                'Transaction has already been committed or discarded')

        self._finished = True
        return self._mutated

    @staticmethod
    def _common_except_commit(error):
        if isinstance(error, grpc._channel._Rendezvous):
            error.details()
            status_code = error.code()
            if status_code == grpc.StatusCode.ABORTED:
                raise errors.AbortedError()

        raise error

    def discard(self, timeout=None, metadata=None, credentials=None):
        """Discards the transaction."""
        if not self._common_discard():
            return

        self._dc.any_client().commit_or_abort(self._ctx, timeout=timeout,
                                              metadata=metadata,
                                              credentials=credentials)

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

        util.merge_lin_reads(self._ctx.lin_read, src.lin_read)
        self._dc.merge_lin_reads(src.lin_read)

        if self._ctx.start_ts == 0:
            self._ctx.start_ts = src.start_ts
        elif self._ctx.start_ts != src.start_ts:
            # This condition should never be true.
            raise Exception('StartTs mismatch')

        self._ctx.keys.extend(src.keys[:])
