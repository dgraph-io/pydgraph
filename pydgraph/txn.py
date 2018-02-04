"""
This module contains the methods for using transactions when using
a dgraph server over gRPC.
"""
import grpc
import json

from pydgraph import util
from pydgraph.proto import api_pb2 as api

__author__ = 'Shailesh Kochhar <shailesh.kochhar@gmail.com>'


class DgraphTxn(object):
    """Class representing a single transaction. A transaction will maintain
    three items of state for each transaction.

    Attributes
    ==========

    * lin_read: a linearizable read pointer, maintained independently of the
                parent client
    * start_ts: a starting timestamp, this uniquely identifies a transaction
                and doesn't change over its lifetime.
    * keys: the set of keys modified by the transaction to aid in conflict
            detection
    """
    def __init__(self, client):
        self.client = client
        self.start_ts = 0
        self.lin_read = api.LinRead()
        self.lin_read.MergeFrom(client.lin_read)
        self.keys = []

        self._mutated = False
        self._finished = False

    def merge_context(self, txn_context):
        """Merges context from a txn context into the current txn state."""
        ## This will be true if the server does not return a txn context after
        ## a query or a mutation
        if not txn_context: return

        if self.start_ts == 0:
            self.start_ts = txn_context.start_ts
        elif self.start_ts != txn_context.start_ts:
            raise Exception('StartTs mismatch in txn(%s) vs updated context(%s)' %
                            (self.start_ts, txn_context.start_ts))

        self.client.merge_context(txn_context)
        util.merge_lin_reads(self.lin_read, txn_context.lin_read)
        self.keys = txn_context.keys[:]

    def query(self, q, *args, **kwargs):
        if self._finished: raise Exception('Transaction is complete')
        request = api.Request(query=q, start_ts=self.start_ts, lin_read=self.lin_read)
        response = self.client.stub.Query(request, *args, **kwargs)
        self.merge_context(response.txn)
        return response

    async def aquery(self, q, *args, **kwargs):
        if self._finished: raise Exception('Transaction is complete')
        request = api.Request(query=q, start_ts=self.start_ts, lin_read=self.lin_read)
        response = await self.client.stub.Query.future(request, *args, **kwargs)
        self.merge_context(response.txn)
        return response

    def mutate(self, setnquads=None, delnquads=None, *args, **kwargs):
        """Mutate extends MutateObj to allow mutations to be specified as
        N-Quad strings.

        Mutations also support a commit_now method which commits the transaction
        along with the mutation. This mode is presently unsupported.

        Params
        ======
          * setnquads: a string containing nquads to set
          * delnquads: a string containing nquads to delete

        N-Quad format is
            <subj> <pred> <obj> .
        """
        if self._finished: raise Exception('Transaction is complete')
        mutation = api.Mutation(start_ts=self.start_ts, commit_now=False)
        if kwargs.pop('ignore_index_conflict', None):
            mutation.ignore_index_conflict = True
        if setnquads:
            mutation.set_nquads=setnquads.encode('utf8')
        if delnquads:
            mutation.del_nquads=delnquads.encode('utf8')

        assigned = self.client.stub.Mutate(mutation, *args, **kwargs)
        self.merge_context(assigned.context)
        self._mutated = True
        return assigned

    def mutate_obj(self, setobj=None, delobj=None, *args, **kwargs):
        """Mutate allows modification of the data stored in the DGraph instance.

        A mutation can be described either using JSON or via RDF quads. This
        method presently support mutations described via JSON.

        Mutations also support a commit_now method which commits the transaction
        along with the mutation. This mode is presently unsupported.

        Params
        ======
          * setobj: an object with data to set, to be encoded as JSON and
                converted to utf8 bytes
          * delobj: an object with data to be deleted, to be encoded as JSON
                and converted to utf8 bytes.
        """
        if self._finished: raise Exception('Transaction is complete')
        mutation = api.Mutation(start_ts=self.start_ts, commit_now=False)
        if kwargs.pop('ignore_index_conflict', None):
            mutation.ignore_index_conflict = True
        if setobj:
            mutation.set_json=json.dumps(setobj).encode('utf8')
        if delobj:
            mutation.delete_json=json.dumps(delobj).encode('utf8')

        assigned = self.client.stub.Mutate(mutation, *args, **kwargs)
        self.merge_context(assigned.context)
        self._mutated = True
        return assigned

    async def amutate_obj(self, setobj=None, delobj=None, *args, **kwargs):
        if self._finished: raise Exception('Transaction is complete')
        mutation = api.Mutation(start_ts=self.start_ts, commit_now=False)
        if kwargs.pop('ignore_index_conflict', None):
            mutation.ignore_index_conflict = True
        if setobj:
            mutation.set_json=json.dumps(setobj).encode('utf8')
        if delobj:
            mutation.del_json=json.dumps(delobj).encode('utf8'),

        assigned = await self.client.stub.Mutate.future(mutation, *args, **kwargs)
        self.merge_context(assinged.context)
        self._mutated = True
        return assigned

    def commit(self, *args, **kwargs):
        """Commits any mutations performed in the transaction. Once the
        transaction is committed its lifespan is complete and no further
        mutations or commits can be made."""
        if self._finished: raise Exception('Cannot commit a transaction which is complete')

        self._finished = True
        if not self._mutated: return

        txn_context = api.TxnContext(start_ts=self.start_ts,
                                     keys=self.keys,
                                     lin_read=self.lin_read)
        resp_txn_context = self.client.stub.CommitOrAbort(txn_context, *args, **kwargs)
        return resp_txn_context

    def abort(self, *args, **kwargs):
        """Aborts any mutations performed in the transaction. Once the
        transaction is aborted its lifespan is complete and no further
        mutations or commits can be made."""
        if self._finished: raise Exception('Cannot abort a transaction which is complete')

        self._finished = True
        if not self._mutated: return

        txn_context = api.TxnContext(start_ts=self.start_ts,
                                     keys=self.keys,
                                     lin_read=self.lin_read,
                                     aborted=True)
        resp_txn_context = self.client.stub.CommitOrAbort(txn_context, *args, **kwargs)
        return resp_txn_context
