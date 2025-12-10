# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Dgraph async atomic transaction support."""

import asyncio
import json

from pydgraph import errors, util
from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api

__author__ = "Hypermode Inc."
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"
__version__ = VERSION
__status__ = "development"


class AsyncTxn:
    """Async transaction for atomic ACID operations.

    A transaction lifecycle is as follows:

    1. Created using AsyncDgraphClient.txn()
    2. Modified via calls to query and mutate
    3. Committed or discarded. If any mutations have been made, it's important
       that at least one of these methods is called to clean up resources. Discard
       is a no-op if commit has already been called, so it's safe to call discard
       after calling commit.

    Can be used as an async context manager:
        async with client.txn() as txn:
            response = await txn.query(query_string)
            # Automatically discarded on exit
    """

    def __init__(self, client, read_only=False, best_effort=False):
        """Initialize async transaction.

        Args:
            client: AsyncDgraphClient instance
            read_only: If True, transaction is read-only
            best_effort: If True, use best-effort mode (only for read-only)

        Raises:
            Exception: If best_effort is True but read_only is False
        """
        if not read_only and best_effort:
            raise Exception(
                "Best effort transactions are only compatible with "
                "read-only transactions"
            )

        self._dg = client
        self._dc = client.any_client()
        self._ctx = api.TxnContext()

        self._finished = False
        self._mutated = False
        self._read_only = read_only
        self._best_effort = best_effort
        self._lock = asyncio.Lock()  # Protect transaction state from concurrent access

    async def query(
        self,
        query,
        variables=None,
        timeout=None,
        metadata=None,
        credentials=None,
        resp_format="JSON",
    ):
        """Executes a query operation.

        Args:
            query: Query string
            variables: Dictionary of query variables
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials
            resp_format: Response format ("JSON" or "RDF")

        Returns:
            Response protobuf message

        Raises:
            TransactionError: If transaction is already finished
            Various gRPC errors on failure
        """
        req = self.create_request(
            query=query, variables=variables, resp_format=resp_format
        )
        return await self.do_request(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def mutate(
        self,
        mutation=None,
        set_obj=None,
        del_obj=None,
        set_nquads=None,
        del_nquads=None,
        cond=None,
        commit_now=None,
        timeout=None,
        metadata=None,
        credentials=None,
    ):
        """Executes a mutate operation.

        Args:
            mutation: Mutation protobuf message (optional if using other params)
            set_obj: Dictionary to set as JSON
            del_obj: Dictionary to delete as JSON
            set_nquads: NQuads string to set
            del_nquads: NQuads string to delete
            cond: Conditional mutation string
            commit_now: If True, commit immediately after mutation
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            Response protobuf message

        Raises:
            TransactionError: If transaction is read-only or already finished
            Various gRPC errors on failure
        """
        mutation = self.create_mutation(
            mutation, set_obj, del_obj, set_nquads, del_nquads, cond
        )
        commit_now = commit_now or mutation.commit_now
        req = self.create_request(mutations=[mutation], commit_now=commit_now)
        return await self.do_request(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    async def do_request(self, request, timeout=None, metadata=None, credentials=None):
        """Executes a query/mutate operation on the server.

        Handles JWT refresh automatically if token expires.

        Args:
            request: Request protobuf message
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            Response protobuf message

        Raises:
            TransactionError: If transaction is already finished or read-only with mutations
            Various gRPC errors on failure
        """
        async with self._lock:
            if self._finished:
                raise errors.TransactionError(
                    "Transaction has already been committed or discarded"
                )

            if len(request.mutations) > 0:
                if self._read_only:
                    raise errors.TransactionError(
                        "Readonly transaction cannot run mutations"
                    )
                self._mutated = True

            request.start_ts = self._ctx.start_ts
            request.hash = self._ctx.hash
            new_metadata = self._dg.add_login_metadata(metadata)
            query_error = None

            try:
                response = await self._dc.query(
                    request, timeout=timeout, metadata=new_metadata, credentials=credentials
                )
            except asyncio.CancelledError:
                # Preserve cancellation - don't catch or wrap it
                raise
            except Exception as error:
                # Handle JWT expiration with automatic retry
                if util.is_jwt_expired(error):
                    await self._dg.retry_login()
                    new_metadata = self._dg.add_login_metadata(metadata)
                    try:
                        response = await self._dc.query(
                            request,
                            timeout=timeout,
                            metadata=new_metadata,
                            credentials=credentials,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        query_error = error
                else:
                    query_error = error

            if query_error is not None:
                # Try to discard the transaction on error
                try:
                    await self.discard(
                        timeout=timeout, metadata=metadata, credentials=credentials
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Ignore discard error - user should see the original error
                    pass

                self._common_except_mutate(query_error)

            if request.commit_now:
                self._finished = True

            self.merge_context(response.txn)
            return response

    def create_mutation(
        self,
        mutation=None,
        set_obj=None,
        del_obj=None,
        set_nquads=None,
        del_nquads=None,
        cond=None,
    ):
        """Creates a mutation protobuf message.

        Args:
            mutation: Existing mutation to update (or None to create new)
            set_obj: Dictionary to set as JSON
            del_obj: Dictionary to delete as JSON
            set_nquads: NQuads string to set
            del_nquads: NQuads string to delete
            cond: Conditional mutation string

        Returns:
            Mutation protobuf message
        """
        if not mutation:
            mutation = api.Mutation()
        if set_obj:
            mutation.set_json = json.dumps(set_obj).encode("utf8")
        if del_obj:
            mutation.delete_json = json.dumps(del_obj).encode("utf8")
        if set_nquads:
            mutation.set_nquads = set_nquads.encode("utf8")
        if del_nquads:
            mutation.del_nquads = del_nquads.encode("utf8")
        if cond:
            mutation.cond = cond.encode("utf8")
        return mutation

    def create_request(
        self,
        query=None,
        variables=None,
        mutations=None,
        commit_now=None,
        resp_format="JSON",
    ):
        """Creates a request protobuf message.

        Args:
            query: Query string
            variables: Dictionary of query variables (keys and values must be strings)
            mutations: List of mutation protobuf messages
            commit_now: If True, commit immediately
            resp_format: Response format ("JSON" or "RDF")

        Returns:
            Request protobuf message

        Raises:
            TransactionError: If resp_format is invalid or variables are not strings
        """
        if resp_format == "JSON":
            resp_format = api.Request.RespFormat.JSON
        elif resp_format == "RDF":
            resp_format = api.Request.RespFormat.RDF
        else:
            raise errors.TransactionError(
                "Response format should be either RDF or JSON"
            )

        request = api.Request(
            start_ts=self._ctx.start_ts,
            commit_now=commit_now,
            read_only=self._read_only,
            best_effort=self._best_effort,
            resp_format=resp_format,
        )

        if variables is not None:
            for key, value in variables.items():
                if util.is_string(key) and util.is_string(value):
                    request.vars[key] = value
                else:
                    raise errors.TransactionError(
                        "Values and keys in variable map must be strings"
                    )
        if query:
            request.query = query.encode("utf8")
        if mutations:
            request.mutations.extend(mutations)
        return request

    @staticmethod
    def _common_except_mutate(error):
        """Maps gRPC errors to pydgraph exceptions.

        Args:
            error: Exception from gRPC call

        Raises:
            AbortedError: If transaction was aborted
            RetriableError: If error is retriable
            ConnectionError: If error is connection-related
            The original error otherwise
        """
        if util.is_aborted_error(error):
            raise errors.AbortedError()

        if util.is_retriable_error(error):
            raise errors.RetriableError(error)

        if util.is_connection_error(error):
            raise errors.ConnectionError(error)

        raise error

    async def commit(self, timeout=None, metadata=None, credentials=None):
        """Commits the transaction.

        Args:
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Returns:
            TxnContext protobuf message

        Raises:
            TransactionError: If transaction is read-only or already finished
            AbortedError: If transaction was aborted
        """
        async with self._lock:
            if not self._common_commit():
                return

            new_metadata = self._dg.add_login_metadata(metadata)
            try:
                return await self._dc.commit_or_abort(
                    self._ctx,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Handle JWT expiration with automatic retry
                if util.is_jwt_expired(error):
                    await self._dg.retry_login()
                    new_metadata = self._dg.add_login_metadata(metadata)
                    try:
                        return await self._dc.commit_or_abort(
                            self._ctx,
                            timeout=timeout,
                            metadata=new_metadata,
                            credentials=credentials,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        return self._common_except_commit(error)

                self._common_except_commit(error)

    def _common_commit(self):
        """Validates and prepares for commit.

        Returns:
            True if commit should proceed, False if transaction was not mutated

        Raises:
            TransactionError: If transaction is read-only or already finished
        """
        if self._read_only:
            raise errors.TransactionError(
                "Readonly transaction cannot run mutations or be committed"
            )
        if self._finished:
            raise errors.TransactionError(
                "Transaction has already been committed or discarded"
            )

        self._finished = True
        return self._mutated

    @staticmethod
    def _common_except_commit(error):
        """Maps commit errors to pydgraph exceptions.

        Args:
            error: Exception from commit operation

        Raises:
            AbortedError: If transaction was aborted
            The original error otherwise
        """
        if util.is_aborted_error(error):
            raise errors.AbortedError()

        raise error

    async def discard(self, timeout=None, metadata=None, credentials=None):
        """Discards the transaction.

        Safe to call multiple times or after commit.

        Args:
            timeout: Request timeout in seconds
            metadata: Request metadata
            credentials: Call credentials

        Raises:
            Various gRPC errors on failure
        """
        async with self._lock:
            if not self._common_discard():
                return

            new_metadata = self._dg.add_login_metadata(metadata)
            try:
                await self._dc.commit_or_abort(
                    self._ctx,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Handle JWT expiration with automatic retry
                if util.is_jwt_expired(error):
                    await self._dg.retry_login()
                    new_metadata = self._dg.add_login_metadata(metadata)
                    await self._dc.commit_or_abort(
                        self._ctx,
                        timeout=timeout,
                        metadata=new_metadata,
                        credentials=credentials,
                    )
                else:
                    raise error

    def _common_discard(self):
        """Validates and prepares for discard.

        Returns:
            True if discard should proceed, False if already finished or not mutated
        """
        if self._finished:
            return False

        self._finished = True
        if not self._mutated:
            return False

        self._ctx.aborted = True
        return True

    def merge_context(self, src=None):
        """Merges transaction context from server response.

        Args:
            src: TxnContext protobuf message from server

        Raises:
            TransactionError: If start_ts doesn't match
        """
        if src is None:
            # This condition will be true only if the server doesn't return a
            # txn context after a query or mutation.
            return

        if self._ctx.start_ts == 0:
            self._ctx.start_ts = src.start_ts
        elif self._ctx.start_ts != src.start_ts:
            # This condition should never be true.
            raise errors.TransactionError("StartTs mismatch")

        self._ctx.hash = src.hash
        self._ctx.keys.extend(src.keys)
        self._ctx.preds.extend(src.preds)

    async def __aenter__(self):
        """Async context manager entry.

        Returns:
            Self for use in 'async with' statement
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit.

        Automatically discards transaction if not already finished.

        Args:
            exc_type: Exception type (if any)
            exc_val: Exception value (if any)
            exc_tb: Exception traceback (if any)

        Returns:
            False to propagate any exception
        """
        if not self._finished:
            try:
                # Use a reasonable timeout to prevent hangs
                thirty_seconds = 30
                await self.discard(timeout=thirty_seconds)
            except asyncio.CancelledError:
                # Preserve cancellation during cleanup
                raise
            except Exception:
                # Suppress discard errors during cleanup
                pass
        return False
