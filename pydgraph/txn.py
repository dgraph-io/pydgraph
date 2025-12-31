# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dgraph atomic transaction support."""

from __future__ import annotations

import contextlib
import json
from typing import TYPE_CHECKING, Any

import grpc

from pydgraph import errors, util
from pydgraph.meta import VERSION
from pydgraph.proto import api_pb2 as api

if TYPE_CHECKING:
    from pydgraph.client import DgraphClient
    from pydgraph.client_stub import DgraphClientStub

__author__ = "Shailesh Kochhar <shailesh.kochhar@gmail.com>"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"


class Txn:
    """Txn is a single atomic transaction.

    A transaction lifecycle is as follows:

    1. Created using Client.newTxn.

    2. Modified via calls to query and mutate.

    3. Committed or discarded. If any mutations have been made, it's important
    that at least one of these methods is called to clean up resources. Discard
    is a no-op if commit has already been called, so it's safe to call discard
    after calling commit.
    """

    def __init__(
        self,
        client: DgraphClient,
        read_only: bool = False,
        best_effort: bool = False,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        if not read_only and best_effort:
            # FIXME: Should use errors.TransactionError for better exception handling
            # but changing exception type could break existing code that catches Exception
            raise Exception(  # noqa: TRY002
                "Best effort transactions are only compatible with "
                "read-only transactions"
            )

        self._dg: DgraphClient = client
        self._dc: DgraphClientStub = client.any_client()
        self._ctx: api.TxnContext = api.TxnContext()

        self._finished: bool = False
        self._mutated: bool = False
        self._read_only: bool = read_only
        self._best_effort: bool = best_effort
        self._commit_kwargs: dict[str, Any] = {
            "timeout": timeout,
            "metadata": metadata,
            "credentials": credentials,
        }

    def __enter__(self) -> Txn:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            self.discard(**self._commit_kwargs)
            raise exc_val
        if not self._read_only and not self._finished:
            self.commit(**self._commit_kwargs)
        else:
            self.discard(**self._commit_kwargs)

    def query(
        self,
        query: str,
        variables: dict[str, str] | None = None,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
        resp_format: str = "JSON",
    ) -> api.Response:
        """Executes a query operation."""
        req = self.create_request(
            query=query, variables=variables, resp_format=resp_format
        )
        return self.do_request(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def async_query(
        self,
        query: str,
        variables: dict[str, str] | None = None,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
        resp_format: str = "JSON",
    ) -> grpc.Future:
        """Async version of query."""
        req = self.create_request(
            query=query, variables=variables, resp_format=resp_format
        )
        return self.async_do_request(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def mutate(
        self,
        mutation: api.Mutation | None = None,
        set_obj: dict[str, Any] | None = None,
        del_obj: dict[str, Any] | None = None,
        set_nquads: str | None = None,
        del_nquads: str | None = None,
        cond: str | None = None,
        commit_now: bool | None = None,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Response:
        """Executes a mutate operation."""
        mutation = self.create_mutation(
            mutation, set_obj, del_obj, set_nquads, del_nquads, cond
        )
        commit_now = commit_now or mutation.commit_now
        req = self.create_request(mutations=[mutation], commit_now=commit_now)
        return self.do_request(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def async_mutate(
        self,
        mutation: api.Mutation | None = None,
        set_obj: dict[str, Any] | None = None,
        del_obj: dict[str, Any] | None = None,
        set_nquads: str | None = None,
        del_nquads: str | None = None,
        cond: str | None = None,
        commit_now: bool | None = None,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> grpc.Future:
        """Async version of mutate."""
        mutation = self.create_mutation(
            mutation, set_obj, del_obj, set_nquads, del_nquads, cond
        )
        commit_now = commit_now or mutation.commit_now
        req = self.create_request(mutations=[mutation], commit_now=commit_now)
        return self.async_do_request(
            req, timeout=timeout, metadata=metadata, credentials=credentials
        )

    def do_request(
        self,
        request: api.Request,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.Response:
        """Executes a query/mutate operation on the server."""
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

        request.hash = self._ctx.hash
        new_metadata = self._dg.add_login_metadata(metadata)
        query_error = None
        try:
            response = self._dc.query(
                request, timeout=timeout, metadata=new_metadata, credentials=credentials
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self._dg.retry_login()
                new_metadata = self._dg.add_login_metadata(metadata)
                try:
                    response = self._dc.query(
                        request,
                        timeout=timeout,
                        metadata=new_metadata,
                        credentials=credentials,
                    )
                except Exception as retry_error:
                    query_error = retry_error
            else:
                query_error = error

        if query_error is not None:
            # Ignore error during discard - user should see the original error
            with contextlib.suppress(Exception):
                self.discard(
                    timeout=timeout, metadata=metadata, credentials=credentials
                )

            self._common_except_mutate(query_error)

        if request.commit_now:
            self._finished = True

        self.merge_context(response.txn)
        return response

    def async_do_request(
        self,
        request: api.Request,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> grpc.Future:
        """Async version of do_request."""
        if self._finished:
            # FIXME: Should use errors.TransactionError for better exception handling
            raise Exception("Transaction has already been committed or discarded")  # noqa: TRY002

        if len(request.mutations) > 0:
            if self._read_only:
                # FIXME: Should use errors.TransactionError for better exception handling
                raise Exception("Readonly transaction cannot run mutations")  # noqa: TRY002
            self._mutated = True

        new_metadata = self._dg.add_login_metadata(metadata)
        return self._dc.async_query(
            request, timeout=timeout, metadata=new_metadata, credentials=credentials
        )

    @staticmethod
    def handle_query_future(future: grpc.Future) -> api.Response:
        """Method to call when getting the result of a future returned by async_query"""
        try:
            response = future.result()
        except Exception as error:
            Txn._common_except_mutate(error)

        return response

    @staticmethod
    def handle_mutate_future(
        txn: Txn, future: grpc.Future, commit_now: bool
    ) -> api.Response:
        """Method to call when getting the result of a future returned by async_mutate"""
        try:
            response = future.result()
        except Exception as error:
            # Ignore error during discard - user should see the original error
            with contextlib.suppress(Exception):
                txn.discard(**txn._commit_kwargs)
            Txn._common_except_mutate(error)

        if commit_now:
            txn._finished = True

        txn.merge_context(response.txn)
        return response

    def create_mutation(
        self,
        mutation: api.Mutation | None = None,
        set_obj: dict[str, Any] | None = None,
        del_obj: dict[str, Any] | None = None,
        set_nquads: str | None = None,
        del_nquads: str | None = None,
        cond: str | None = None,
    ) -> api.Mutation:
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
            mutation.cond = cond
        return mutation

    def create_request(
        self,
        query: str | None = None,
        variables: dict[str, str] | None = None,
        mutations: list[api.Mutation] | None = None,
        commit_now: bool | None = None,
        resp_format: str = "JSON",
    ) -> api.Request:
        """Creates a request object"""
        if resp_format == "JSON":
            format_value = api.Request.RespFormat.JSON
        elif resp_format == "RDF":
            format_value = api.Request.RespFormat.RDF
        else:
            raise errors.TransactionError(
                "Response format should be either RDF or JSON"
            )

        request = api.Request(
            start_ts=self._ctx.start_ts,
            commit_now=commit_now if commit_now is not None else False,
            read_only=self._read_only,
            best_effort=self._best_effort,
            resp_format=format_value,
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
            request.query = query
        if mutations:
            request.mutations.extend(mutations)
        return request

    @staticmethod
    def _common_except_mutate(error: Exception) -> None:
        if util.is_aborted_error(error):
            raise errors.AbortedError

        if util.is_retriable_error(error):
            raise errors.RetriableError(error)

        if util.is_connection_error(error):
            raise errors.ConnectionError(error)

        raise error

    def commit(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> api.TxnContext | None:
        """Commits the transaction."""
        if not self._common_commit():
            return None

        new_metadata = self._dg.add_login_metadata(metadata)
        try:
            return self._dc.commit_or_abort(
                self._ctx,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self._dg.retry_login()
                new_metadata = self._dg.add_login_metadata(metadata)
                try:
                    return self._dc.commit_or_abort(
                        self._ctx,
                        timeout=timeout,
                        metadata=new_metadata,
                        credentials=credentials,
                    )
                except Exception as retry_error:
                    self._common_except_commit(retry_error)
                    raise  # This should never be reached due to _common_except_commit raising

            self._common_except_commit(error)
            raise  # This should never be reached due to _common_except_commit raising

    def _common_commit(self) -> bool:
        if self._finished:
            raise errors.TransactionError(
                "Transaction has already been committed or discarded"
            )
        if self._read_only:
            raise errors.TransactionError("Readonly transaction cannot run mutations")

        self._finished = True
        return self._mutated

    @staticmethod
    def _common_except_commit(error: Exception) -> None:
        if util.is_aborted_error(error):
            raise errors.AbortedError

        raise error

    def discard(
        self,
        timeout: float | None = None,
        metadata: list[tuple[str, str]] | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """Discards the transaction."""
        if not self._common_discard():
            return

        new_metadata = self._dg.add_login_metadata(metadata)
        try:
            self._dc.commit_or_abort(
                self._ctx,
                timeout=timeout,
                metadata=new_metadata,
                credentials=credentials,
            )
        except Exception as error:
            if util.is_jwt_expired(error):
                self._dg.retry_login()
                new_metadata = self._dg.add_login_metadata(metadata)
                self._dc.commit_or_abort(
                    self._ctx,
                    timeout=timeout,
                    metadata=new_metadata,
                    credentials=credentials,
                )
            else:
                raise

    def _common_discard(self) -> bool:
        if self._finished:
            return False

        self._finished = True
        if not self._mutated:
            return False

        self._ctx.aborted = True
        return True

    def merge_context(self, src: api.TxnContext | None = None) -> None:
        """Merges context from this instance with src."""
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

    def retry_login(self) -> None:
        self._dg.retry_login()
