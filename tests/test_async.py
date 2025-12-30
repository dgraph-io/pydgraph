# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests to verify async client methods."""

from __future__ import annotations

__author__ = "Martin Martinez Rivera"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import json

import pydgraph

from . import helper


class TestAsync(helper.ClientIntegrationTestCase):
    server_addr = "localhost:9180"

    def setUp(self) -> None:
        super().setUp()
        helper.drop_all(self.client)

    def test_mutation_and_query(self) -> None:
        """Runs mutation and queries asyncronously."""
        alter_future = self.client.async_alter(
            pydgraph.Operation(schema="name: string @index(term) .")
        )
        response = pydgraph.DgraphClient.handle_alter_future(alter_future)

        txn = self.client.txn()
        mutate_future = txn.async_mutate(
            pydgraph.Mutation(commit_now=True),
            set_nquads="""
            <_:alice> <name> \"Alice\" .
            <_:greg> <name> \"Greg\" .
            <_:alice> <follows> <_:greg> .
        """,
        )
        _ = pydgraph.Txn.handle_mutate_future(txn, mutate_future, True)

        query = """query me($a: string) {
            me(func: anyofterms(name, "Alice"))
            {
                name
                follows
                {
                    name
                }
            }
        }"""

        txn = self.client.txn()
        query_future = txn.async_query(query, variables={"$a": "Alice"})
        response = pydgraph.Txn.handle_query_future(query_future)
        assert json.loads(response.json).get("me") == [{"name": "Alice", "follows": [{"name": "Greg"}]}]
