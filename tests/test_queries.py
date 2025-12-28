# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests behavior of queries after mutation in the same transaction."""

__author__ = "Mohit Ranka <mohitranka@gmail.com>"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import json
import logging
import os
import unittest

import pydgraph
from pydgraph import open

from . import helper


class TestQueries(helper.ClientIntegrationTestCase):
    """Tests behavior of queries after mutation in the same transaction."""

    def setUp(self):
        super(TestQueries, self).setUp()
        host = os.environ.get("TEST_SERVER_ADDR", "localhost")
        host, port = host.split(":")
        self.dgraph_host = host
        self.dgraph_port = port
        helper.drop_all(self.client)
        helper.set_schema(self.client, "name: string @index(term) .")
        self.query = """query me($a: string) {
            me(func: anyofterms(name, "Alice"))
            {
                name
                follows
                {
                    name
                }
            }
        }"""

    def test_check_version(self):
        """Verifies the check_version method correctly returns the cluster version"""
        success = 0
        for _i in range(3):
            try:
                tag = self.client.check_version()
                self.assertGreater(len(tag), 0)
                success += 1
            except Exception:  # nosec B112
                continue
        self.assertGreater(success, 0)

    def test_mutation_and_query(self):
        """Runs mutation and verifies queries see the results."""
        txn = self.client.txn()
        _ = txn.mutate(
            pydgraph.Mutation(commit_now=True),
            set_nquads="""
            <_:alice> <name> \"Alice\" .
            <_:greg> <name> \"Greg\" .
            <_:alice> <follows> <_:greg> .
        """,
        )

        queryRDF = """query q($a: string) {
            q(func: anyofterms(name, "Alice"))
            {
                uid
                name
            }
        }"""

        response = self.client.txn().query(self.query, variables={"$a": "Alice"})
        self.assertEqual(
            [{"name": "Alice", "follows": [{"name": "Greg"}]}],
            json.loads(response.json).get("me"),
        )
        self.assertTrue(
            is_number(response.latency.parsing_ns), "Parsing latency is not available"
        )
        self.assertTrue(
            is_number(response.latency.processing_ns),
            "Processing latency is not available",
        )
        self.assertTrue(
            is_number(response.latency.encoding_ns), "Encoding latency is not available"
        )

        """ Run query with JSON and RDF resp_format and verify the result """
        response = self.client.txn().query(queryRDF, variables={"$a": "Alice"})
        uid = json.loads(response.json).get("q")[0]["uid"]
        expected_rdf = '<{}> <name> "Alice" .\n'.format(uid)
        response = self.client.txn().query(
            queryRDF, variables={"$a": "Alice"}, resp_format="RDF"
        )
        self.assertEqual(expected_rdf, response.rdf.decode("utf-8"))

    def test_run_dql(self):
        """Call run_dql (a version 25+ feature) and verify the result"""
        helper.skip_if_dgraph_version_below(self.client, "25.0.0", self)
        _ = self.client.run_dql(
            dql_query="""
            {
                set {
                    _:alice <name> "Alice" .
                    _:greg <name> "Greg" .
                    _:alice <follows> _:greg .
                }
            }
            """
        )
        response = self.client.run_dql(
            dql_query=self.query,
            vars={"$a": "Alice"},
            resp_format="JSON",
            read_only=True,
        )
        self.assertEqual(
            [{"name": "Alice", "follows": [{"name": "Greg"}]}],
            json.loads(response.json).get("me"),
        )

    def test_run_dql_with_vars(self):
        """Call run_dql_with_vars (a version 25+ feature) and verify the result"""
        helper.skip_if_dgraph_version_below(self.client, "25.0.0", self)
        helper.drop_all(self.client)
        # Set up schema
        schema = """
            name: string @index(exact) .
            email: string @index(exact) .
            age: int .
        """
        helper.set_schema(self.client, schema)

        # Add data
        _ = self.client.run_dql(
            dql_query="""
            {
                set {
                    _:alice <name> "Alice" .
                    _:alice <email> "alice@example.com" .
                    _:alice <age> "29" .
                }
            }
            """
        )

        # Query with variables using run_dql_with_vars
        query_dql_with_var = """query Alice($name: string) {
            alice(func: eq(name, $name)) {
                name
                email
                age
            }
        }"""
        vars = {"$name": "Alice"}
        resp = self.client.run_dql_with_vars(query_dql_with_var, vars, read_only=True)

        m = json.loads(resp.json)
        self.assertEqual(len(m.get("alice", [])), 1)
        self.assertEqual(m["alice"][0]["name"], "Alice")
        self.assertEqual(m["alice"][0]["email"], "alice@example.com")
        self.assertEqual(m["alice"][0]["age"], 29)

        # Test that vars=None raises ValueError
        with self.assertRaises(ValueError) as context:
            self.client.run_dql_with_vars(query_dql_with_var, None, read_only=True)
        self.assertIn("vars parameter is required", str(context.exception))

    def test_run_dql_in_namespace(self):

        namespace_client = None
        original_client = self.client

        username = os.environ.get("DGRAPH_USERNAME", "groot")
        password = os.environ.get("DGRAPH_PASSWORD", "password")

        try:
            try:
                namespace_id = self.client.create_namespace(timeout=10)
            except Exception as e:
                self.fail(f"Failed to create namespace: {type(e).__name__}: {e}")

            try:
                url = f"dgraph://{username}:{password}@{self.dgraph_host}:{self.dgraph_port}?namespace={namespace_id}"
                namespace_client = open(url)
                self.client = namespace_client
            except Exception as e:
                self.fail(
                    f"Failed to open client connection to namespace with username:password auth: {type(e).__name__}: {e}"
                )

            # Set up schema
            schema = """
                name: string @index(exact) .
            """
            try:
                helper.set_schema(self.client, schema)
            except Exception as e:
                self.fail(f"Failed to set schema: {type(e).__name__}: {e}")

            try:
                _ = self.client.run_dql(
                    dql_query="""
                    {
                        set {
                            _:alice <name> "Alice" .
                        }
                    }
                    """
                )
            except Exception as e:
                self.fail(
                    f"Failed to execute DQL mutation with username:password auth: {type(e).__name__}: {e}"
                )

            # Query with variables using run_dql (this tests run_dql query with username:password auth)
            query_dql_with_var = """query Alice($name: string) {
                alice(func: eq(name, $name)) {
                    name
                }
            }"""
            vars = {"$name": "Alice"}

            try:
                resp = self.client.run_dql(query_dql_with_var, vars, read_only=True)
            except Exception as e:
                self.fail(
                    f"Failed to execute DQL query with username:password auth: {type(e).__name__}: {e}"
                )

            try:
                m = json.loads(resp.json)
            except (json.JSONDecodeError, AttributeError) as e:
                self.fail(f"Failed to parse JSON response: {type(e).__name__}: {e}")

            self.assertEqual(len(m.get("alice", [])), 1)
            self.assertEqual(m["alice"][0]["name"], "Alice")

            try:
                resp = self.client.run_dql(
                    dql_query=query_dql_with_var,
                    vars=vars,
                    read_only=True,
                )
            except Exception as e:
                self.fail(
                    f"Failed to execute second DQL query with username:password auth: {type(e).__name__}: {e}"
                )

            try:
                m = json.loads(resp.json)
            except (json.JSONDecodeError, AttributeError) as e:
                self.fail(
                    f"Failed to parse JSON response from second query: {type(e).__name__}: {e}"
                )

            self.assertEqual(len(m.get("alice", [])), 1)
            self.assertEqual(m["alice"][0]["name"], "Alice")
        finally:
            if namespace_client is not None:
                try:
                    namespace_client.close()
                except Exception:  # nosec B110
                    pass
                self.client = original_client


def is_number(number):
    """Returns true if object is a number."""
    return isinstance(number, int)


def suite():
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestQueries())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
