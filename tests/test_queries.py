# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Tests behavior of queries after mutation in the same transaction."""

__author__ = "Mohit Ranka <mohitranka@gmail.com>"
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"

import json
import logging
import unittest

import pydgraph

from . import helper


class TestQueries(helper.ClientIntegrationTestCase):
    """Tests behavior of queries after mutation in the same transaction."""

    def setUp(self):
        super(TestQueries, self).setUp()

        helper.drop_all(self.client)
        helper.set_schema(self.client, "name: string @index(term) .")

    def test_check_version(self):
        """Verifies the check_version method correctly returns the cluster version"""
        success = 0
        for _i in range(3):
            try:
                tag = self.client.check_version()
                self.assertGreater(len(tag), 0)
                success += 1
            except Exception:
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

        queryRDF = """query q($a: string) {
            q(func: anyofterms(name, "Alice"))
            {
                uid
                name
            }
        }"""

        response = self.client.txn().query(query, variables={"$a": "Alice"})
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

        """ Call run_dql and verify the result """
        response = self.client.run_dql(
            dql_query=query,
            vars={"$a": "Alice"},
            resp_format="JSON",
            read_only=True,
        )
        self.assertEqual(
            [{"name": "Alice", "follows": [{"name": "Greg"}]}],
            json.loads(response.json).get("me"),
        )


def is_number(number):
    """Returns true if object is a number"""
    return isinstance(number, int)


def suite():
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestQueries())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
