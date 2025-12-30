# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests to verify type system."""

from __future__ import annotations

import json
import logging
import unittest

from tests import helper

__author__ = "Animesh Pathak <animesh@dgrpah.io>"
__maintainer__ = "Istari Digital <contact@istaridigital.com>"


class TestTypeSystem(helper.ClientIntegrationTestCase):
    def setUp(self) -> None:
        super().setUp()
        helper.drop_all(self.client)

        schema = """
                    type Person {
                      name
                      age
                    }
                    name: string @index(term, exact) .
                    age: int .
                """

        helper.set_schema(self.client, schema)

    def test_type_deletion_failure(self) -> None:
        """It tries to delete all predicates of a node without having any type"""

        rdfs = """
                   _:animesh <name> "Animesh" .
                   _:animesh <age> "24" .
               """

        self.insert_delete_and_check(rdfs, 1)

    def test_type_deletion(self) -> None:
        rdfs = """
                   _:animesh <name> "Animesh" .
                   _:animesh <age> "24" .
                   _:animesh <dgraph.type> "Person" .
               """

        self.insert_delete_and_check(rdfs, 0)

    def insert_delete_and_check(
        self, rdfs: str, expected_result_count: int = 0
    ) -> None:
        txn = self.client.txn()
        txn.mutate(set_nquads=rdfs, commit_now=True)

        query = """
                {
                  u as var(func: eq(name, "Animesh"))
                }
                """

        txn = self.client.txn()
        mutation = txn.create_mutation(del_nquads="uid(u) * * .")
        request = txn.create_request(mutations=[mutation], query=query, commit_now=True)
        txn.do_request(request)

        query = """
                {
                  me(func: eq(name, "Animesh")) {
                    name
                  }
                }
                """

        txn = self.client.txn()
        response = txn.query(query=query)
        data = json.loads(response.json)
        if len(data["me"]) != expected_result_count:
            self.fail("Type system test failed: Error while deleting predicates.")


def suite() -> unittest.TestSuite:
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestTypeSystem())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
