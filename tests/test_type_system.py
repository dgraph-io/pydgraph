# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Tests to verify type system."""
__author__ = "Animesh Pathak <animesh@dgrpah.io>"
__maintainer__ = "Animesh Pathak <animesh@dgrpah.io>"

import json
import logging
import unittest

from tests import helper


class TestTypeSystem(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestTypeSystem, self).setUp()
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

    def test_type_deletion_failure(self):
        """It tries to delete all predicates of a node without having any type"""

        rdfs = """
                   _:animesh <name> "Animesh" .
                   _:animesh <age> "24" .
               """

        self.insert_delete_and_check(rdfs, 1)

    def test_type_deletion(self):

        rdfs = """
                   _:animesh <name> "Animesh" .
                   _:animesh <age> "24" .
                   _:animesh <dgraph.type> "Person" .
               """

        self.insert_delete_and_check(rdfs, 0)

    def insert_delete_and_check(self, rdfs, expected_result_count=0):
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


def suite():
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestTypeSystem())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
