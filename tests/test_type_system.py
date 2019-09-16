# Copyright 2019 Dgraph Labs, Inc.
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

"""Tests to verify type system."""
__author__ = 'Animesh Pathak <animesh@dgrpah.io>'
__maintainer__ = 'Animesh Pathak <animesh@dgrpah.io>'

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
                      name: string
                      age: int
                    }
                    name: string @index(term, exact) .
                    age: int .
                """

        helper.set_schema(self.client, schema)

    def test_type_function(self):
        txn = self.client.txn()

        rdfs = """
                   _:animesh <name> "Animesh" .
                   _:animesh <age> "24" .
                   _:animesh <dgraph.type> "Person" .
               """

        txn.mutate(set_nquads=rdfs)

        query = """
                {
                  me(func: type(Person)) {
                    expand(_all_)
                  }
                }
                """

        response = txn.query(query)
        data = json.loads(response.json)
        if len(data["me"]) != 1:
            self.fail("Type system test failed: No Person type node found")

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
        mutation = txn.create_mutation(del_nquads='uid(u) * * .')
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
