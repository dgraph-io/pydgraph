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

"""Tests to verify upsert block."""
__author__ = 'Animesh Pathak <animesh@dgrpah.io>'
__maintainer__ = 'Animesh Pathak <animesh@dgrpah.io>'

import unittest
import logging
import json

from . import helper


class TestUpsertBlock(helper.ClientIntegrationTestCase):
    """Tests for Upsert Block"""

    def setUp(self):
        super(TestUpsertBlock, self).setUp()
        helper.drop_all(self.client)
        helper.set_schema(self.client, 'name: string @index(term) @upsert .')

    def test_upsert_block_one_mutation(self):
        txn = self.client.txn()
        mutation = txn.create_mutation(set_nquads='_:animesh <name> "Animesh" .')
        request = txn.create_request(mutations=[mutation], commit_now=True)
        txn.do_request(request)

    def test_upsert_block_multiple_mutation(self):
        txn = self.client.txn()
        mutation1 = txn.create_mutation(set_nquads='_:animesh <name> "Animesh" .')
        mutation2 = txn.create_mutation(set_nquads='_:aman <name> "Aman" .')
        request = txn.create_request(mutations=[mutation1, mutation2], commit_now=True)
        try:
            txn.do_request(request)
            self.fail("Upsert block test failed: Multiple mutations succeeded")
        except Exception as e:
            txn.discard()
            self.assertTrue("Only 1 mutation per request is supported" in str(e))

    def test_one_mutation_one_query(self):
        txn = self.client.txn()
        mutation = txn.create_mutation(set_nquads='uid(u) <name> "Animesh" .')

        query = """
                {
                  me(func: eq(name, "Animesh")) {
                    u as uid
                  }
                }
                """

        request = txn.create_request(mutations=[mutation], query=query, commit_now=True)
        txn.do_request(request)

    def test_one_query(self):
        self.insert_sample_data()
        txn = self.client.txn()

        query = """
                {
                  me(func: eq(name, "Animesh")) {
                    name
                    uid
                  }
                }
                """

        request = txn.create_request(query=query)
        response = txn.do_request(request)
        data = json.loads(response.json)
        if len(data["me"]) <= 0:
            self.fail("Upsert block test failed: No data found in query")

    def test_no_query_no_mutation(self):
        txn = self.client.txn()
        request = txn.create_request()
        try:
            txn.do_request(request)
            self.fail("Upsert block test failed: Empty query succeeded")
        except Exception as e:
            txn.discard()
            self.assertTrue("Empty query" in str(e))

    def test_conditional_upsert(self):
        self.insert_sample_data()
        txn = self.client.txn()

        query = """
                {
                  u as var(func: eq(name, "Animesh"))
                }
                """

        mutation = txn.create_mutation(cond="@if(gt(len(u), 0))", set_nquads='uid(u) <name> "Ashish" .')
        request = txn.create_request(mutations=[mutation], query=query, commit_now=True)
        txn.do_request(request)
        self.was_upsert_successful()

    def test_bulk_set(self):
        rdfs = """
                   _:animesh <name> "Animesh" .
                   _:aman <name> "Aman" .
                   _:ashish <name> "Ashish" .
               """

        txn = self.client.txn()
        txn.mutate(set_nquads=rdfs, commit_now=True)

        query = """
                {
                    me(func: has(name)) {
                        u as uid
                    }
                }
                """

        txn = self.client.txn()
        mutation = txn.create_mutation(set_nquads='uid(u) <name> "Random" .')
        request = txn.create_request(mutations=[mutation], query=query, commit_now=True)
        txn.do_request(request)

        query = """
                {
                    me(func: eq(name, "Animesh")) {
                        uid
                    }
                }
                """

        txn = self.client.txn()
        response = txn.query(query)
        data = json.loads(response.json)['me']
        if len(data) > 0:
            self.fail("Upsert block test failed: Couldn't do bulk set")

    def test_json(self):
        txn = self.client.txn()
        data = {"uid": "_:animesh", "name": "Pathak"}
        txn.mutate(set_obj=data, commit_now=True)

    def insert_sample_data(self):
        txn = self.client.txn()
        txn.mutate(set_nquads='_:animesh <name> "Animesh" .', commit_now=True)

    def was_upsert_successful(self):
        query = """
                {
                  me(func: eq(name, "Animesh")) {
                    uid
                  }
                }
                """

        txn = self.client.txn()
        response = txn.query(query)
        data = json.loads(response.json)
        if len(data["me"]) != 0:
            self.fail("Upsert block test failed: Couldn't delete data.")

        query = """
                {
                  me(func: eq(name, "Ashish")) {
                    uid
                  }
                }
                """

        txn = self.client.txn()
        response = txn.query(query)
        data = json.loads(response.json)
        if len(data["me"]) != 1:
            self.fail("Upsert block test failed: Couldn't update data.")


def suite():
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestUpsertBlock())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
