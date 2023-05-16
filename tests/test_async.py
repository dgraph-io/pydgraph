# Copyright 2023 Dgraph Labs, Inc.
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

"""Tests to verify async client methods."""

__author__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'

import json
import pydgraph

from tests import helper

class TestAsync(helper.ClientIntegrationTestCase):
    server_addr = 'localhost:9180'

    def setUp(self):
        super(TestAsync, self).setUp()
        helper.drop_all(self.client)

    def test_mutation_and_query(self):
        """Runs mutation and queries asyncronously."""
        alter_future = self.client.async_alter(pydgraph.Operation(
            schema="name: string @index(term) ."))
        response = pydgraph.DgraphClient.handle_alter_future(alter_future)

        txn = self.client.txn()
        mutate_future = txn.async_mutate(pydgraph.Mutation(commit_now=True), set_nquads="""
            <_:alice> <name> \"Alice\" .
            <_:greg> <name> \"Greg\" .
            <_:alice> <follows> <_:greg> .
        """)
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
        query_future = txn.async_query(query, variables={'$a': 'Alice'})
        response = pydgraph.Txn.handle_query_future(query_future)
        self.assertEqual([{'name': 'Alice', 'follows': [{'name': 'Greg'}]}],
                         json.loads(response.json).get('me'))
