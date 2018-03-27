# Copyright 2016 Dgraph Labs, Inc.
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

__author__ = 'Mohit Ranka <mohitranka@gmail.com>'
__maintainer__ = 'Garvit Pahal <garvit@dgraph.io>'

import json

from pydgraph.proto import api_pb2 as api

from . import helper


class TestQueries(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestQueries, self).setUp()

        helper.drop_all(self.client)
        helper.set_schema(self.client, 'name: string @index(term) .')

    def test_mutation_and_query(self):
        txn = self.client.txn()
        assigned = txn.mutate(api.Mutation(commit_now=True), set_nquads="""
            <_:alice> <name> \"Alice\" .
            <_:greg> <name> \"Greg\" .
            <_:alice> <follows> <_:greg> .
        """)

        query_string = """{
            me(func: anyofterms(name, "Alice"))
            {
                name
                follows
                {
                    name
                }
            }
        }
        """
        response = self.client.query(query_string)
        self.assertEqual([{"name": "Alice", "follows": [{"name": "Greg"}]}], json.loads(response.json).get("me"))
        self.assertTrue(isinstance(response.latency.parsing_ns, int), 'Parsing latency is not available')
        self.assertTrue(isinstance(response.latency.processing_ns, int), 'Processing latency is not available')
        self.assertTrue(isinstance(response.latency.encoding_ns, int), 'Encoding latency is not available')
