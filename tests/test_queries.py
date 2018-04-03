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

import unittest
import sys
import logging
import json

import pydgraph

from . import helper


class TestQueries(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestQueries, self).setUp()

        helper.drop_all(self.client)
        helper.set_schema(self.client, 'name: string @index(term) .')

    def test_mutation_and_query(self):
        txn = self.client.txn()
        _ = txn.mutate(pydgraph.Mutation(commit_now=True), set_nquads="""
            <_:alice> <name> \"Alice\" .
            <_:greg> <name> \"Greg\" .
            <_:alice> <follows> <_:greg> .
        """)

        query = """{
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

        response = self.client.query(query)
        self.assertEqual([{'name': 'Alice', 'follows': [{'name': 'Greg'}]}], json.loads(response.json).get('me'))
        self.assertTrue(is_number(response.latency.parsing_ns), 'Parsing latency is not available')
        self.assertTrue(is_number(response.latency.processing_ns), 'Processing latency is not available')
        self.assertTrue(is_number(response.latency.encoding_ns), 'Encoding latency is not available')


def is_number(n):
    if sys.version_info[0] < 3:
        return isinstance(n, (int, long))

    return isinstance(n, int)


def suite():
    s = unittest.TestSuite()
    s.addTest(TestQueries())
    return s


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
