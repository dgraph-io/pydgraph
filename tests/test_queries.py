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

"""Tests behavior of queries after mutation in the same transaction."""

__author__ = 'Mohit Ranka <mohitranka@gmail.com>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'

import unittest
import sys
import logging
import json

import pydgraph

from . import helper


class TestQueries(helper.ClientIntegrationTestCase):
    """Tests behavior of queries after mutation in the same transaction."""

    def setUp(self):
        super(TestQueries, self).setUp()

        helper.drop_all(self.client)
        helper.set_schema(self.client, 'name: string @index(term) .')
        pydgraph.util.wait_for_indexing(self.client, "name", ["term"], False, False)

    def test_mutation_and_query(self):
        """Runs mutation and verifies queries see the results."""

        txn = self.client.txn()
        _ = txn.mutate(pydgraph.Mutation(commit_now=True), set_nquads="""
            <_:alice> <name> \"Alice\" .
            <_:greg> <name> \"Greg\" .
            <_:alice> <follows> <_:greg> .
        """)

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

        response = self.client.txn().query(query, variables={'$a': 'Alice'})
        self.assertEqual([{'name': 'Alice', 'follows': [{'name': 'Greg'}]}],
                         json.loads(response.json).get('me'))
        self.assertTrue(is_number(response.latency.parsing_ns),
                        'Parsing latency is not available')
        self.assertTrue(is_number(response.latency.processing_ns),
                        'Processing latency is not available')
        self.assertTrue(is_number(response.latency.encoding_ns),
                        'Encoding latency is not available')


def is_number(number):
    """Returns true if object is a number. Compatible with Python 2 and 3."""
    if sys.version_info[0] < 3:
        return isinstance(number, (int, long))

    return isinstance(number, int)


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestQueries())
    return suite_obj


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
