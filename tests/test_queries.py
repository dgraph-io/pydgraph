# Copyright 2016 DGraph Labs, Inc.
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
__maintainer__ = 'Mohit Ranka <mohitranka@gmail.com>'

import unittest
from pydgraph.client import DgraphClient


class DgraphClientTestCases(unittest.TestCase):
    TEST_HOSTNAME = 'localhost'
    TEST_PORT = 8081

    def setUp(self):
        self.client = DgraphClient(self.TEST_HOSTNAME, self.TEST_PORT)

    def test_mutation_and_query(self):
        query_string = """
        mutation
        {
            set
            {
                <alice> <name> \"Alice\" .
                <greg> <name> \"Greg\" .
                <alice> <follows> <greg> .
            }
        }

        query
        {
            me(_xid_: alice)
            {
                follows
                {
                    name _xid_
                }
            }
        }
        """
        response = self.client.query(query_string)
        self.assertEqual(response.n.children[0].xid, "greg")
        self.assertEqual(response.n.children[0].attribute, "follows")
        self.assertEqual(response.n.children[0].properties[0].prop, "name")
        self.assertEqual(response.n.children[0].properties[0].val, "Greg")
        self.assertTrue(isinstance(response.l.parsing, basestring), 'Parsing latency is not available')
        self.assertTrue(isinstance(response.l.pb, basestring), 'Protocol buffers latency is not available')
        self.assertTrue(isinstance(response.l.processing, basestring), 'Processing latency is not available')

    def test_mutation_and_query_two_levels(self):
        query_string = """
        mutation
        {
            set
            {
                <bob> <name> \"Bob\" .
                <josh> <name> \"Josh\" .
                <rose> <name> \"Rose\" .
                <bob> <follows> <josh> .
                <josh> <follows> <rose> .
            }
        }

        query
        {
            me(_xid_: bob)
            {
                name _xid_ follows
                {
                    name _xid_ follows
                    {
                        name _xid_
                    }
                }
            }
        }
        """
        response = self.client.query(query_string)

        self.assertEqual(len(response.n.children), 1)
        self.assertEqual(response.n.children[0].xid, "josh")
        self.assertEqual(response.n.children[0].attribute, "follows")
        self.assertEqual(response.n.children[0].properties[0].prop, "name")
        self.assertEqual(response.n.children[0].properties[0].val, "Josh")

        self.assertEqual(len(response.n.children[0].children), 1)
        self.assertEqual(response.n.children[0].children[0].xid, "rose")
        self.assertEqual(response.n.children[0].children[0].attribute, "follows")
        self.assertEqual(response.n.children[0].children[0].properties[0].prop, "name")
        self.assertEqual(response.n.children[0].children[0].properties[0].val, "Rose")


        self.assertTrue(isinstance(response.l.parsing, basestring), 'Parsing latency is not available')
        self.assertTrue(isinstance(response.l.pb, basestring), 'Protocol buffers latency is not available')
        self.assertTrue(isinstance(response.l.processing, basestring), 'Processing latency is not available')

