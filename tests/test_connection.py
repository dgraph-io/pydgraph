# Copyright 2018 Dgraph Labs, Inc.
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

"""Tests connection error."""

__author__ = 'Brilant Kasami <brilant@hexoon.com>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'

import unittest
import logging
import pydgraph

from . import helper


class TestConnection(helper.ClientIntegrationTestCase):
    TEST_SERVER_ADDR = "localhost:9939" #random inexistent conn
    """Tests mutation after query behavior."""
    def setUp(self):
        self.client = helper.create_client(self.TEST_SERVER_ADDR)

    def testConnectionLogin(self):
        with self.assertRaises(pydgraph.ConnectionError):
            self.client.login("groot", "password")

    def testConnectionAlter(self):
        with self.assertRaises(pydgraph.ConnectionError):
            helper.set_schema(self.client, 'name: string @index(term) .')


    def testConnectionFailedTxn(self):
        with self.assertRaises(pydgraph.ConnectionError):
            """Tests what happens when making a mutation on a txn after querying."""

            _ = self.client.txn(read_only=True).query('{firsts(func: has(first)) { uid first }}')

def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestConnection())
    return suite_obj


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
