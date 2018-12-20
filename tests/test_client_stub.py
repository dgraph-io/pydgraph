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

__author__ = 'Garvit Pahal <garvit@dgraph.io>'
__maintainer__ = 'Garvit Pahal <garvit@dgraph.io>'

import unittest
import sys

import pydgraph
from . import helper

class TestDgraphClientStub(helper.ClientIntegrationTestCase):

    def validate_version_object(self, version):
        tag = version.tag
        if sys.version_info[0] < 3:
            self.assertIsInstance(tag, basestring)
            return

        self.assertIsInstance(tag, str)

    def check_version(self, stub):
        self.validate_version_object(stub.check_version(pydgraph.Check()))

    def test_constructor(self):
        self.check_version(pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR))

    def test_timeout(self):
        with self.assertRaises(Exception):
            pydgraph.DgraphClientStub(self.TEST_SERVER_ADDR).check_version(
                pydgraph.Check(), timeout=-1)

    def test_close(self):
        client_stub = pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR)
        self.check_version(client_stub)
        client_stub.close()
        with self.assertRaises(Exception):
            client_stub.check_version(pydgraph.Check())


def suite():
    s = unittest.TestSuite()
    s.addTest(TestDgraphClientStub())
    return s


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
