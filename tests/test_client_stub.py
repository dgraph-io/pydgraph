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

from pydgraph.client_stub import DgraphClientStub
from pydgraph.proto import api_pb2 as api


class TestDgraphClientStub(unittest.TestCase):
    def validate_version_object(self, version):
        tag = version.tag
        self.assertIsInstance(tag, str)

    def check_version(self, stub):
        self.validate_version_object(stub.check_version(api.Check()))

    def test_constructor(self):
        self.check_version(DgraphClientStub())
    
    def test_timeout(self):
        with self.assertRaises(Exception):
            DgraphClientStub().check_version(api.Check(), timeout=-1)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDgraphClientStub())
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
