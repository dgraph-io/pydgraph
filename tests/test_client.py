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

from pydgraph import client


class TestDgraphClient(unittest.TestCase):
    def test_constructor(self):
        with self.assertRaises(ValueError):
            client.DgraphClient()


def suite():
    s = unittest.TestSuite()
    s.addTest(TestDgraphClient())
    return s


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
