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

from pydgraph import util
from pydgraph.proto import api_pb2 as api

from . import helper


class TestMergeLinReads(unittest.TestCase):
    def common_test(self, lr1, lr2, expected):
        self.assertTrue(helper.are_lin_reads_equal(util.merge_lin_reads(lr1, lr2), expected))
        self.assertTrue(helper.are_lin_reads_equal(lr1, expected))

    def test_disjoint(self):
        lr1 = helper.create_lin_read({1: 1})
        lr2 = helper.create_lin_read({2: 2, 3: 3})
        res = helper.create_lin_read({1: 1, 2: 2, 3: 3})
        self.common_test(lr1, lr2, res)
    
    def test_lower_value(self):
        lr1 = helper.create_lin_read({1: 2})
        lr2 = helper.create_lin_read({1: 1})
        res = helper.create_lin_read({1: 2})
        self.common_test(lr1, lr2, res)
    
    def test_higher_value(self):
        lr1 = helper.create_lin_read({1: 1})
        lr2 = helper.create_lin_read({1: 2})
        res = helper.create_lin_read({1: 2})
        self.common_test(lr1, lr2, res)
    
    def test_equal_value(self):
        lr1 = helper.create_lin_read({1: 1})
        lr2 = helper.create_lin_read({1: 1})
        res = helper.create_lin_read({1: 1})
        self.common_test(lr1, lr2, res)
    
    def test_none(self):
        lr1 = helper.create_lin_read({1: 1})
        lr2 = None
        res = helper.create_lin_read({1: 1})
        self.common_test(lr1, lr2, res)
    
    def test_no_src_ids(self):
        lr1 = helper.create_lin_read({1: 1})
        lr2 = api.LinRead()
        res = helper.create_lin_read({1: 1})
        self.common_test(lr1, lr2, res)
    
    def test_no_target_ids(self):
        lr1 = api.LinRead()
        lr2 = helper.create_lin_read({1: 1})
        res = helper.create_lin_read({1: 1})
        self.common_test(lr1, lr2, res)


class TestIsString(unittest.TestCase):
    def test_is_string(self):
        self.assertTrue(util.is_string(''))
        self.assertTrue(util.is_string('a'))
        self.assertFalse(util.is_string(object()))
        self.assertFalse(util.is_string({}))


def suite():
    s = unittest.TestSuite()
    s.addTest(TestMergeLinReads())
    s.addTest(TestIsString())
    return s


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
