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

"""Tests utility functions."""

__author__ = 'Garvit Pahal <garvit@dgraph.io>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'

import unittest

from pydgraph import util
import pydgraph

from . import helper


class TestMergeLinReads(unittest.TestCase):
    """Tests merge_lin_reads utility function."""

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
        lr2 = pydgraph.LinRead()
        res = helper.create_lin_read({1: 1})
        self.common_test(lr1, lr2, res)

    def test_no_target_ids(self):
        lr1 = pydgraph.LinRead()
        lr2 = helper.create_lin_read({1: 1})
        res = helper.create_lin_read({1: 1})
        self.common_test(lr1, lr2, res)


class TestIsString(unittest.TestCase):
    """Tests is_string utility function."""

    def test_is_string(self):
        self.assertTrue(util.is_string(''))
        self.assertTrue(util.is_string('a'))
        self.assertFalse(util.is_string(object()))
        self.assertFalse(util.is_string({}))


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestMergeLinReads())
    suite_obj.addTest(TestIsString())
    return suite_obj


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
