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

from tests import helper


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
    suite_obj.addTest(TestIsString())
    return suite_obj


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
