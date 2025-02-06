# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Tests utility functions."""

__author__ = "Garvit Pahal"
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"

import unittest

from pydgraph import util


class TestUtil(unittest.TestCase):
    """Tests util utility functions."""

    def test_is_string(self):
        self.assertTrue(util.is_string(""))
        self.assertTrue(util.is_string("a"))
        self.assertFalse(util.is_string(object()))
        self.assertFalse(util.is_string({}))


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestUtil())
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
