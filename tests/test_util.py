# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests utility functions."""

from __future__ import annotations

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import unittest

from pydgraph import util


class TestUtil(unittest.TestCase):
    """Tests util utility functions."""

    def test_is_string(self) -> None:
        self.assertTrue(util.is_string(""))
        self.assertTrue(util.is_string("a"))
        self.assertFalse(util.is_string(object()))
        self.assertFalse(util.is_string({}))


def suite() -> unittest.TestSuite:
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestUtil())
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
