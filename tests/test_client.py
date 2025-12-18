# SPDX-FileCopyrightText: Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests construction of Dgraph client."""

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. "

import unittest

import pydgraph


class TestDgraphClient(unittest.TestCase):
    """Tests construction of Dgraph client."""

    def test_constructor(self):
        with self.assertRaises(ValueError):
            pydgraph.DgraphClient()


def suite():
    """Returns a tests suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(unittest.makeSuite(TestDgraphClient))
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
