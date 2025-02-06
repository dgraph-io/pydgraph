# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Tests client stub."""

__author__ = "Garvit Pahal"
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"

import sys
import unittest

import pydgraph

from . import helper


class TestDgraphClientStub(helper.ClientIntegrationTestCase):
    """Tests client stub."""

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
                pydgraph.Check(), timeout=-1
            )

    def test_close(self):
        client_stub = pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR)
        self.check_version(client_stub)
        client_stub.close()
        with self.assertRaises(Exception):
            client_stub.check_version(pydgraph.Check())


class TestFromCloud(unittest.TestCase):
    """Tests the from_cloud function"""

    def test_from_cloud(self):
        testcases = [
            {"endpoint": "godly.grpc.region.aws.cloud.dgraph.io"},
            {"endpoint": "godly.grpc.region.aws.cloud.dgraph.io:443"},
            {"endpoint": "https://godly.grpc.region.aws.cloud.dgraph.io:443"},
            {"endpoint": "https://godly.region.aws.cloud.dgraph.io/graphql"},
            {"endpoint": "godly.region.aws.cloud.dgraph.io"},
            {"endpoint": "https://godly.region.aws.cloud.dgraph.io"},
            {"endpoint": "godly.region.aws.cloud.dgraph.io:random"},
            {"endpoint": "random:url", "error": True},
            {"endpoint": "google", "error": True},
        ]

        for case in testcases:
            try:
                pydgraph.DgraphClientStub.from_cloud(case["endpoint"], "api-key")
            except IndexError as e:
                if not case["error"]:
                    # we didn't expect an error
                    raise (e)


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestDgraphClientStub())
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
