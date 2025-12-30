# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests client stub."""

from __future__ import annotations

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import unittest
from typing import Any

import pytest

import pydgraph

from . import helper


class TestDgraphClientStub(helper.ClientIntegrationTestCase):
    """Tests client stub."""

    def validate_version_object(self, version: Any) -> None:
        tag = version.tag
        assert isinstance(tag, str)

    def check_version(self, stub: Any) -> None:
        self.validate_version_object(stub.check_version(pydgraph.Check()))

    def test_constructor(self) -> None:
        self.check_version(pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR))

    def test_timeout(self) -> None:
        with pytest.raises(Exception):  # noqa: PT011 - gRPC timeout error
            pydgraph.DgraphClientStub(self.TEST_SERVER_ADDR).check_version(
                pydgraph.Check(), timeout=-1
            )

    def test_close(self) -> None:
        client_stub = pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR)
        self.check_version(client_stub)
        client_stub.close()
        with pytest.raises(Exception):  # noqa: PT011 - gRPC channel closed error
            client_stub.check_version(pydgraph.Check())


class TestFromCloud(unittest.TestCase):
    """Tests the from_cloud function"""

    def test_from_cloud(self) -> None:
        testcases: list[dict[str, Any]] = [
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
                pydgraph.DgraphClientStub.from_cloud(case["endpoint"], "api-key")  # type: ignore[arg-type]
            except IndexError:
                if not case.get("error", False):
                    # we didn't expect an error
                    raise


def suite() -> unittest.TestSuite:
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestDgraphClientStub())
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
