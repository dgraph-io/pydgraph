# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Tests for V2 API operations."""

__author__ = "Cascade"
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"

import os
import random
import unittest

from pydgraph import open
from pydgraph.errors import V2NotSupportedError
from pydgraph.proto.api_v2_pb2 import (
    CreateNamespaceResponse,
    DropNamespaceResponse,
    ListNamespacesResponse,
    PingResponse,
)


class TestV2Ops(unittest.TestCase):
    client = None

    @classmethod
    def setUpClass(cls):
        """Set up a client for all tests."""
        addr = os.environ.get("TEST_SERVER_ADDR", "localhost:9080")
        cls.client = open(f"dgraph://{addr}")

    @classmethod
    def tearDownClass(cls):
        """Tear down the client."""
        if cls.client:
            cls.client.close()

    def test_ping(self):
        """Test the ping method."""
        self.assertIsNotNone(self.client, "Client is not initialized")

        version_info = self.client.check_version()
        print(f"version_info: {version_info}")
        major_version_str = version_info.lstrip("v").split(".")[0]
        is_v1 = int(major_version_str) < 25

        if is_v1:
            with self.assertRaises(V2NotSupportedError):
                self.client.ping()
        else:
            response = self.client.ping()
            self.assertIsNotNone(response, "Ping response is None")
            self.assertIsInstance(
                response, PingResponse, "Response is not a PingResponse"
            )
            self.assertTrue(response.version, "Ping response has no version")
            print(f"Dgraph version (from ping): {response.version}")

    def test_namespace(self):
        """Test the namespace operations."""
        self.assertIsNotNone(self.client, "Client is not initialized")

        # generate a random namespace name
        namespace = "test_" + str(random.randint(0, 10000))  # trunk-ignore(bandit/B311)
        version_info = self.client.check_version()
        major_version_str = version_info.lstrip("v").split(".")[0]
        is_v1 = int(major_version_str) < 25

        if is_v1:
            with self.assertRaises(V2NotSupportedError):
                self.client.create_namespace(namespace)
        else:
            response = self.client.create_namespace(namespace)
            self.assertIsNotNone(response, "Create namespace response is None")
            self.assertIsInstance(
                response,
                CreateNamespaceResponse,
                "Response is not a CreateNamespaceResponse",
            )

            response = self.client.list_namespaces()
            self.assertIsNotNone(
                response,
                "List namespaces response is None",
            )
            self.assertIsInstance(
                response,
                ListNamespacesResponse,
                "Response is not a ListNamespacesResponse",
            )
            self.assertIn(namespace, response.ns_list, "Namespace not found in list")

            response = self.client.drop_namespace(namespace)
            self.assertIsNotNone(response, "Drop namespace response is None")
            self.assertIsInstance(
                response,
                DropNamespaceResponse,
                "Response is not a DropNamespaceResponse",
            )


if __name__ == "__main__":
    unittest.main()
