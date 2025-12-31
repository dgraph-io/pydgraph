# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests client stub."""

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import unittest

import pydgraph

from . import helper


class TestDgraphClientStub(helper.ClientIntegrationTestCase):
    """Tests client stub."""

    def validate_version_object(self, version):
        tag = version.tag
        self.assertIsInstance(tag, str)

    def check_version(self, stub):
        self.validate_version_object(stub.check_version(pydgraph.Check()))

    def test_constructor(self):
        self.check_version(pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR))

    def test_timeout(self):
        with self.assertRaises(Exception):  # noqa: B017
            pydgraph.DgraphClientStub(self.TEST_SERVER_ADDR).check_version(
                pydgraph.Check(), timeout=-1
            )

    def test_close(self):
        client_stub = pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR)
        self.check_version(client_stub)
        client_stub.close()
        with self.assertRaises(Exception):  # noqa: B017
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


class TestDgraphClientStubContextManager(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestDgraphClientStubContextManager, self).setUp()

    def test_context_manager(self):
        """Test basic context manager usage for DgraphClientStub."""
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
            ver = client_stub.check_version(pydgraph.Check())
            self.assertIsNotNone(ver)

    def test_context_manager_code_exception(self):
        """Test that exceptions within context manager are properly handled."""
        with self.assertRaises(AttributeError):
            with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
                self.check_version(client_stub)  # AttributeError: no such method

    def test_context_manager_function_wrapper(self):
        """Test the client_stub() function wrapper for context manager."""
        with pydgraph.client_stub(addr=self.TEST_SERVER_ADDR) as client_stub:
            ver = client_stub.check_version(pydgraph.Check())
            self.assertIsNotNone(ver)

    def test_context_manager_closes_stub(self):
        """Test that the stub is properly closed after exiting context manager."""
        stub = None
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
            stub = client_stub
            ver = client_stub.check_version(pydgraph.Check())
            self.assertIsNotNone(ver)

        # After exiting context, stub should be closed and unusable
        with self.assertRaises(Exception):  # noqa: B017
            stub.check_version(pydgraph.Check())

    def test_context_manager_with_client(self):
        """Test using DgraphClientStub context manager with DgraphClient."""
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
            client = pydgraph.DgraphClient(client_stub)

            # Perform a simple operation
            txn = client.txn(read_only=True)
            query = "{ me(func: has(name)) { name } }"
            resp = txn.query(query)
            self.assertIsNotNone(resp)

    def test_context_manager_exception_still_closes(self):
        """Test that stub is closed even when an exception occurs."""
        stub_ref = None
        try:
            with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
                stub_ref = client_stub
                client_stub.check_version(pydgraph.Check())
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Stub should still be closed despite the exception
        with self.assertRaises(Exception):  # noqa: B017
            stub_ref.check_version(pydgraph.Check())

    def test_context_manager_function_wrapper_closes(self):
        """Test that client_stub() function wrapper properly closes the stub."""
        stub_ref = None
        with pydgraph.client_stub(addr=self.TEST_SERVER_ADDR) as client_stub:
            stub_ref = client_stub
            ver = client_stub.check_version(pydgraph.Check())
            self.assertIsNotNone(ver)

        # Stub should be closed after exiting
        with self.assertRaises(Exception):  # noqa: B017
            stub_ref.check_version(pydgraph.Check())

    def test_context_manager_multiple_operations(self):
        """Test performing multiple operations within context manager."""
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
            # Check version multiple times
            ver1 = client_stub.check_version(pydgraph.Check())
            ver2 = client_stub.check_version(pydgraph.Check())
            self.assertIsNotNone(ver1)
            self.assertIsNotNone(ver2)

            # Create client and perform operations
            client = pydgraph.DgraphClient(client_stub)
            txn = client.txn(read_only=True)
            query = "{ me(func: has(name)) { name } }"
            resp = txn.query(query)
            self.assertIsNotNone(resp)

    def test_context_manager_nested_with_client_operations(self):
        """Test full workflow: stub context manager with client and transaction operations."""
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as stub:
            client = pydgraph.DgraphClient(stub)

            # Set schema
            schema = "test_name: string @index(fulltext) ."
            op = pydgraph.Operation(schema=schema)
            client.alter(op)

            # Perform mutation and query
            with client.txn() as txn:
                response = txn.mutate(set_obj={"test_name": "ContextManagerTest"})
                self.assertEqual(1, len(response.uids))
                uid = list(response.uids.values())[0]

            # Verify data was committed
            query = """{{
                me(func: uid("{uid}")) {{
                    test_name
                }}
            }}""".format(
                uid=uid
            )

            with client.txn(read_only=True) as txn:
                resp = txn.query(query)
                import json

                results = json.loads(resp.json).get("me")
                self.assertEqual([{"test_name": "ContextManagerTest"}], results)


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestDgraphClientStub())
    suite_obj.addTest(TestDgraphClientStubContextManager())
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
