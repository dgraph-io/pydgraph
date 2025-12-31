# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
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


class TestDgraphClientStubContextManager(helper.ClientIntegrationTestCase):
    def setUp(self) -> None:
        super().setUp()

    def check_version(self, stub: Any) -> None:
        """Helper method to check version using the stub."""
        version = stub.check_version(pydgraph.Check())
        assert version is not None

    def test_context_manager(self) -> None:
        """Test basic context manager usage for DgraphClientStub."""
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
            ver = client_stub.check_version(pydgraph.Check())
            assert ver is not None

    def test_context_manager_code_exception(self) -> None:
        """Test that exceptions within context manager are properly handled."""
        with (
            pytest.raises(AttributeError),
            pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub,
        ):
            client_stub.nonexistent_method()  # type: ignore[attr-defined]  # Intentionally calling non-existent method

    def test_context_manager_function_wrapper(self) -> None:
        """Test the client_stub() function wrapper for context manager."""
        with pydgraph.client_stub(addr=self.TEST_SERVER_ADDR) as client_stub:
            ver = client_stub.check_version(pydgraph.Check())
            assert ver is not None

    def test_context_manager_closes_stub(self) -> None:
        """Test that the stub is properly closed after exiting context manager."""
        stub = None
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
            stub = client_stub
            ver = client_stub.check_version(pydgraph.Check())
            assert ver is not None

        # After exiting context, stub should be closed and unusable
        with pytest.raises(Exception):  # noqa: PT011
            stub.check_version(pydgraph.Check())

    def test_context_manager_with_client(self) -> None:
        """Test using DgraphClientStub context manager with DgraphClient."""
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
            client = pydgraph.DgraphClient(client_stub)
            client.login("groot", "password")

            # Perform a simple operation
            txn = client.txn(read_only=True)
            query = "{ me(func: has(name)) { name } }"
            resp = txn.query(query)
            assert resp is not None

    def test_context_manager_exception_still_closes(self) -> None:
        """Test that stub is closed even when an exception occurs."""
        stub_ref = None
        try:
            with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
                stub_ref = client_stub
                client_stub.check_version(pydgraph.Check())
                raise ValueError("Test exception")  # noqa: TRY301
        except ValueError:
            pass

        # Stub should still be closed despite the exception
        assert stub_ref is not None
        with pytest.raises(Exception):  # noqa: PT011
            stub_ref.check_version(pydgraph.Check())

    def test_context_manager_function_wrapper_closes(self) -> None:
        """Test that client_stub() function wrapper properly closes the stub."""
        stub_ref = None
        with pydgraph.client_stub(addr=self.TEST_SERVER_ADDR) as client_stub:
            stub_ref = client_stub
            ver = client_stub.check_version(pydgraph.Check())
            assert ver is not None

        # Stub should be closed after exiting
        with pytest.raises(Exception):  # noqa: PT011
            stub_ref.check_version(pydgraph.Check())

    def test_context_manager_multiple_operations(self) -> None:
        """Test performing multiple operations within context manager."""
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as client_stub:
            # Check version multiple times
            ver1 = client_stub.check_version(pydgraph.Check())
            ver2 = client_stub.check_version(pydgraph.Check())
            assert ver1 is not None
            assert ver2 is not None

            # Create client and perform operations
            client = pydgraph.DgraphClient(client_stub)
            client.login("groot", "password")
            txn = client.txn(read_only=True)
            query = "{ me(func: has(name)) { name } }"
            resp = txn.query(query)
            assert resp is not None

    def test_context_manager_nested_with_client_operations(self) -> None:
        """Test full workflow: stub context manager with client and transaction operations."""
        with pydgraph.DgraphClientStub(addr=self.TEST_SERVER_ADDR) as stub:
            client = pydgraph.DgraphClient(stub)
            client.login("groot", "password")

            # Set schema
            schema = "test_name: string @index(fulltext) ."
            op = pydgraph.Operation(schema=schema)
            client.alter(op)

            # Perform mutation and query
            with client.txn() as txn:
                response = txn.mutate(set_obj={"test_name": "ContextManagerTest"})
                assert len(response.uids) == 1
                uid = next(iter(response.uids.values()))

            # Verify data was committed
            query = f"""{{
                me(func: uid("{uid}")) {{
                    test_name
                }}
            }}"""

            with client.txn(read_only=True) as txn:
                resp = txn.query(query)
                import json

                results = json.loads(resp.json).get("me")
                assert results == [{"test_name": "ContextManagerTest"}]


def suite() -> unittest.TestSuite:
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestDgraphClientStub())
    suite_obj.addTest(TestDgraphClientStubContextManager())
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
