# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

__author__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import logging
import time
import unittest

import pytest

from . import helper


class TestNamespaces(helper.ClientIntegrationTestCase):
    """Tests for the namespace management methods."""

    def setUp(self) -> None:
        super().setUp()
        helper.skip_if_dgraph_version_below(self.client, "25.0.0", self)
        helper.drop_all(self.client)

    def _wait_for_namespace_deletion(
        self, namespace_id: int, max_retries: int = 5, initial_delay: float = 0.1
    ) -> None:
        """Wait for namespace deletion to propagate.

        Namespace deletion is eventually consistent in Dgraph. This helper
        implements exponential backoff to wait for the deletion to propagate
        across the cluster before verifying the namespace is gone.

        Args:
            namespace_id: The namespace ID to check for deletion
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds (doubles each retry)

        Raises:
            AssertionError: If namespace still exists after all retries
        """
        delay = initial_delay
        for attempt in range(max_retries):
            namespaces = self.client.list_namespaces()
            if namespace_id not in namespaces:
                return  # Deletion propagated successfully

            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2  # Exponential backoff

        # Final check after all retries
        namespaces = self.client.list_namespaces()
        assert namespace_id not in namespaces, f"Namespace {namespace_id} still exists after {max_retries} retries"

    def test_create_namespace(self) -> None:
        """Test creating a new namespace returns valid namespace ID."""
        namespace_id = self.client.create_namespace()

        # Verify we get a valid namespace ID
        assert isinstance(namespace_id, int)
        assert namespace_id > 0

        # Test creating another namespace gives us a different ID
        namespace_id2 = self.client.create_namespace()
        assert isinstance(namespace_id2, int)
        assert namespace_id2 > 0
        assert namespace_id != namespace_id2

    def test_list_namespaces(self) -> None:
        """Test listing namespaces returns a dictionary."""
        # Create a namespace first
        namespace_id = self.client.create_namespace()

        # List namespaces
        namespaces = self.client.list_namespaces()

        # Verify we get a dictionary
        assert isinstance(namespaces, dict)

        # The created namespace should be in the list
        assert namespace_id in namespaces

        # Each namespace should have the expected structure
        namespace_obj = namespaces[namespace_id]
        assert hasattr(namespace_obj, "id")
        assert namespace_obj.id == namespace_id

    def test_drop_namespace(self) -> None:
        """Test dropping a namespace removes it from the list."""
        # Create a namespace
        namespace_id = self.client.create_namespace()

        # Verify it exists in the list
        namespaces_before = self.client.list_namespaces()
        assert namespace_id in namespaces_before

        # Only drop if it's not namespace 0 (system namespace cannot be deleted)
        if namespace_id != 0:
            # Drop the namespace
            self.client.drop_namespace(namespace_id)

            # Wait for deletion to propagate (eventual consistency)
            # Namespace operations are eventually consistent across the cluster,
            # so we use retry logic with exponential backoff
            self._wait_for_namespace_deletion(namespace_id)
        else:
            # If we got namespace 0, verify we can't drop it
            with pytest.raises(Exception, match="cannot be deleted"):
                self.client.drop_namespace(namespace_id)

    def test_create_and_drop_multiple_namespaces(self) -> None:
        """Test creating and dropping multiple namespaces."""
        # Create multiple namespaces
        namespace_ids = []
        for _ in range(3):
            namespace_id = self.client.create_namespace()
            namespace_ids.append(namespace_id)

        # Verify all are in the list
        namespaces = self.client.list_namespaces()
        for namespace_id in namespace_ids:
            assert namespace_id in namespaces

        # Drop all namespaces (except namespace 0 which cannot be deleted)
        droppable_ids = [ns_id for ns_id in namespace_ids if ns_id != 0]
        for namespace_id in droppable_ids:
            self.client.drop_namespace(namespace_id)

        # Wait for all deletions to propagate (eventual consistency)
        # Namespace deletion operations are eventually consistent across the
        # Dgraph cluster. We verify each deletion separately with retry logic
        # to accommodate the propagation delay in CI environments.
        for namespace_id in droppable_ids:
            self._wait_for_namespace_deletion(namespace_id)

        # Verify all droppable namespaces are no longer in the list
        # (this check should pass immediately after _wait_for_namespace_deletion,
        # but we keep it for explicitness)
        namespaces_after = self.client.list_namespaces()
        for namespace_id in droppable_ids:
            assert namespace_id not in namespaces_after

        # If namespace 0 was created, it should still be in the list
        if 0 in namespace_ids:
            assert 0 in namespaces_after

    def test_cannot_drop_namespace_zero(self) -> None:
        """Test that namespace 0 cannot be dropped."""
        # Namespace 0 is the system namespace and cannot be deleted
        import grpc

        with pytest.raises(grpc.RpcError) as cm:
            self.client.drop_namespace(0)
        assert "cannot be deleted" in str(cm.value)


def suite() -> unittest.TestSuite:
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestNamespaces())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
