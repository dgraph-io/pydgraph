# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

__author__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import logging
import unittest

from . import helper


class TestNamespaces(helper.ClientIntegrationTestCase):
    """Tests for the namespace management methods."""

    def setUp(self):
        super(TestNamespaces, self).setUp()
        helper.skip_if_dgraph_version_below(self.client, "25.0.0", self)
        helper.drop_all(self.client)

    def test_create_namespace(self):
        """Test creating a new namespace returns valid namespace ID."""
        namespace_id = self.client.create_namespace()

        # Verify we get a valid namespace ID
        self.assertIsInstance(namespace_id, int)
        self.assertGreater(namespace_id, 0)

        # Test creating another namespace gives us a different ID
        namespace_id2 = self.client.create_namespace()
        self.assertIsInstance(namespace_id2, int)
        self.assertGreater(namespace_id2, 0)
        self.assertNotEqual(namespace_id, namespace_id2)

    def test_list_namespaces(self):
        """Test listing namespaces returns a dictionary."""
        # Create a namespace first
        namespace_id = self.client.create_namespace()

        # List namespaces
        namespaces = self.client.list_namespaces()

        # Verify we get a dictionary
        self.assertIsInstance(namespaces, dict)

        # The created namespace should be in the list
        self.assertIn(namespace_id, namespaces)

        # Each namespace should have the expected structure
        namespace_obj = namespaces[namespace_id]
        self.assertTrue(hasattr(namespace_obj, "id"))
        self.assertEqual(namespace_obj.id, namespace_id)

    def test_drop_namespace(self):
        """Test dropping a namespace removes it from the list."""
        # Create a namespace
        namespace_id = self.client.create_namespace()

        # Verify it exists in the list
        namespaces_before = self.client.list_namespaces()
        self.assertIn(namespace_id, namespaces_before)

        # Only drop if it's not namespace 0 (system namespace cannot be deleted)
        if namespace_id != 0:
            # Drop the namespace
            self.client.drop_namespace(namespace_id)

            # Verify it's no longer in the list
            namespaces_after = self.client.list_namespaces()
            self.assertNotIn(namespace_id, namespaces_after)
        else:
            # If we got namespace 0, verify we can't drop it
            with self.assertRaises(Exception) as cm:
                self.client.drop_namespace(namespace_id)
            self.assertIn("cannot be deleted", str(cm.exception))

    def test_create_and_drop_multiple_namespaces(self):
        """Test creating and dropping multiple namespaces."""
        # Create multiple namespaces
        namespace_ids = []
        for _ in range(3):
            namespace_id = self.client.create_namespace()
            namespace_ids.append(namespace_id)

        # Verify all are in the list
        namespaces = self.client.list_namespaces()
        for namespace_id in namespace_ids:
            self.assertIn(namespace_id, namespaces)

        # Drop all namespaces (except namespace 0 which cannot be deleted)
        droppable_ids = [ns_id for ns_id in namespace_ids if ns_id != 0]
        for namespace_id in droppable_ids:
            self.client.drop_namespace(namespace_id)

        # Verify droppable namespaces are no longer in the list
        namespaces_after = self.client.list_namespaces()
        for namespace_id in droppable_ids:
            self.assertNotIn(namespace_id, namespaces_after)

        # If namespace 0 was created, it should still be in the list
        if 0 in namespace_ids:
            self.assertIn(0, namespaces_after)

    def test_cannot_drop_namespace_zero(self):
        """Test that namespace 0 cannot be dropped."""
        # Namespace 0 is the system namespace and cannot be deleted
        import grpc

        with self.assertRaises(grpc.RpcError) as cm:
            self.client.drop_namespace(0)
        self.assertIn("cannot be deleted", str(cm.exception))


def suite():
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestNamespaces())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
