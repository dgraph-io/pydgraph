# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the allocation methods (allocate_uids, allocate_timestamps, allocate_namespaces)."""

from __future__ import annotations

import logging
import unittest

import pytest

from . import helper

__author__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"


class TestAllocations(helper.ClientIntegrationTestCase):
    """Tests for the allocation methods."""

    def setUp(self) -> None:
        super().setUp()
        helper.skip_if_dgraph_version_below(self.client, "25.0.0", self)
        helper.drop_all(self.client)

    def test_allocate_uids(self) -> None:
        """Test allocating UIDs returns valid range."""
        how_many = 100
        start, end = self.client.allocate_uids(how_many)

        # Verify we get a valid range
        assert isinstance(start, int)
        assert isinstance(end, int)
        assert start > 0
        assert end > start
        assert end - start == how_many

        # Test allocating again gives us a different range
        start2, _end2 = self.client.allocate_uids(how_many)
        assert start != start2
        assert start2 >= end  # Should be non-overlapping

    def test_allocate_timestamps(self) -> None:
        """Test allocating timestamps returns valid range."""
        how_many = 50
        start, end = self.client.allocate_timestamps(how_many)

        # Verify we get a valid range
        assert isinstance(start, int)
        assert isinstance(end, int)
        assert start > 0
        assert end > start
        assert end - start == how_many

        # Test allocating again gives us a different range
        start2, _end2 = self.client.allocate_timestamps(how_many)
        assert start != start2
        assert start2 >= end  # Should be non-overlapping

    def test_allocate_namespaces(self) -> None:
        """Test allocating namespaces returns valid range."""
        how_many = 10
        start, end = self.client.allocate_namespaces(how_many)

        # Verify we get a valid range
        assert isinstance(start, int)
        assert isinstance(end, int)
        assert start > 0
        assert end > start
        assert end - start == how_many

        # Test allocating again gives us a different range
        start2, _end2 = self.client.allocate_namespaces(how_many)
        assert start != start2
        assert start2 >= end  # Should be non-overlapping

    def test_allocate_uids_different_sizes(self) -> None:
        """Test allocating different numbers of UIDs."""
        # Test small allocation
        start1, end1 = self.client.allocate_uids(1)
        assert end1 - start1 == 1

        # Test larger allocation
        start2, end2 = self.client.allocate_uids(1000)
        assert end2 - start2 == 1000

        # Ensure ranges don't overlap
        assert start2 >= end1

    def test_allocate_zero_items(self) -> None:
        """Test allocating zero items raises ValueError."""
        with pytest.raises(ValueError, match="how_many must be greater than 0"):
            self.client.allocate_uids(0)

        # Test negative values also raise ValueError
        with pytest.raises(ValueError, match="how_many must be greater than 0"):
            self.client.allocate_timestamps(-1)

    def test_allocation_methods_are_independent(self) -> None:
        """Test that different allocation types don't interfere with each other."""
        # Allocate from each type
        uid_start, uid_end = self.client.allocate_uids(100)
        ts_start, ts_end = self.client.allocate_timestamps(100)
        ns_start, ns_end = self.client.allocate_namespaces(100)

        # All should return valid ranges
        assert uid_end - uid_start == 100
        assert ts_end - ts_start == 100
        assert ns_end - ns_start == 100

        # The ranges can be different (they're different types of IDs)
        # We just verify they're all positive and valid
        assert uid_start > 0
        assert ts_start > 0
        assert ns_start > 0

    def test_allocate_with_timeout(self) -> None:
        """Test allocation methods work with timeout parameter."""
        start, end = self.client.allocate_uids(10, timeout=30)
        assert end - start == 10

        start, end = self.client.allocate_timestamps(10, timeout=30)
        assert end - start == 10

        start, end = self.client.allocate_namespaces(10, timeout=30)
        assert end - start == 10


def suite() -> unittest.TestSuite:
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestAllocations())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
