# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests utility functions."""

from __future__ import annotations

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import unittest

import grpc

from pydgraph import util


class MockGrpcError:
    """Mock gRPC error with code() method for testing."""

    def __init__(self, status_code: grpc.StatusCode) -> None:
        self._code = status_code

    def code(self) -> grpc.StatusCode:
        return self._code


class TestUtil(unittest.TestCase):
    """Tests util utility functions."""

    def test_is_string(self) -> None:
        assert util.is_string("")
        assert util.is_string("a")
        assert not util.is_string(object())
        assert not util.is_string({})

    def test_is_aborted_error_with_aborted_status(self) -> None:
        """Test is_aborted_error returns True for ABORTED status."""
        error = MockGrpcError(grpc.StatusCode.ABORTED)
        assert util.is_aborted_error(error)

    def test_is_aborted_error_with_failed_precondition(self) -> None:
        """Test is_aborted_error returns True for FAILED_PRECONDITION status."""
        error = MockGrpcError(grpc.StatusCode.FAILED_PRECONDITION)
        assert util.is_aborted_error(error)

    def test_is_aborted_error_with_other_status(self) -> None:
        """Test is_aborted_error returns False for other status codes."""
        error = MockGrpcError(grpc.StatusCode.UNAVAILABLE)
        assert not util.is_aborted_error(error)

        error = MockGrpcError(grpc.StatusCode.INTERNAL)
        assert not util.is_aborted_error(error)

    def test_is_aborted_error_with_non_grpc_error(self) -> None:
        """Test is_aborted_error returns False for non-gRPC errors."""
        assert not util.is_aborted_error(ValueError("test"))
        assert not util.is_aborted_error(Exception("test"))
        assert not util.is_aborted_error("not an error")


def suite() -> unittest.TestSuite:
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestUtil())
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
