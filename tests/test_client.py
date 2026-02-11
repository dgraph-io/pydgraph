# SPDX-FileCopyrightText: Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests construction of Dgraph client."""

from __future__ import annotations

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. "

import unittest

import pytest

import pydgraph


class TestDgraphClient(unittest.TestCase):
    """Tests construction of Dgraph client."""

    def test_constructor(self) -> None:
        with pytest.raises(ValueError, match="No clients provided"):
            pydgraph.DgraphClient()


if __name__ == "__main__":
    unittest.main()
