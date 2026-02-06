# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Shared test helpers, constants, and utility functions.

This module contains constants and helper functions used across multiple test files.
It's separate from conftest.py to keep fixture definitions isolated from shared utilities.
"""

from __future__ import annotations

import os
import random
from pathlib import Path
from typing import Any

# =============================================================================
# Configuration Constants
# =============================================================================

TEST_SERVER_ADDR = os.getenv("TEST_SERVER_ADDR", "localhost:9180")

# Synthetic test schema for stress tests
SYNTHETIC_SCHEMA = """
name: string @index(term, exact) .
email: string @index(exact) @upsert .
age: int @index(int) .
balance: float .
active: bool @index(bool) .
created: datetime @index(hour) .
friends: [uid] @count @reverse .
"""

# Path to movie dataset (local to test resources)
TEST_RESOURCES = Path(__file__).parent / "resources"
MOVIE_SCHEMA_PATH = TEST_RESOURCES / "1million.schema"
MOVIE_DATA_PATH = TEST_RESOURCES / "1million.rdf.gz"


# =============================================================================
# Helper Functions
# =============================================================================


def generate_person(index: int) -> dict[str, Any]:
    """Generate a person object for testing.

    Args:
        index: Unique index for this person (used in name/email generation)

    Returns:
        Dictionary with person attributes suitable for Dgraph mutation
    """
    return {
        "name": f"Person_{index}_{random.randint(1000, 9999)}",  # noqa: S311
        "email": f"person{index}_{random.randint(1000, 9999)}@test.com",  # noqa: S311
        "age": random.randint(18, 80),  # noqa: S311
        "balance": random.uniform(0, 10000),  # noqa: S311
        "active": random.choice([True, False]),  # noqa: S311
    }
