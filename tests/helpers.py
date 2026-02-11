# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Shared test helpers, constants, and utility functions.

This module contains constants and helper functions used across multiple test files.
It's separate from conftest.py to keep fixture definitions isolated from shared utilities.
"""

from __future__ import annotations

import os
import random
from typing import Any

# =============================================================================
# Configuration Constants
# =============================================================================

TEST_SERVER_ADDR = os.getenv("TEST_SERVER_ADDR", "localhost:9180")


# =============================================================================
# Helper Functions
# =============================================================================


def generate_movie(index: int) -> dict[str, Any]:
    """Generate a movie object for testing using 1million.schema predicates.

    Args:
        index: Unique index for this movie (used in name generation)

    Returns:
        Dictionary with movie attributes suitable for Dgraph mutation
    """
    return {
        "name": f"TestMovie_{index}_{random.randint(1000, 9999)}",  # noqa: S311
        "tagline": f"An amazing test film number {index}",
        "email": f"movie{index}_{random.randint(1000, 9999)}@test.com",  # noqa: S311
    }
