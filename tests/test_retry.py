# SPDX-FileCopyrightText: © 2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for transaction retry utilities."""

from __future__ import annotations

import asyncio
import unittest
from typing import Any
from unittest.mock import MagicMock

import pytest

import pydgraph
from pydgraph import errors
from pydgraph.retry import (
    _calculate_delay,
    _is_retriable,
    retry,
    retry_async,
    with_retry,
    with_retry_async,
)

__author__ = "Istari Digital, Inc."
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"


class TestCalculateDelay(unittest.TestCase):
    """Tests for delay calculation with exponential backoff."""

    def test_initial_delay(self) -> None:
        """First attempt should use base delay."""
        delay = _calculate_delay(0, base_delay=0.1, max_delay=5.0, jitter=0.0)
        assert delay == 0.1

    def test_exponential_backoff(self) -> None:
        """Delay should double with each attempt."""
        delay1 = _calculate_delay(0, base_delay=0.1, max_delay=5.0, jitter=0.0)
        delay2 = _calculate_delay(1, base_delay=0.1, max_delay=5.0, jitter=0.0)
        delay3 = _calculate_delay(2, base_delay=0.1, max_delay=5.0, jitter=0.0)

        assert delay1 == 0.1
        assert delay2 == 0.2
        assert delay3 == 0.4

    def test_max_delay_cap(self) -> None:
        """Delay should not exceed max_delay."""
        delay = _calculate_delay(100, base_delay=0.1, max_delay=5.0, jitter=0.0)
        assert delay == 5.0

    def test_jitter_adds_randomness(self) -> None:
        """Jitter should add some randomness to delay."""
        delays = set()
        for _ in range(10):
            delay = _calculate_delay(0, base_delay=1.0, max_delay=5.0, jitter=0.5)
            delays.add(round(delay, 3))

        # With jitter, we should get different values
        # Base delay is 1.0, jitter 0.5 means delay is in range [1.0, 1.5]
        assert all(1.0 <= d <= 1.5 for d in delays)


class TestIsRetriable(unittest.TestCase):
    """Tests for error classification."""

    def test_aborted_error_is_retriable(self) -> None:
        """AbortedError should be retriable."""
        error = errors.AbortedError()
        assert _is_retriable(error)

    def test_retriable_error_is_retriable(self) -> None:
        """RetriableError should be retriable."""
        error = errors.RetriableError(Exception("test"))
        assert _is_retriable(error)

    def test_other_errors_not_retriable(self) -> None:
        """Other errors should not be retriable."""
        assert not _is_retriable(Exception("test"))
        assert not _is_retriable(ValueError("test"))
        assert not _is_retriable(errors.TransactionError("test"))


class TestAbortedErrorPickle(unittest.TestCase):
    """Tests that AbortedError survives pickle round-trips (e.g. Celery)."""

    def test_pickle_default_message(self) -> None:
        """AbortedError() should pickle and unpickle correctly."""
        import pickle

        err = errors.AbortedError()
        restored = pickle.loads(pickle.dumps(err))  # noqa: S301
        assert isinstance(restored, errors.AbortedError)
        assert str(restored) == "Transaction has been aborted. Please retry"

    def test_pickle_custom_message(self) -> None:
        """AbortedError(msg) should preserve the custom message."""
        import pickle

        err = errors.AbortedError("custom conflict message")
        restored = pickle.loads(pickle.dumps(err))  # noqa: S301
        assert isinstance(restored, errors.AbortedError)
        assert str(restored) == "custom conflict message"

    def test_raise_class_directly(self) -> None:
        """'raise AbortedError' (no parens) should also be picklable."""
        import pickle

        try:
            raise errors.AbortedError  # noqa: TRY301
        except errors.AbortedError as caught:
            restored = pickle.loads(pickle.dumps(caught))  # noqa: S301
            assert isinstance(restored, errors.AbortedError)
            assert str(restored) == "Transaction has been aborted. Please retry"



class TestRetryGenerator(unittest.TestCase):
    """Tests for sync retry generator."""

    def test_success_on_first_attempt(self) -> None:
        """Should not retry if first attempt succeeds."""
        # Use very short delays to avoid actual sleeping
        attempts = 0
        for attempt in retry(max_retries=3, base_delay=0.001, jitter=0.0):
            with attempt:
                attempts += 1
                # Success - no exception

        assert attempts == 1

    def test_retry_on_aborted_error(self) -> None:
        """Should retry on AbortedError."""
        attempts = 0
        for attempt in retry(max_retries=3, base_delay=0.001, jitter=0.0):
            with attempt:
                attempts += 1
                if attempts < 3:
                    raise errors.AbortedError
                # Third attempt succeeds

        assert attempts == 3

    def test_raises_after_max_retries(self) -> None:
        """Should raise AbortedError after exhausting retries."""
        attempts = 0

        with pytest.raises(errors.AbortedError):
            for attempt in retry(max_retries=2, base_delay=0.001, jitter=0.0):
                with attempt:
                    attempts += 1
                    raise errors.AbortedError

        assert attempts == 3  # Initial + 2 retries

    def test_non_retriable_error_propagates(self) -> None:
        """Non-retriable errors should propagate immediately."""
        attempts = 0

        with pytest.raises(ValueError):
            for attempt in retry(max_retries=3, base_delay=0.001, jitter=0.0):
                with attempt:
                    attempts += 1
                    raise ValueError("not retriable")

        assert attempts == 1


class TestRetryAsyncGenerator(unittest.TestCase):
    """Tests for async retry generator."""

    def test_success_on_first_attempt(self) -> None:
        """Should not retry if first attempt succeeds."""

        async def test() -> int:
            attempts = 0
            async for attempt in retry_async(
                max_retries=3, base_delay=0.001, jitter=0.0
            ):
                with attempt:
                    attempts += 1
                    # Success
            return attempts

        result = asyncio.run(test())
        assert result == 1

    def test_retry_on_aborted_error(self) -> None:
        """Should retry on AbortedError."""

        async def test() -> int:
            attempts = 0
            async for attempt in retry_async(
                max_retries=3, base_delay=0.001, jitter=0.0
            ):
                with attempt:
                    attempts += 1
                    if attempts < 3:
                        raise errors.AbortedError
            return attempts

        result = asyncio.run(test())
        assert result == 3

    def test_raises_after_max_retries(self) -> None:
        """Should raise AbortedError after exhausting retries."""

        async def test() -> int:
            attempts = 0
            async for attempt in retry_async(
                max_retries=2, base_delay=0.001, jitter=0.0
            ):
                with attempt:
                    attempts += 1
                    raise errors.AbortedError
            return attempts

        with pytest.raises(errors.AbortedError):
            asyncio.run(test())


class TestWithRetryDecorator(unittest.TestCase):
    """Tests for sync retry decorator."""

    def test_success_on_first_attempt(self) -> None:
        """Should not retry if function succeeds."""

        @with_retry(max_retries=3, base_delay=0.001, jitter=0.0)
        def my_func() -> str:
            return "success"

        result = my_func()
        assert result == "success"

    def test_retry_on_aborted_error(self) -> None:
        """Should retry on AbortedError."""
        attempts = {"count": 0}

        @with_retry(max_retries=3, base_delay=0.001, jitter=0.0)
        def my_func() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise errors.AbortedError
            return "success"

        result = my_func()
        assert result == "success"
        assert attempts["count"] == 3

    def test_raises_after_max_retries(self) -> None:
        """Should raise after exhausting retries."""

        @with_retry(max_retries=2, base_delay=0.001, jitter=0.0)
        def my_func() -> None:
            raise errors.AbortedError

        with pytest.raises(errors.AbortedError):
            my_func()

    def test_preserves_function_metadata(self) -> None:
        """Should preserve function name and docstring."""

        @with_retry(base_delay=0.001, jitter=0.0)
        def my_documented_func() -> None:
            """This is a docstring."""

        assert my_documented_func.__name__ == "my_documented_func"
        assert my_documented_func.__doc__ == "This is a docstring."


class TestWithRetryAsyncDecorator(unittest.TestCase):
    """Tests for async retry decorator."""

    def test_success_on_first_attempt(self) -> None:
        """Should not retry if function succeeds."""

        @with_retry_async(max_retries=3, base_delay=0.001, jitter=0.0)
        async def my_func() -> str:
            return "success"

        result = asyncio.run(my_func())
        assert result == "success"

    def test_retry_on_aborted_error(self) -> None:
        """Should retry on AbortedError."""
        attempts = {"count": 0}

        @with_retry_async(max_retries=3, base_delay=0.001, jitter=0.0)
        async def my_func() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise errors.AbortedError
            return "success"

        result = asyncio.run(my_func())
        assert result == "success"
        assert attempts["count"] == 3

    def test_raises_after_max_retries(self) -> None:
        """Should raise after exhausting retries."""

        @with_retry_async(max_retries=2, base_delay=0.001, jitter=0.0)
        async def my_func() -> None:
            raise errors.AbortedError

        with pytest.raises(errors.AbortedError):
            asyncio.run(my_func())


class TestRetryImports(unittest.TestCase):
    """Tests for module-level imports."""

    def test_retry_exported_from_pydgraph(self) -> None:
        """Retry utilities should be importable from pydgraph."""
        assert hasattr(pydgraph, "retry")
        assert hasattr(pydgraph, "retry_async")
        assert hasattr(pydgraph, "with_retry")
        assert hasattr(pydgraph, "with_retry_async")
        assert hasattr(pydgraph, "run_transaction")
        assert hasattr(pydgraph, "run_transaction_async")


class TestRetryWithRetriableError(unittest.TestCase):
    """Tests for RetriableError handling."""

    def test_retry_on_retriable_error(self) -> None:
        """Should also retry on RetriableError."""
        attempts = {"count": 0}

        @with_retry(max_retries=3, base_delay=0.001, jitter=0.0)
        def my_func() -> str:
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise errors.RetriableError(Exception("temporary"))
            return "success"

        result = my_func()
        assert result == "success"
        assert attempts["count"] == 2


class TestRetryExponentialBackoff(unittest.TestCase):
    """Tests to verify exponential backoff behavior with timing."""

    def test_backoff_increases_delay(self) -> None:
        """Verify delays increase exponentially."""
        # Import the module directly using importlib (pydgraph.retry is shadowed by the function)
        import importlib

        retry_mod = importlib.import_module("pydgraph.retry")

        sleep_durations: list[float] = []
        original_sleep = retry_mod.time.sleep

        def mock_sleep(duration: float) -> None:
            sleep_durations.append(duration)
            # Don't actually sleep

        # Temporarily replace sleep
        retry_mod.time.sleep = mock_sleep  # type: ignore[method-assign]
        try:
            with pytest.raises(errors.AbortedError):
                for attempt in retry(max_retries=3, base_delay=0.1, jitter=0.0):
                    with attempt:
                        raise errors.AbortedError

            # Should have 3 sleeps (between attempts 1-2, 2-3, 3-4)
            assert len(sleep_durations) == 3
            # Verify exponential backoff: 0.1, 0.2, 0.4
            assert sleep_durations[0] == pytest.approx(0.1, abs=0.001)
            assert sleep_durations[1] == pytest.approx(0.2, abs=0.001)
            assert sleep_durations[2] == pytest.approx(0.4, abs=0.001)
        finally:
            retry_mod.time.sleep = original_sleep


class TestParameterValidation(unittest.TestCase):
    """Tests for parameter validation."""

    def test_negative_max_retries_raises(self) -> None:
        """Negative max_retries should raise ValueError."""
        from pydgraph.retry import _validate_retry_params

        with pytest.raises(ValueError) as ctx:
            _validate_retry_params(
                max_retries=-1, base_delay=0.1, max_delay=5.0, jitter=0.1
            )
        assert "max_retries" in str(ctx.value)

    def test_negative_base_delay_raises(self) -> None:
        """Negative base_delay should raise ValueError."""
        from pydgraph.retry import _validate_retry_params

        with pytest.raises(ValueError) as ctx:
            _validate_retry_params(
                max_retries=3, base_delay=-0.1, max_delay=5.0, jitter=0.1
            )
        assert "base_delay" in str(ctx.value)

    def test_invalid_jitter_raises(self) -> None:
        """Jitter outside [0, 1] should raise ValueError."""
        from pydgraph.retry import _validate_retry_params

        with pytest.raises(ValueError) as ctx:
            _validate_retry_params(
                max_retries=3, base_delay=0.1, max_delay=5.0, jitter=1.5
            )
        assert "jitter" in str(ctx.value)

    def test_valid_params_pass(self) -> None:
        """Valid parameters should not raise."""
        from pydgraph.retry import _validate_retry_params

        # Should not raise
        _validate_retry_params(max_retries=0, base_delay=0.0, max_delay=0.0, jitter=0.0)
        _validate_retry_params(
            max_retries=10, base_delay=1.0, max_delay=30.0, jitter=1.0
        )


class TestRunTransaction(unittest.TestCase):
    """Tests for run_transaction helper."""

    def test_success_on_first_attempt(self) -> None:
        """Should succeed if operation doesn't raise."""
        from pydgraph.retry import run_transaction

        mock_client = MagicMock()
        mock_txn = MagicMock()
        mock_client.txn.return_value = mock_txn

        def operation(txn: Any) -> str:
            return "success"

        result = run_transaction(
            mock_client, operation, max_retries=3, base_delay=0.001
        )
        assert result == "success"
        mock_txn.discard.assert_called_once()

    def test_retry_on_aborted_error(self) -> None:
        """Should retry on AbortedError."""
        from pydgraph.retry import run_transaction

        mock_client = MagicMock()
        mock_txn = MagicMock()
        mock_client.txn.return_value = mock_txn
        attempts = {"count": 0}

        def operation(txn: Any) -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise errors.AbortedError
            return "success"

        result = run_transaction(
            mock_client, operation, max_retries=5, base_delay=0.001, jitter=0.0
        )
        assert result == "success"
        assert attempts["count"] == 3

    def test_raises_after_max_retries(self) -> None:
        """Should raise after exhausting retries."""
        from pydgraph.retry import run_transaction

        mock_client = MagicMock()
        mock_txn = MagicMock()
        mock_client.txn.return_value = mock_txn

        def operation(txn: Any) -> None:
            raise errors.AbortedError

        with pytest.raises(errors.AbortedError):
            run_transaction(
                mock_client, operation, max_retries=2, base_delay=0.001, jitter=0.0
            )


if __name__ == "__main__":
    unittest.main()
