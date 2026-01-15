# SPDX-FileCopyrightText: © 2025 Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Tests for transaction retry utilities."""

import asyncio
import time as time_module
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

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

__author__ = "Hypermode Inc."
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"


class TestCalculateDelay(unittest.TestCase):
    """Tests for delay calculation with exponential backoff."""

    def test_initial_delay(self):
        """First attempt should use base delay."""
        delay = _calculate_delay(0, base_delay=0.1, max_delay=5.0, jitter=0.0)
        self.assertEqual(delay, 0.1)

    def test_exponential_backoff(self):
        """Delay should double with each attempt."""
        delay1 = _calculate_delay(0, base_delay=0.1, max_delay=5.0, jitter=0.0)
        delay2 = _calculate_delay(1, base_delay=0.1, max_delay=5.0, jitter=0.0)
        delay3 = _calculate_delay(2, base_delay=0.1, max_delay=5.0, jitter=0.0)

        self.assertEqual(delay1, 0.1)
        self.assertEqual(delay2, 0.2)
        self.assertEqual(delay3, 0.4)

    def test_max_delay_cap(self):
        """Delay should not exceed max_delay."""
        delay = _calculate_delay(100, base_delay=0.1, max_delay=5.0, jitter=0.0)
        self.assertEqual(delay, 5.0)

    def test_jitter_adds_randomness(self):
        """Jitter should add some randomness to delay."""
        delays = set()
        for _ in range(10):
            delay = _calculate_delay(0, base_delay=1.0, max_delay=5.0, jitter=0.5)
            delays.add(round(delay, 3))

        # With jitter, we should get different values
        # Base delay is 1.0, jitter 0.5 means delay is in range [1.0, 1.5]
        self.assertTrue(all(1.0 <= d <= 1.5 for d in delays))


class TestIsRetriable(unittest.TestCase):
    """Tests for error classification."""

    def test_aborted_error_is_retriable(self):
        """AbortedError should be retriable."""
        error = errors.AbortedError()
        self.assertTrue(_is_retriable(error))

    def test_retriable_error_is_retriable(self):
        """RetriableError should be retriable."""
        error = errors.RetriableError(Exception("test"))
        self.assertTrue(_is_retriable(error))

    def test_other_errors_not_retriable(self):
        """Other errors should not be retriable."""
        self.assertFalse(_is_retriable(Exception("test")))
        self.assertFalse(_is_retriable(ValueError("test")))
        self.assertFalse(_is_retriable(errors.TransactionError("test")))


class TestRetryGenerator(unittest.TestCase):
    """Tests for sync retry generator."""

    def test_success_on_first_attempt(self):
        """Should not retry if first attempt succeeds."""
        # Use very short delays to avoid actual sleeping
        attempts = 0
        for attempt in retry(max_retries=3, base_delay=0.001, jitter=0.0):
            with attempt:
                attempts += 1
                # Success - no exception

        self.assertEqual(attempts, 1)

    def test_retry_on_aborted_error(self):
        """Should retry on AbortedError."""
        attempts = 0
        for attempt in retry(max_retries=3, base_delay=0.001, jitter=0.0):
            with attempt:
                attempts += 1
                if attempts < 3:
                    raise errors.AbortedError()
                # Third attempt succeeds

        self.assertEqual(attempts, 3)

    def test_raises_after_max_retries(self):
        """Should raise AbortedError after exhausting retries."""
        attempts = 0

        with self.assertRaises(errors.AbortedError):
            for attempt in retry(max_retries=2, base_delay=0.001, jitter=0.0):
                with attempt:
                    attempts += 1
                    raise errors.AbortedError()

        self.assertEqual(attempts, 3)  # Initial + 2 retries

    def test_non_retriable_error_propagates(self):
        """Non-retriable errors should propagate immediately."""
        attempts = 0

        with self.assertRaises(ValueError):
            for attempt in retry(max_retries=3, base_delay=0.001, jitter=0.0):
                with attempt:
                    attempts += 1
                    raise ValueError("not retriable")

        self.assertEqual(attempts, 1)


class TestRetryAsyncGenerator(unittest.TestCase):
    """Tests for async retry generator."""

    def test_success_on_first_attempt(self):
        """Should not retry if first attempt succeeds."""

        async def test():
            attempts = 0
            async for attempt in retry_async(max_retries=3, base_delay=0.001, jitter=0.0):
                with attempt:
                    attempts += 1
                    # Success
            return attempts

        result = asyncio.get_event_loop().run_until_complete(test())
        self.assertEqual(result, 1)

    def test_retry_on_aborted_error(self):
        """Should retry on AbortedError."""

        async def test():
            attempts = 0
            async for attempt in retry_async(max_retries=3, base_delay=0.001, jitter=0.0):
                with attempt:
                    attempts += 1
                    if attempts < 3:
                        raise errors.AbortedError()
            return attempts

        result = asyncio.get_event_loop().run_until_complete(test())
        self.assertEqual(result, 3)

    def test_raises_after_max_retries(self):
        """Should raise AbortedError after exhausting retries."""

        async def test():
            attempts = 0
            async for attempt in retry_async(max_retries=2, base_delay=0.001, jitter=0.0):
                with attempt:
                    attempts += 1
                    raise errors.AbortedError()
            return attempts

        with self.assertRaises(errors.AbortedError):
            asyncio.get_event_loop().run_until_complete(test())


class TestWithRetryDecorator(unittest.TestCase):
    """Tests for sync retry decorator."""

    def test_success_on_first_attempt(self):
        """Should not retry if function succeeds."""

        @with_retry(max_retries=3, base_delay=0.001, jitter=0.0)
        def my_func():
            return "success"

        result = my_func()
        self.assertEqual(result, "success")

    def test_retry_on_aborted_error(self):
        """Should retry on AbortedError."""
        attempts = {"count": 0}

        @with_retry(max_retries=3, base_delay=0.001, jitter=0.0)
        def my_func():
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise errors.AbortedError()
            return "success"

        result = my_func()
        self.assertEqual(result, "success")
        self.assertEqual(attempts["count"], 3)

    def test_raises_after_max_retries(self):
        """Should raise after exhausting retries."""

        @with_retry(max_retries=2, base_delay=0.001, jitter=0.0)
        def my_func():
            raise errors.AbortedError()

        with self.assertRaises(errors.AbortedError):
            my_func()

    def test_preserves_function_metadata(self):
        """Should preserve function name and docstring."""

        @with_retry(base_delay=0.001, jitter=0.0)
        def my_documented_func():
            """This is a docstring."""
            pass

        self.assertEqual(my_documented_func.__name__, "my_documented_func")
        self.assertEqual(my_documented_func.__doc__, "This is a docstring.")


class TestWithRetryAsyncDecorator(unittest.TestCase):
    """Tests for async retry decorator."""

    def test_success_on_first_attempt(self):
        """Should not retry if function succeeds."""

        @with_retry_async(max_retries=3, base_delay=0.001, jitter=0.0)
        async def my_func():
            return "success"

        result = asyncio.get_event_loop().run_until_complete(my_func())
        self.assertEqual(result, "success")

    def test_retry_on_aborted_error(self):
        """Should retry on AbortedError."""
        attempts = {"count": 0}

        @with_retry_async(max_retries=3, base_delay=0.001, jitter=0.0)
        async def my_func():
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise errors.AbortedError()
            return "success"

        result = asyncio.get_event_loop().run_until_complete(my_func())
        self.assertEqual(result, "success")
        self.assertEqual(attempts["count"], 3)

    def test_raises_after_max_retries(self):
        """Should raise after exhausting retries."""

        @with_retry_async(max_retries=2, base_delay=0.001, jitter=0.0)
        async def my_func():
            raise errors.AbortedError()

        with self.assertRaises(errors.AbortedError):
            asyncio.get_event_loop().run_until_complete(my_func())


class TestRetryImports(unittest.TestCase):
    """Tests for module-level imports."""

    def test_retry_exported_from_pydgraph(self):
        """Retry utilities should be importable from pydgraph."""
        self.assertTrue(hasattr(pydgraph, "retry"))
        self.assertTrue(hasattr(pydgraph, "retry_async"))
        self.assertTrue(hasattr(pydgraph, "with_retry"))
        self.assertTrue(hasattr(pydgraph, "with_retry_async"))
        self.assertTrue(hasattr(pydgraph, "run_transaction"))
        self.assertTrue(hasattr(pydgraph, "run_transaction_async"))


class TestRetryWithRetriableError(unittest.TestCase):
    """Tests for RetriableError handling."""

    def test_retry_on_retriable_error(self):
        """Should also retry on RetriableError."""
        attempts = {"count": 0}

        @with_retry(max_retries=3, base_delay=0.001, jitter=0.0)
        def my_func():
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise errors.RetriableError(Exception("temporary"))
            return "success"

        result = my_func()
        self.assertEqual(result, "success")
        self.assertEqual(attempts["count"], 2)


class TestRetryExponentialBackoff(unittest.TestCase):
    """Tests to verify exponential backoff behavior with timing."""

    def test_backoff_increases_delay(self):
        """Verify delays increase exponentially."""
        # Import the module directly using importlib (pydgraph.retry is shadowed by the function)
        import importlib

        retry_mod = importlib.import_module("pydgraph.retry")

        sleep_durations = []
        original_sleep = retry_mod.time.sleep

        def mock_sleep(duration):
            sleep_durations.append(duration)
            # Don't actually sleep

        # Temporarily replace sleep
        retry_mod.time.sleep = mock_sleep
        try:
            attempts = 0
            with self.assertRaises(errors.AbortedError):
                for attempt in retry(max_retries=3, base_delay=0.1, jitter=0.0):
                    with attempt:
                        attempts += 1
                        raise errors.AbortedError()

            # Should have 3 sleeps (between attempts 1-2, 2-3, 3-4)
            self.assertEqual(len(sleep_durations), 3)
            # Verify exponential backoff: 0.1, 0.2, 0.4
            self.assertAlmostEqual(sleep_durations[0], 0.1, places=3)
            self.assertAlmostEqual(sleep_durations[1], 0.2, places=3)
            self.assertAlmostEqual(sleep_durations[2], 0.4, places=3)
        finally:
            retry_mod.time.sleep = original_sleep


class TestParameterValidation(unittest.TestCase):
    """Tests for parameter validation."""

    def test_negative_max_retries_raises(self):
        """Negative max_retries should raise ValueError."""
        from pydgraph.retry import _validate_retry_params

        with self.assertRaises(ValueError) as ctx:
            _validate_retry_params(max_retries=-1, base_delay=0.1, max_delay=5.0, jitter=0.1)
        self.assertIn("max_retries", str(ctx.exception))

    def test_negative_base_delay_raises(self):
        """Negative base_delay should raise ValueError."""
        from pydgraph.retry import _validate_retry_params

        with self.assertRaises(ValueError) as ctx:
            _validate_retry_params(max_retries=3, base_delay=-0.1, max_delay=5.0, jitter=0.1)
        self.assertIn("base_delay", str(ctx.exception))

    def test_invalid_jitter_raises(self):
        """Jitter outside [0, 1] should raise ValueError."""
        from pydgraph.retry import _validate_retry_params

        with self.assertRaises(ValueError) as ctx:
            _validate_retry_params(max_retries=3, base_delay=0.1, max_delay=5.0, jitter=1.5)
        self.assertIn("jitter", str(ctx.exception))

    def test_valid_params_pass(self):
        """Valid parameters should not raise."""
        from pydgraph.retry import _validate_retry_params

        # Should not raise
        _validate_retry_params(max_retries=0, base_delay=0.0, max_delay=0.0, jitter=0.0)
        _validate_retry_params(max_retries=10, base_delay=1.0, max_delay=30.0, jitter=1.0)


class TestRunTransaction(unittest.TestCase):
    """Tests for run_transaction helper."""

    def test_success_on_first_attempt(self):
        """Should succeed if operation doesn't raise."""
        from pydgraph.retry import run_transaction

        mock_client = MagicMock()
        mock_txn = MagicMock()
        mock_client.txn.return_value = mock_txn

        def operation(txn):
            return "success"

        result = run_transaction(mock_client, operation, max_retries=3, base_delay=0.001)
        self.assertEqual(result, "success")
        mock_txn.discard.assert_called_once()

    def test_retry_on_aborted_error(self):
        """Should retry on AbortedError."""
        from pydgraph.retry import run_transaction

        mock_client = MagicMock()
        mock_txn = MagicMock()
        mock_client.txn.return_value = mock_txn
        attempts = {"count": 0}

        def operation(txn):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise errors.AbortedError()
            return "success"

        result = run_transaction(mock_client, operation, max_retries=5, base_delay=0.001, jitter=0.0)
        self.assertEqual(result, "success")
        self.assertEqual(attempts["count"], 3)

    def test_raises_after_max_retries(self):
        """Should raise after exhausting retries."""
        from pydgraph.retry import run_transaction

        mock_client = MagicMock()
        mock_txn = MagicMock()
        mock_client.txn.return_value = mock_txn

        def operation(txn):
            raise errors.AbortedError()

        with self.assertRaises(errors.AbortedError):
            run_transaction(mock_client, operation, max_retries=2, base_delay=0.001, jitter=0.0)


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(unittest.makeSuite(TestCalculateDelay))
    suite_obj.addTest(unittest.makeSuite(TestIsRetriable))
    suite_obj.addTest(unittest.makeSuite(TestRetryGenerator))
    suite_obj.addTest(unittest.makeSuite(TestRetryAsyncGenerator))
    suite_obj.addTest(unittest.makeSuite(TestWithRetryDecorator))
    suite_obj.addTest(unittest.makeSuite(TestWithRetryAsyncDecorator))
    suite_obj.addTest(unittest.makeSuite(TestRetryImports))
    suite_obj.addTest(unittest.makeSuite(TestRetryWithRetriableError))
    suite_obj.addTest(unittest.makeSuite(TestRetryExponentialBackoff))
    suite_obj.addTest(unittest.makeSuite(TestParameterValidation))
    suite_obj.addTest(unittest.makeSuite(TestRunTransaction))
    return suite_obj


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(suite())
