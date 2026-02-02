# SPDX-FileCopyrightText: © 2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Transaction retry utilities for handling Dgraph transaction conflicts.

Dgraph uses optimistic concurrency control. When transactions conflict
(e.g., concurrent modifications to overlapping data), Dgraph aborts
one of them and expects the client to retry.

This module provides utilities to automatically retry aborted transactions
with exponential backoff.

Example usage:

    # Async context manager style (recommended)
    async with client.txn() as txn:
        async for attempt in retry_async():
            with attempt:
                await txn.mutate(set_obj={"name": "Alice"})
                await txn.commit()

    # Async decorator style
    @with_retry_async()
    async def upsert_user(client, name: str):
        async with client.txn() as txn:
            await txn.mutate(set_obj={"name": name})
            await txn.commit()

    # Sync decorator style
    @with_retry()
    def upsert_user(client, name: str):
        with client.txn() as txn:
            txn.mutate(set_obj={"name": name})
            txn.commit()
"""

import asyncio
import functools
import logging
import random
import time
from collections.abc import AsyncGenerator, Callable, Generator
from types import TracebackType
from typing import Any, Optional, TypeVar

from pydgraph import errors
from pydgraph.meta import VERSION

__author__ = "Istari Digital, Inc."
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"
__version__ = VERSION
__status__ = "development"

logger = logging.getLogger(__name__)

# Type variables for generic decorators
F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")

# Default retry configuration
DEFAULT_MAX_RETRIES = 5
DEFAULT_BASE_DELAY = 0.1  # 100ms
DEFAULT_MAX_DELAY = 5.0  # 5 seconds
DEFAULT_JITTER = 0.1  # 10% jitter


def _calculate_delay(
    attempt: int,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    jitter: float = DEFAULT_JITTER,
) -> float:
    """Calculate delay with exponential backoff and jitter.

    Args:
        attempt: Current attempt number (0-based)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        jitter: Jitter factor (0-1), adds randomness to prevent thundering herd

    Returns:
        Delay in seconds
    """
    # Exponential backoff: base_delay * 2^attempt
    delay = min(base_delay * (2**attempt), max_delay)

    # Add jitter to prevent thundering herd
    if jitter > 0:
        jitter_amount = delay * jitter * random.random()  # noqa: S311  # nosec B311
        delay = delay + jitter_amount

    return delay


def _is_retriable(error: Optional[BaseException]) -> bool:
    """Check if an error is retriable.

    Args:
        error: Exception to check

    Returns:
        True if the error indicates a transaction conflict that should be retried
    """
    return isinstance(error, (errors.AbortedError, errors.RetriableError))


def _validate_retry_params(
    max_retries: int,
    base_delay: float,
    max_delay: float,
    jitter: float,
) -> None:
    """Validate retry parameters.

    Args:
        max_retries: Must be >= 0
        base_delay: Must be >= 0
        max_delay: Must be >= 0
        jitter: Must be between 0 and 1

    Raises:
        ValueError: If any parameter is invalid
    """
    if max_retries < 0:
        raise ValueError(f"max_retries must be >= 0, got {max_retries}")
    if base_delay < 0:
        raise ValueError(f"base_delay must be >= 0, got {base_delay}")
    if max_delay < 0:
        raise ValueError(f"max_delay must be >= 0, got {max_delay}")
    if not (0 <= jitter <= 1):
        raise ValueError(f"jitter must be between 0 and 1, got {jitter}")


class RetryAttempt:
    """Context manager for a single retry attempt.

    Used with the retry() generator to provide clean error handling:

        for attempt in retry():
            with attempt:
                # Your transaction code here
                txn.commit()
    """

    def __init__(self) -> None:
        self.failed = False
        self.error: Optional[Exception] = None

    def __enter__(self) -> "RetryAttempt":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        if exc_type is not None and _is_retriable(exc_val):
            self.failed = True
            self.error = exc_val  # type: ignore[assignment]
            return True  # Suppress the exception
        return False


class AsyncRetryAttempt:
    """Async context manager for a single retry attempt.

    Used with the retry_async() generator:

        async for attempt in retry_async():
            with attempt:
                await txn.commit()
    """

    def __init__(self) -> None:
        self.failed = False
        self.error: Optional[Exception] = None

    def __enter__(self) -> "AsyncRetryAttempt":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        if exc_type is not None and _is_retriable(exc_val):
            self.failed = True
            self.error = exc_val  # type: ignore[assignment]
            return True  # Suppress the exception
        return False


def retry(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    jitter: float = DEFAULT_JITTER,
) -> Generator[RetryAttempt, None, None]:
    """Generator for sync transaction retry with exponential backoff.

    Yields RetryAttempt context managers. Use in a for loop:

        for attempt in retry(max_retries=3):
            with attempt:
                txn = client.txn()
                try:
                    txn.mutate(set_obj={"name": "Alice"})
                    txn.commit()
                finally:
                    txn.discard()

    Args:
        max_retries: Maximum number of retry attempts (default: 5)
        base_delay: Base delay between retries in seconds (default: 0.1)
        max_delay: Maximum delay between retries in seconds (default: 5.0)
        jitter: Jitter factor to add randomness (default: 0.1)

    Yields:
        RetryAttempt context managers

    Raises:
        AbortedError: If all retries are exhausted
    """
    for attempt_num in range(max_retries + 1):
        attempt = RetryAttempt()
        yield attempt

        if not attempt.failed:
            # Transaction succeeded
            return

        if attempt_num < max_retries:
            # Calculate delay and sleep before retry
            delay = _calculate_delay(attempt_num, base_delay, max_delay, jitter)
            logger.debug(
                "Transaction aborted (attempt %d/%d), retrying in %.3fs",
                attempt_num + 1,
                max_retries + 1,
                delay,
            )
            time.sleep(delay)
        else:
            # All retries exhausted
            logger.warning("Transaction failed after %d attempts", max_retries + 1)
            raise attempt.error or errors.AbortedError()


async def retry_async(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    jitter: float = DEFAULT_JITTER,
) -> AsyncGenerator[AsyncRetryAttempt, None]:
    """Async generator for transaction retry with exponential backoff.

    Yields AsyncRetryAttempt context managers. Use in an async for loop:

        async for attempt in retry_async(max_retries=3):
            with attempt:
                txn = client.txn()
                try:
                    await txn.mutate(set_obj={"name": "Alice"})
                    await txn.commit()
                finally:
                    await txn.discard()

    Args:
        max_retries: Maximum number of retry attempts (default: 5)
        base_delay: Base delay between retries in seconds (default: 0.1)
        max_delay: Maximum delay between retries in seconds (default: 5.0)
        jitter: Jitter factor to add randomness (default: 0.1)

    Yields:
        AsyncRetryAttempt context managers

    Raises:
        AbortedError: If all retries are exhausted
    """
    for attempt_num in range(max_retries + 1):
        attempt = AsyncRetryAttempt()
        yield attempt

        if not attempt.failed:
            # Transaction succeeded
            return

        if attempt_num < max_retries:
            # Calculate delay and sleep before retry
            delay = _calculate_delay(attempt_num, base_delay, max_delay, jitter)
            logger.debug(
                "Transaction aborted (attempt %d/%d), retrying in %.3fs",
                attempt_num + 1,
                max_retries + 1,
                delay,
            )
            await asyncio.sleep(delay)
        else:
            # All retries exhausted
            logger.warning("Transaction failed after %d attempts", max_retries + 1)
            raise attempt.error or errors.AbortedError()


def with_retry(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    jitter: float = DEFAULT_JITTER,
) -> Callable[[F], F]:
    """Decorator for sync functions that should retry on transaction abort.

    The decorated function will be retried with exponential backoff
    when AbortedError or RetriableError is raised.

    Example:
        @with_retry(max_retries=3)
        def upsert_user(client, name: str):
            txn = client.txn()
            try:
                txn.mutate(set_obj={"name": name})
                txn.commit()
            finally:
                txn.discard()

    Args:
        max_retries: Maximum number of retry attempts (default: 5)
        base_delay: Base delay between retries in seconds (default: 0.1)
        max_delay: Maximum delay between retries in seconds (default: 5.0)
        jitter: Jitter factor to add randomness (default: 0.1)

    Returns:
        Decorator function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Optional[Exception] = None

            for attempt_num in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (errors.AbortedError, errors.RetriableError) as e:  # noqa: PERF203
                    last_error = e

                    if attempt_num < max_retries:
                        delay = _calculate_delay(
                            attempt_num, base_delay, max_delay, jitter
                        )
                        logger.debug(
                            "Transaction aborted in %s (attempt %d/%d), "
                            "retrying in %.3fs",
                            func.__name__,
                            attempt_num + 1,
                            max_retries + 1,
                            delay,
                        )
                        time.sleep(delay)

            # All retries exhausted
            logger.warning(
                "Transaction in %s failed after %d attempts",
                func.__name__,
                max_retries + 1,
            )
            raise last_error or errors.AbortedError()

        return wrapper  # type: ignore[return-value]

    return decorator


def with_retry_async(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    jitter: float = DEFAULT_JITTER,
) -> Callable[[F], F]:
    """Decorator for async functions that should retry on transaction abort.

    The decorated function will be retried with exponential backoff
    when AbortedError or RetriableError is raised.

    Example:
        @with_retry_async(max_retries=3)
        async def upsert_user(client, name: str):
            txn = client.txn()
            try:
                await txn.mutate(set_obj={"name": name})
                await txn.commit()
            finally:
                await txn.discard()

    Args:
        max_retries: Maximum number of retry attempts (default: 5)
        base_delay: Base delay between retries in seconds (default: 0.1)
        max_delay: Maximum delay between retries in seconds (default: 5.0)
        jitter: Jitter factor to add randomness (default: 0.1)

    Returns:
        Decorator function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Optional[Exception] = None

            for attempt_num in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (errors.AbortedError, errors.RetriableError) as e:  # noqa: PERF203
                    last_error = e

                    if attempt_num < max_retries:
                        delay = _calculate_delay(
                            attempt_num, base_delay, max_delay, jitter
                        )
                        logger.debug(
                            "Transaction aborted in %s (attempt %d/%d), "
                            "retrying in %.3fs",
                            func.__name__,
                            attempt_num + 1,
                            max_retries + 1,
                            delay,
                        )
                        await asyncio.sleep(delay)

            # All retries exhausted
            logger.warning(
                "Transaction in %s failed after %d attempts",
                func.__name__,
                max_retries + 1,
            )
            raise last_error or errors.AbortedError()

        return wrapper  # type: ignore[return-value]

    return decorator


def run_transaction(
    client: Any,
    operation: Callable[..., Any],
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    jitter: float = DEFAULT_JITTER,
    read_only: bool = False,
    best_effort: bool = False,
) -> Any:
    """Run a transaction with automatic retry on conflict.

    Executes the operation function with a fresh transaction on each attempt.
    The operation receives the transaction as its only argument.

    Example:
        def upsert_user(txn):
            txn.mutate(set_obj={"name": "Alice"})
            txn.commit()
            return "success"

        result = run_transaction(client, upsert_user, max_retries=3)

    Args:
        client: DgraphClient instance
        operation: Callable that receives a transaction and performs work
        max_retries: Maximum number of retry attempts (default: 5)
        base_delay: Base delay between retries in seconds (default: 0.1)
        max_delay: Maximum delay between retries in seconds (default: 5.0)
        jitter: Jitter factor to add randomness (default: 0.1)
        read_only: If True, create read-only transactions
        best_effort: If True, use best-effort mode (read-only only)

    Returns:
        Result of the operation function

    Raises:
        AbortedError: If all retries are exhausted
        ValueError: If parameters are invalid
    """
    _validate_retry_params(max_retries, base_delay, max_delay, jitter)

    last_error: Optional[Exception] = None

    for attempt_num in range(max_retries + 1):
        txn = client.txn(read_only=read_only, best_effort=best_effort)
        try:
            result = operation(txn)
        except (errors.AbortedError, errors.RetriableError) as e:
            last_error = e
            if attempt_num < max_retries:
                delay = _calculate_delay(attempt_num, base_delay, max_delay, jitter)
                logger.debug(
                    "Transaction aborted (attempt %d/%d), retrying in %.3fs",
                    attempt_num + 1,
                    max_retries + 1,
                    delay,
                )
                time.sleep(delay)
        else:
            return result
        finally:
            try:
                txn.discard()
            except Exception:
                logger.debug("Failed to discard transaction", exc_info=True)

    # All retries exhausted
    logger.warning("Transaction failed after %d attempts", max_retries + 1)
    raise last_error or errors.AbortedError()


async def run_transaction_async(
    client: Any,
    operation: Callable[..., Any],
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    jitter: float = DEFAULT_JITTER,
    read_only: bool = False,
    best_effort: bool = False,
) -> Any:
    """Run an async transaction with automatic retry on conflict.

    Executes the operation function with a fresh transaction on each attempt.
    The operation receives the transaction as its only argument.

    Example:
        async def upsert_user(txn):
            await txn.mutate(set_obj={"name": "Alice"})
            await txn.commit()
            return "success"

        result = await run_transaction_async(client, upsert_user, max_retries=3)

    Args:
        client: AsyncDgraphClient instance
        operation: Async callable that receives a transaction and performs work
        max_retries: Maximum number of retry attempts (default: 5)
        base_delay: Base delay between retries in seconds (default: 0.1)
        max_delay: Maximum delay between retries in seconds (default: 5.0)
        jitter: Jitter factor to add randomness (default: 0.1)
        read_only: If True, create read-only transactions
        best_effort: If True, use best-effort mode (read-only only)

    Returns:
        Result of the operation function

    Raises:
        AbortedError: If all retries are exhausted
        ValueError: If parameters are invalid
    """
    _validate_retry_params(max_retries, base_delay, max_delay, jitter)

    last_error: Optional[Exception] = None

    for attempt_num in range(max_retries + 1):
        txn = client.txn(read_only=read_only, best_effort=best_effort)
        try:
            result = await operation(txn)
        except (errors.AbortedError, errors.RetriableError) as e:
            last_error = e
            if attempt_num < max_retries:
                delay = _calculate_delay(attempt_num, base_delay, max_delay, jitter)
                logger.debug(
                    "Transaction aborted (attempt %d/%d), retrying in %.3fs",
                    attempt_num + 1,
                    max_retries + 1,
                    delay,
                )
                await asyncio.sleep(delay)
        else:
            return result
        finally:
            try:
                await txn.discard()
            except Exception:
                logger.debug("Failed to discard transaction", exc_info=True)

    # All retries exhausted
    logger.warning("Transaction failed after %d attempts", max_retries + 1)
    raise last_error or errors.AbortedError()
