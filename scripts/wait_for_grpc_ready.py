#!/usr/bin/env python3
"""Wait for a gRPC service to be ready by calling the gRPC health check endpoint.

Uses a backoff pattern of 1, 2, 3, 4 seconds between retries.
"""

from __future__ import annotations

import sys
import time

import grpc


def check_grpc_health(host: str, port: int, timeout: float = 2.0) -> bool:
    """Check if gRPC service is healthy using the health check protocol.

    Args:
        host: Hostname or IP address
        port: Port number
        timeout: Connection timeout in seconds

    Returns:
        True if service is healthy, False otherwise
    """
    try:
        channel = grpc.insecure_channel(f"{host}:{port}")
        # Try to do a simple channel connectivity check
        grpc.channel_ready_future(channel).result(timeout=timeout)
        channel.close()
        return True
    except grpc.FutureTimeoutError:
        return False
    except Exception:
        return False


def wait_for_grpc_ready(host: str, port: int, max_attempts: int = 60) -> None:
    """Wait for gRPC service to be ready using backoff pattern.

    Args:
        host: Hostname or IP address
        port: Port number
        max_attempts: Maximum number of connection attempts (default: 60)

    Raises:
        SystemExit: If service doesn't become ready within max_attempts
    """
    print(f"wait-for-grpc-ready: Waiting for gRPC at {host}:{port} to be ready")

    # Backoff pattern: 1, 2, 3, 4 seconds (repeating)
    backoff_pattern = [1, 2, 3, 4]
    attempt = 0

    while True:
        if check_grpc_health(host, port):
            # gRPC service is ready
            print("wait-for-grpc-ready: gRPC service is healthy and ready.")
            return

        # Health check failed, apply backoff
        attempt += 1
        if attempt > max_attempts:
            print("wait-for-grpc-ready: Took longer than expected for gRPC to be ready.")
            print(f"wait-for-grpc-ready: Waiting stopped after {attempt} attempts.")
            sys.exit(1)

        # Calculate backoff time based on pattern
        backoff_index = (attempt - 1) % len(backoff_pattern)
        backoff_seconds = backoff_pattern[backoff_index]
        print(
            f"wait-for-grpc-ready: Attempt {attempt} failed, "
            f"retrying in {backoff_seconds} second(s)..."
        )
        time.sleep(backoff_seconds)


def main() -> None:
    """Main entry point."""
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <host> <port>")
        print(f"Example: {sys.argv[0]} localhost 9080")
        sys.exit(1)

    host = sys.argv[1]
    try:
        port = int(sys.argv[2])
    except ValueError:
        print(f"Error: Port must be an integer, got '{sys.argv[2]}'")
        sys.exit(1)

    wait_for_grpc_ready(host, port)


if __name__ == "__main__":
    main()
