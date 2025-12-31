# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runs protoc with the gRPC plugin to generate messages and gRPC stubs.

This project uses Python 3.13+ as the canonical version for generating protobufs.
The generated proto files are checked into the repository.

Usage: uv run python scripts/protogen.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Minimum required versions
MIN_PYTHON_VERSION = (3, 13)
MIN_GRPCIO_TOOLS_VERSION = "1.66.2"

# Check Python version first
if sys.version_info < MIN_PYTHON_VERSION:
    print("ERROR: Proto generation requires Python 3.13 or higher")
    print(
        f"Current Python version: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    print(f"Required: Python {MIN_PYTHON_VERSION[0]}.{MIN_PYTHON_VERSION[1]}+")
    print()
    print("To set up the project with the correct Python version and dependencies:")
    print("  Option 1: make setup")
    print("  Option 2: uv python install 3.13 && uv sync --group dev --extra dev")
    print()
    print("Then retry: uv run python scripts/protogen.py")
    sys.exit(1)

# Import grpc_tools after Python version check
try:
    from grpc_tools import protoc
except ImportError:
    print("ERROR: grpcio-tools is not installed")
    print()
    print("To install dependencies:")
    print("  Option 1: make setup")
    print("  Option 2: uv sync --group dev --extra dev")
    print()
    print("Then retry: uv run python scripts/protogen.py")
    sys.exit(1)

# Check grpcio version (grpcio-tools doesn't expose __version__)
try:
    import grpc
    from packaging import version

    current_version = version.parse(grpc.__version__)
    required_version = version.parse(MIN_GRPCIO_TOOLS_VERSION)

    if current_version < required_version:
        print("ERROR: grpcio version is too old")
        print(f"Current version: {grpc.__version__}")
        print(f"Required: {MIN_GRPCIO_TOOLS_VERSION}+ (grpcio-tools should match)")
        print()
        print("To upgrade dependencies:")
        print("  Option 1: make setup")
        print("  Option 2: uv sync --group dev --extra dev")
        print()
        print("Then retry: uv run python scripts/protogen.py")
        sys.exit(1)
except ImportError:
    # If we can't check version, trust that uv sync installed the correct version
    print("Warning: Could not verify grpcio version, proceeding anyway...")
    print("Ensure you ran 'make setup' or 'uv sync --group dev --extra dev' first.")

dirpath = Path(__file__).resolve().parent
protopath = (dirpath / "../pydgraph/proto").resolve()

protoc.main(
    (
        "",
        "-I" + str(protopath),
        "--python_out=" + str(protopath),
        "--mypy_out=" + str(protopath),
        "--grpc_python_out=" + str(protopath),
        "--mypy_grpc_out=" + str(protopath),
        str(protopath / "api.proto"),
    )
)

# Fix import in generated stub file
# mypy-protobuf generates `import api_pb2` but mypy needs a relative import
# to properly resolve the module when checking the package
api_pb2_grpc_pyi = protopath / "api_pb2_grpc.pyi"
with open(api_pb2_grpc_pyi) as f:
    content = f.read()

# Replace absolute import with relative import
content = content.replace("import api_pb2\n", "from . import api_pb2\n")

with open(api_pb2_grpc_pyi, "w") as f:
    f.write(content)

# Note: Modern grpcio supports async via grpc.aio channels.
# No separate async stub generation needed - use DgraphStub with grpc.aio.Channel
