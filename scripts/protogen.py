# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runs protoc with the gRPC plugin to generate messages and gRPC stubs."""

import os
import sys

from grpc_tools import protoc

# Check Python version compatibility
if sys.version_info >= (3, 13):
    # Verify we have the correct grpcio-tools version for Python 3.13+
    import grpc_tools
    if hasattr(grpc_tools, '__version__'):
        version_parts = grpc_tools.__version__.split('.')
        major, minor = int(version_parts[0]), int(version_parts[1])
        if major < 1 or (major == 1 and minor < 66):
            print("ERROR: Python 3.13+ requires grpcio-tools >=1.66.2")
            print(f"Found grpcio-tools {grpc_tools.__version__}")
            print("Please upgrade: pip install 'grpcio-tools>=1.66.2'")
            sys.exit(1)

dirpath = os.path.dirname(os.path.realpath(__file__))
protopath = os.path.realpath(os.path.join(dirpath, "../pydgraph/proto"))

protoc.main(
    (
        "",
        "-I" + protopath,
        "--python_out=" + protopath,
        "--grpc_python_out=" + protopath,
        os.path.join(protopath, "api.proto"),
    )
)
# Note: Modern grpcio supports async via grpc.aio channels.
# No separate async stub generation needed - use DgraphStub with grpc.aio.Channel
