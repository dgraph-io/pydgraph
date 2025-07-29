# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Runs protoc with the gRPC plugin to generate messages and gRPC stubs."""

import os
import sys

from grpc_tools import protoc

# Check Python version compatibility
if sys.version_info >= (3, 13):
    print("ERROR: Python 3.13+ requires grpcio-tools >=1.66.2, which generates")
    print("protobufs that are incompatible with older grpcio-tools versions.")
    print("Please use Python 3.12 or lower to generate compatible protobufs.")
    print("Exiting without generating protobufs.")
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
