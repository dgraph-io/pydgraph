# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Runs protoc with the gRPC plugin to generate messages and gRPC stubs."""

import os

from grpc_tools import protoc

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
