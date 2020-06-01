# Copyright 2018 Dgraph Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Various utility functions."""

import grpc
import sys

from pydgraph.meta import VERSION

__author__ = 'Shailesh Kochhar <shailesh.kochhar@gmail.com>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'
__version__ = VERSION
__status__ = 'development'


def is_string(string):
    """Checks if argument is a string. Compatible with Python 2 and 3."""
    if sys.version_info[0] < 3:
        return isinstance(string, basestring)

    return isinstance(string, str)

def is_jwt_expired(exception):
    return 'Token is expired' in str(exception)

def is_aborted_error(error):
    """Returns true if the error is due to an aborted transaction."""
    if isinstance(error, grpc._channel._Rendezvous) or \
       isinstance(error, grpc._channel._InactiveRpcError):
        status_code = error.code()
        if (status_code == grpc.StatusCode.ABORTED or
            status_code == grpc.StatusCode.FAILED_PRECONDITION):
            return True
    return False

def is_retriable_error(error):
    """Returns true if the error is retriable (e.g server is not ready yet)."""
    msg = str(error)
    return 'Please retry' in msg or 'opIndexing is already running' in msg

def is_connection_error(error):
    """Returns true if the error is caused connection issues."""
    msg = str(error)
    return 'Unhealthy connection' in msg or 'No connection exists' in msg
