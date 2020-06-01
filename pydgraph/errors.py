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

"""Errors thrown by the Dgraph client."""

from pydgraph.meta import VERSION

__author__ = 'Garvit Pahal <garvit@dgraph.io>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'
__version__ = VERSION
__status__ = 'development'


class AbortedError(Exception):
    """Error thrown by aborted transactions."""

    def __init__(self):
        super(AbortedError, self).__init__(
            'Transaction has been aborted. Please retry')

class RetriableError(Exception):
    """Error thrown when the error return by Dgraph indicates the op should be retried."""

    def __init__(self, exception):
        self.exception = exception

    def __str__(self):
        return str(self.exception)

class ConnectionError(Exception):
    """Error thrown when the error return when the client has trouble connecting to Dgraph."""
    def __init__(self, exception):
        self.exception = exception

    def __str__(self):
        return str(self.exception)

class TransactionError(Exception):
    """Error thrown when the transaction is invalid (e.g trying to mutate in read-only mode)."""
    def __init__(self, msg):
        super(TransactionError, self).__init__(msg)

