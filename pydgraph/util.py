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

import json
import sys
import time

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


def wait_for_indexing(client, pred, tokenizers, count, reverse):
    """Waits for indexes to be built."""
    tokenizers.sort()
    query = "schema(pred: [{}]){{tokenizer reverse count}}".format(pred)
    while True:
        resp = client.txn(read_only=True).query(query)
        if has_indexes(resp, tokenizers, count, reverse):
            break
        time.sleep(0.1)

def has_indexes(resp, tokenizers, count, reverse):
    schema = json.loads(resp.json)
    if len(schema["schema"]) != 1:
        return False
    index = schema["schema"][0]
    if len(index.get("tokenizer", [])) != len(tokenizers):
        return False
    if index.get("count", False) != count or index.get("reverse", False) != reverse:
        return False
    # if no tokenizer is expected
    if len(index.get("tokenizer", [])) == 0:
        return True
    index["tokenizer"].sort()
    for i in range(len(tokenizers)):
        if tokenizers[i] != index["tokenizer"][i]:
            return False
    return True
