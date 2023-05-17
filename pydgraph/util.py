# Copyright 2023 Dgraph Labs, Inc.
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

def extract_dict(nodes: dict, edges: list, data: dict, parent: dict = None, name: str = None):
    """Recursively extract nodes and edges from a dict created from the result of a Dgraph query.

    Nodes (vertices) from the query must have an ``id`` field in order to be recognized
    as a node. Optionally, if a ``type`` field is present (either as a list or a string),
    the type will be applied to the node. Attributes of nodes encountered in more than 
    one place in the result dict will be merged.

    Edges are automatically extracted from the query result. If a node has an an id and a parent,
    a relationship is made. The relationship predicate name is assigned as the edge type.
    """
    def update_node(nodes: dict, key: str, value: dict):
        if not key in nodes:
            nodes[key] = {}
        for k, v in value.items():
            if not isinstance(v, list):
                nodes[key][k] = v
                
    if isinstance(data, dict):
        # ignore the Dgraph 'extensions' field
        if name == "extensions":
            return
        # id is a special field, we use it to identify nodes
        if "id" in data:
            update_node(nodes, data['id'], data)
            # if we have a parent, add an edge
            if parent and "id" in parent:
                edges.append(
                    {"src": parent["id"], "dst": data["id"], "type": name})
        # recurse into the dict
        for key, value in data.items():
            if isinstance(value, dict):
                extract_dict(nodes, edges, value, data, key)
            elif isinstance(value, list) and len(value) > 0:
                # if the list is named 'type', assign it to the node
                if key == "type":
                    update_node(nodes, data["id"], {"type": value[0]})
                    continue
                # else, recurse into the list if it contains dicts
                if isinstance(value[0], dict):
                    for v in value:
                        extract_dict(nodes, edges, v, data, key)
                # list is of scalars, assign it to the node
                else:
                    nodes[data['id']][key] = value
