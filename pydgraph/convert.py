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

"""Functions to transform data."""

def extract_dict(nodes: dict, edges: list, data: dict, parent: dict = None, name: str = None):
    """Recursively extract nodes and edges from a dict created from the result of a Dgraph query.

    Nodes (vertices) from the query must have an ``id`` field in order to be recognized
    as a node. Optionally, if a ``type`` field is present (either as a list or a string),
    the type will be applied to the node. Attributes of nodes encountered in more than 
    one place in the result dict will be merged.

    Edges are automatically extracted from the query result. If a node has an an id and a parent,
    a relationship is made. The relationship predicate name is assigned as the edge type.

    Note: this code is experimental and may change in the future.
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

