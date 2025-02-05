# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

"""Functions to transform data."""


def extract_dict(
    nodes: dict, edges: list, data: dict, parent: dict = None, name: str = None
):
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
        if key not in nodes:
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
            update_node(nodes, data["id"], data)
            # if we have a parent, add an edge
            if parent and "id" in parent:
                edges.append({"src": parent["id"], "dst": data["id"], "type": name})
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
                    nodes[data["id"]][key] = value
