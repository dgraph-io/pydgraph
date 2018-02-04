"""
Module containing utilities for dgraph RPC messages
"""

__author__ = 'Shailesh Kochhar <shailesh.kochhar@gmail.com>'


def merge_lin_reads(current, update_lin_read):
    """Merges LinRead protobufs by adding all keys in the ids field from the
    updated_lin_read to the current one. If the key already exists, the one
    with the larger value is preserved."""
    # cache for the loop
    curr_lin_read_ids = current.ids
    curr_lin_read_ids_get = curr_lin_read_ids.get

    for (key, update_value) in update_lin_read.ids.items():
        if curr_lin_read_ids_get(key, 0) <= update_value:
            curr_lin_read_ids[key] = update_value
