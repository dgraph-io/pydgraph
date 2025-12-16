# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests mutation after query behavior."""

__author__ = "Shailesh Kochhar <shailesh.kochhar@gmail.com>"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import json
import logging
import unittest

from . import helper


class TestEssentials(helper.ClientIntegrationTestCase):
    """Tests mutation after query behavior."""

    def testMutationAfterQuery(self):
        """Tests what happens when making a mutation on a txn after querying."""

        _ = self.client.txn(read_only=True).query(
            "{firsts(func: has(first)) { uid first }}"
        )

        txn = self.client.txn()
        mutation = txn.mutate(set_nquads='_:node <first> "Node name first" .')
        self.assertTrue(len(mutation.uids) > 0, "Mutation did not create new node")

        created = mutation.uids.get("node")
        self.assertIsNotNone(created)

        txn.commit()

        query = "{{node(func: uid({uid:s})) {{ uid }} }}".format(uid=created)
        reread = self.client.txn(read_only=True).query(query)
        self.assertEqual(created, json.loads(reread.json).get("node")[0]["uid"])


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestEssentials())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
