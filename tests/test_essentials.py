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

"""Tests mutation after query behavior."""

__author__ = 'Shailesh Kochhar <shailesh.kochhar@gmail.com>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'

import unittest
import logging
import json

from tests import helper


class TestEssentials(helper.ClientIntegrationTestCase):
    """Tests mutation after query behavior."""

    def testMutationAfterQuery(self):
        """Tests what happens when making a mutation on a txn after querying."""

        _ = self.client.txn(read_only=True).query('{firsts(func: has(first)) { uid first }}')

        txn = self.client.txn()
        mutation = txn.mutate(set_nquads='_:node <first> "Node name first" .')
        self.assertTrue(len(mutation.uids) > 0, 'Mutation did not create new node')

        created = mutation.uids.get('node')
        self.assertIsNotNone(created)

        txn.commit()

        query = '{{node(func: uid({uid:s})) {{ uid }} }}'.format(uid=created)
        reread = self.client.txn(read_only=True).query(query)
        self.assertEqual(created, json.loads(reread.json).get('node')[0]['uid'])


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestEssentials())
    return suite_obj


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
