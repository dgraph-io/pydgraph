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

__author__ = 'Shailesh Kochhar <shailesh.kochhar@gmail.com>'
__maintainer__ = 'Garvit Pahal <garvit@dgraph.io>'

import unittest
import logging
import json

from . import helper


class TestEssentials(helper.ClientIntegrationTestCase):
    def testMutationAfterQuery(self):
        """Tests what happens when making a mutation on a txn after querying
        on the client."""

        _ = self.client.query('{firsts(func: has(first)) { uid first }}')

        txn = self.client.txn()
        mutation = txn.mutate(set_nquads='_:node <first> "Node name first" .')
        self.assertTrue(len(mutation.uids) > 0, 'Mutation did not create new node')

        created = mutation.uids.get('node')
        self.assertIsNotNone(created)

        txn.commit()

        query = '{{node(func: uid({uid:s})) {{ uid }} }}'.format(uid=created)
        reread = self.client.query(query)
        self.assertEqual(created, json.loads(reread.json).get('node')[0]['uid'])


def suite():
    s = unittest.TestSuite()
    s.addTest(TestEssentials())
    return s


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
