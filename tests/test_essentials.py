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
__maintainer__ = 'Shailesh Kochhar <shailesh.kochhar@gmail.com>'

import grpc
import json
import logging
import unittest
from pydgraph import client

import test_acct_upsert as integ

class TestEssentials(integ.DgraphClientIntegrationTestCase):
    """Tests the essentials of the client."""

    def testMutationAfterQuery(self):
        """Tests what happens when making a mutation on a txn after querying
        on the client."""
        _ = self.client.query('''{firsts(func: has(first)) { uid first }}''')

        txn = self.client.txn()
        mutation = txn.mutate(setnquads='_:node <first> "Node name first" .')
        self.assertTrue(len(mutation.uids) > 0, "Mutation did not create new node")
        created = mutation.uids.get('node')
        self.assertIsNotNone(created)
        txn.commit()

        query = '{{node(func: uid({uid:s})) {{ uid }} }}'.format(uid=created)
        reread = self.client.query(query)
        self.assertEqual(created, json.loads(reread.json).get('node')[0]['uid'])


    def testMultipleMutationsInTxn(self):
        """Test what happens when running multiple mutations in a txn."""
        txn = self.client.txn()

        for i in range(100):
            m = txn.mutate('''_:node <seq> "{seq:d}" .'''.format(seq=i))

        self.assertEqual(100, len(txn.keys),
                         "Expected txn to have only 100 modified keys.")
        key_counter = {}
        for key in txn.keys:
            key_counter[key] = key_counter.setdefault(key, 0) + 1
        recurring_keys = [(k, c) for (k, c) in key_counter.items() if c > 1]
        self.assertEqual(0, len(recurring_keys),
                         "Expected txn not to have recurring keys")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
