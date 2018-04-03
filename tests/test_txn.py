# Copyright 2016 Dgraph Labs, Inc.
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

__author__ = 'Garvit Pahal <garvit@dgraph.io>'
__maintainer__ = 'Garvit Pahal <garvit@dgraph.io>'

import unittest
import logging
import json

import pydgraph

from . import helper


class TestTxn(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestTxn, self).setUp()

        helper.drop_all(self.client)
        helper.set_schema(self.client, 'name: string @index(fulltext) .')

    def test_read_at_start_ts(self):
        """Tests read after write when readTs == startTs"""

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish'})
        self.assertEqual(1, len(assigned.uids), 'Nothing was assigned')

        for _, uid in assigned.uids.items():
            uid = uid

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)
        resp = txn.query(query)
        self.assertEqual([{'name': 'Manish'}], json.loads(resp.json).get('me'))

    def test_read_before_start_ts(self):
        """Tests read before write when readTs < startTs"""

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish'})
        self.assertEqual(1, len(assigned.uids), 'Nothing was assigned')

        for _, uid in assigned.uids.items():
            uid = uid

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)

        resp = self.client.query(query)
        self.assertEqual([], json.loads(resp.json).get('me'))

    def test_read_after_start_ts(self):
        """Tests read after committing a write when readTs > startTs"""

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish'})
        self.assertEqual(1, len(assigned.uids), 'Nothing was assigned')

        for _, uid in assigned.uids.items():
            uid = uid
        txn.commit()

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)

        resp = self.client.query(query)
        self.assertEqual([{'name': 'Manish'}], json.loads(resp.json).get('me'))

    def test_read_before_and_after_start_ts(self):
        """Test read before and after committing a transaction when
        readTs1 < startTs and readTs2 > startTs"""

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish'})
        self.assertEqual(1, len(assigned.uids), 'Nothing was assigned')

        for _, uid in assigned.uids.items():
            uid = uid
        txn.commit()

        txn2 = self.client.txn()

        # start a new txn and mutate the object
        txn3 = self.client.txn()
        _ = txn3.mutate(set_obj={'uid': uid, 'name': 'Manish2'})

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)

        # object is unchanged since txn3 is uncommitted
        resp2 = txn2.query(query)
        self.assertEqual([{'name': 'Manish'}], json.loads(resp2.json).get('me'))

        # once txn3 is committed, other txns observe the update
        txn3.commit()

        resp4 = self.client.query(query)
        self.assertEqual([{'name': 'Manish2'}], json.loads(resp4.json).get('me'))

    def test_read_from_new_client(self):
        """Tests committed reads from a new client with startTs == 0."""

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish'})
        self.assertEqual(1, len(assigned.uids), 'Nothing was assigned')

        for _, uid in assigned.uids.items():
            uid = uid
        txn.commit()

        client2 = helper.create_client(self.TEST_SERVER_ADDR)
        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)

        resp2 = client2.query(query)
        self.assertEqual([{'name': 'Manish'}], json.loads(resp2.json).get('me'))
        self.assertTrue(resp2.txn.start_ts > 0)

        txn2 = client2.txn()
        assigned = txn2.mutate(set_obj={'uid': uid, 'name': 'Manish2'})
        self.assertTrue(assigned.context.start_ts > 0)
        txn2.commit()

        resp = self.client.query(query)
        self.assertEqual([{'name': 'Manish2'}], json.loads(resp.json).get('me'))

    def test_conflict(self):
        """Tests committing two transactions which conflict."""

        helper.drop_all(self.client)

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish'})
        self.assertEqual(1, len(assigned.uids), 'Nothing was assigned')

        for _, uid in assigned.uids.items():
            uid = uid

        txn2 = self.client.txn()
        _ = txn2.mutate(set_obj={'uid': uid, 'name': 'Manish'})

        txn.commit()
        self.assertRaises(pydgraph.AbortedError, txn2.commit)

        txn3 = self.client.txn()
        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)

        resp3 = txn3.query(query)
        self.assertEqual([{'name': 'Manish'}], json.loads(resp3.json).get('me'))

    def test_conflict_reverse_order(self):
        """Tests committing a transaction after a newer transaction has been
        committed."""

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish'})
        self.assertEqual(1, len(assigned.uids), 'Nothing was assigned')

        for _, uid in assigned.uids.items():
            uid = uid

        txn2 = self.client.txn()
        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)

        resp = txn2.query(query)
        self.assertEqual([], json.loads(resp.json).get('me'))

        _ = txn2.mutate(set_obj={'uid': uid, 'name': 'Jan the man'})
        txn2.commit()

        self.assertRaises(pydgraph.AbortedError, txn.commit)

        txn3 = self.client.txn()
        resp = txn3.query(query)
        self.assertEqual([{'name': 'Jan the man'}], json.loads(resp.json).get('me'))

    def test_mutation_after_commit(self):
        """Tests a second mutation after failing to commit a first mutation."""

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish'})
        self.assertEqual(1, len(assigned.uids), 'Nothing was assigned')

        for _, uid in assigned.uids.items():
            uid = uid

        txn2 = self.client.txn()
        _ = txn2.mutate(set_obj={'uid': uid, 'name': 'Jan the man'})

        txn.commit()
        self.assertRaises(pydgraph.AbortedError, txn2.commit)

        txn3 = self.client.txn()
        _ = txn3.mutate(set_obj={'uid': uid, 'name': 'Jan the man'})
        txn3.commit()

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)

        resp4 = self.client.query(query)
        self.assertEqual([{'name': 'Jan the man'}], json.loads(resp4.json).get('me'))

    def test_conflict_ignore(self):
        """Tests a mutation with ignore index conflict."""

        txn = self.client.txn()
        assigned1 = txn.mutate(set_obj={'name': 'Manish'}, ignore_index_conflict=True)
        self.assertEqual(1, len(assigned1.uids), 'Nothing was assigned')

        for _, uid in assigned1.uids.items():
            uid1 = uid

        txn2 = self.client.txn()
        assigned2 = txn2.mutate(set_obj={'name': 'Manish'}, ignore_index_conflict=True)
        self.assertEqual(1, len(assigned2.uids), 'Nothing was assigned')

        for _, uid in assigned2.uids.items():
            uid2 = uid

        txn.commit()
        txn2.commit()

        query = """{
            me(func: eq(name, "Manish")) {
                uid
            }
        }"""

        resp = self.client.query(query)
        self.assertEqual([{'uid': uid1}, {'uid': uid2}], json.loads(resp.json).get('me'))

    def test_read_index_key_same_txn(self):
        """Tests reading an indexed field within a transaction."""

        helper.drop_all(self.client)
        helper.set_schema(self.client, 'name: string @index(exact) .')

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish'}, ignore_index_conflict=True)
        self.assertEqual(1, len(assigned.uids), 'Nothing was assigned')

        for _, uid in assigned.uids.items():
            uid = uid

        query = """{
            me(func: le(name, "Manish")) {
                uid
            }
        }"""

        resp = txn.query(query)
        self.assertEqual([{'uid': uid}], json.loads(resp.json).get('me'))


class TestSPStar(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestSPStar, self).setUp()

        helper.drop_all(self.client)
        helper.set_schema(self.client, 'friend: uid .')

    def test_sp_star(self):
        """Tests a Subject Predicate Star query."""

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish', 'friend': [{'name': 'Jan'}]})
        uid1 = assigned.uids['blank-0']
        self.assertEqual(2, len(assigned.uids), 'Expected 2 nodes to be created')

        txn.commit()

        txn2 = self.client.txn()
        assigned2 = txn2.mutate(del_obj={'uid': uid1, 'friend': None})
        self.assertEqual(0, len(assigned2.uids))

        assigned3 = txn2.mutate(set_obj={
            'uid': uid1,
            'name': 'Manish',
            'friend': [{'name': 'Jan2'}]
        })
        self.assertEqual(1, len(assigned3.uids))
        uid2 = assigned3.uids['blank-0']

        query = """{{
            me(func: uid("{uid:s}")) {{
                uid
                friend {{
                    uid
                    name
                }}
            }}
        }}""".format(uid=uid1)

        resp = txn2.query(query)
        self.assertEqual([{
            'uid': uid1,
            'friend': [{'name': 'Jan2', 'uid': uid2}]
        }], json.loads(resp.json).get('me'))

    def test_sp_star2(self):
        """Second test of Subject Predicate Star"""

        txn = self.client.txn()
        assigned = txn.mutate(set_obj={'name': 'Manish', 'friend': [{'name': 'Jan'}]})
        self.assertEqual(2, len(assigned.uids))
        uid1, uid2 = assigned.uids['blank-0'], assigned.uids['blank-1']

        query = """{{
            me(func: uid("{uid:s}")) {{
                uid
                friend {{
                    uid
                    name
                }}
            }}
        }}""".format(uid=uid1)

        resp = txn.query(query)
        self.assertEqual([{
            'uid': uid1,
            'friend': [{'name': 'Jan', 'uid': uid2}]
        }], json.loads(resp.json).get('me'))

        deleted = txn.mutate(del_obj={'uid': uid1, 'friend': None})
        self.assertEqual(0, len(deleted.uids))

        resp = txn.query(query)
        self.assertEqual([{'uid': uid1}], json.loads(resp.json).get('me'))

        # add an edge to Jan2
        assigned2 = txn.mutate(set_obj={
            'uid': uid1,
            'name': 'Manish',
            'friend': [{'name': 'Jan2'}]
        })
        self.assertEqual(1, len(assigned2.uids))
        uid2 = assigned2.uids['blank-0']

        resp = txn.query(query)
        self.assertEqual([{
            'uid': uid1,
            'friend': [{'name': 'Jan2', 'uid': uid2}]
        }], json.loads(resp.json).get('me'))
        
        deleted2 = txn.mutate(del_obj={'uid': uid1, 'friend': None})
        self.assertEqual(0, len(deleted2.uids))
        resp = txn.query(query)
        self.assertEqual([{'uid': uid1}], json.loads(resp.json).get('me'))


def suite():
    s = unittest.TestSuite()
    s.addTest(TestTxn())
    s.addTest(TestSPStar())
    return s


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
