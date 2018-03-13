# Copyright 2016 DGraph Labs, Inc.
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
"""
test_txn.py

implements test case for transactions
"""

import grpc
import json
import logging
import os
import random
import time
import unittest

from pydgraph import client
from . import test_acct_upsert as integ


class TestClientTxns(integ.DgraphClientIntegrationTestCase):
    """Transactions test cases."""

    def setUp(self):
        """Drops all the existing schema and creates schema for tests."""
        super(TestClientTxns, self).setUp()

        _ = self.client.drop_all()
        _ = self.client.alter(schema="""name: string @index(fulltext) .""")

    def test_TxnReadAtStartTs(self):
        """Tests read after write when readTs == startTs"""
        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish"})
        self.assertEqual(1, len(assigned.uids), "Nothing was assigned")

        for _, uid in assigned.uids.items():
            uid = uid

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)
        resp = txn.query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp.json).get("me"))

    def test_TxnReadBeforeStartTs(self):
        """Tests read before write when readTs < startTs"""
        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish"})
        self.assertEqual(1, len(assigned.uids), "Nothing was assigned")

        for _, uid in assigned.uids.items():
            uid = uid

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)

        txn2 = self.client.txn()
        resp = txn2.query(query)
        self.assertEqual([], json.loads(resp.json).get("me"))

    def test_TxnReadAfterStartTs(self):
        """Tests read after commiting a write when readTs > startTs"""
        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish"})
        self.assertEqual(1, len(assigned.uids), "Nothing was assigned")

        for _, uid in assigned.uids.items():
            uid = uid
        _ = txn.commit()

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)
        txn2 = self.client.txn()
        resp = txn2.query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp.json).get("me"))

    def test_TxnReadBeforeAndAfterStartTs(self):
        """Test read before and after commiting a transaction when
        readTs1 < startTs and readTs2 > startTs"""
        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish"})
        self.assertEqual(1, len(assigned.uids), "Nothing was assigned")

        for _, uid in assigned.uids.items():
            uid = uid
        _ = txn.commit()

        txn2 = self.client.txn()
        # start a new txn and mutate the object
        txn3 = self.client.txn()
        assigned = txn3.mutate_obj({"uid": uid, "name": "Manish2"})

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)

        # the object is unmutated in other txns since txn3 is uncommitted
        resp2 = txn2.query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp2.json).get("me"))

        # once txn3 is committed, other txns observe the update
        txn3.commit()
        txn4 = self.client.txn()
        resp4 = txn4.query(query)
        self.assertEqual([{"name": "Manish2"}], json.loads(resp4.json).get("me"))

    def test_ReadFromNewClient(self):
        """Tests committed reads from a new client with startTs == 0."""
        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish"})
        self.assertEqual(1, len(assigned.uids), "Nothing was assigned")

        for _, uid in assigned.uids.items():
            uid = uid
        _ = txn.commit()

        client2 = client.DgraphClient(self.TEST_HOSTNAME, self.TEST_PORT)
        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)
        resp2 = client2.query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp2.json).get("me"))
        self.assertTrue(resp2.txn.start_ts > 0)

        txn2 = client2.txn()
        assigned = txn2.mutate_obj({"uid": uid, "name": "Manish2"})
        self.assertTrue(assigned.context.start_ts > 0)
        txn2.commit()

        resp = self.client.query(query)
        self.assertEqual([{"name": "Manish2"}], json.loads(resp.json).get("me"))

    def test_Conflict(self):
        """Tests committing two transactions which conflict."""
        _ = self.client.drop_all()

        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish"})
        for _, uid in assigned.uids.items():
            uid = uid

        txn2 = self.client.txn()
        assigned2 = txn2.mutate_obj({"uid": uid, "name": "Manish"})

        txn.commit()
        self.assertRaises(grpc._channel._Rendezvous, txn2.commit)

        txn3 = self.client.txn()
        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)
        resp3 = txn3.query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp3.json).get("me"))

    def test_ConflictReverseOrder(self):
        """Tests committing a transaction after a newer transaction has been
        committed."""
        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish"})
        for _, uid in assigned.uids.items():
            uid = uid

        txn2 = self.client.txn()
        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)
        resp = txn2.query(query)
        self.assertEqual([], json.loads(resp.json).get("me"))

        assigned2 = txn2.mutate_obj({"uid": uid, "name": "Jan the man"})
        txn2.commit()
        self.assertRaises(grpc._channel._Rendezvous, txn.commit)

        txn3 = self.client.txn()
        resp = txn3.query(query)
        self.assertEqual([{"name": "Jan the man"}], json.loads(resp.json).get("me"))

    def test_MutationAfterCommit(self):
        """Tests a second mutation after failing to commit a first mutation."""
        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish"})
        self.assertEqual(1, len(assigned.uids), "Nothing was assigned")
        for _, uid in assigned.uids.items():
            uid = uid

        txn2 = self.client.txn()
        assigned2 = txn2.mutate_obj({"uid": uid, "name": "Jan the man"})

        txn.commit()
        self.assertRaises(grpc._channel._Rendezvous, txn2.commit)

        txn3 = self.client.txn()
        assigned3 = txn3.mutate_obj({"uid": uid, "name": "Jan the man"})
        txn3.commit()

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(uid=uid)
        txn4 = self.client.txn()
        resp4 = txn4.query(query)
        self.assertEqual([{"name": "Jan the man"}], json.loads(resp4.json).get("me"))

    def test_ConflictIgnore(self):
        """Tests a mutation with ignore index conflict."""
        txn = self.client.txn()
        assigned1 = txn.mutate_obj({"name": "Manish"}, ignore_index_conflict=True)
        self.assertEqual(1, len(assigned1.uids), "Nothing was assigned")
        for _, uid in assigned1.uids.items():
            uid1 = uid

        txn2 = self.client.txn()
        assigned2 = txn2.mutate_obj({"name": "Manish"}, ignore_index_conflict=True)
        self.assertEqual(1, len(assigned2.uids), "Nothing was assigned")
        for _, uid in assigned2.uids.items():
            uid2 = uid

        txn.commit()
        txn2.commit()

        txn3 = self.client.txn()
        query = """{
            me(func: eq(name, "Manish")) {
                uid
            }
        }"""
        resp = txn3.query(query)
        self.assertEqual([{"uid": uid1}, {"uid": uid2}], json.loads(resp.json).get("me"))

    def test_ReadIndexKeySameTxn(self):
        """Tests reading an indexed field within a transaction."""
        _ = self.client.drop_all()
        _ = self.client.alter(schema="""name: string @index(exact) .""")

        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish"}, ignore_index_conflict=True)
        self.assertEqual(1, len(assigned.uids), "Nothing was assigned")
        for _, uid in assigned.uids.items():
            uid = uid

        query = """{
            me(func: le(name, "Manish")) {
                uid
            }
        }"""
        resp = txn.query(query)
        self.assertEqual([{"uid": uid}], json.loads(resp.json).get("me"))


class TestSPStar(integ.DgraphClientIntegrationTestCase):

    def setUp(self):
        """Drops everything and sets some schema."""
        super(TestSPStar, self).setUp()
        _ = self.client.drop_all()
        _ = self.client.alter(schema="""friend: uid .""")

    def test_SPStar(self):
        """Tests a Subj Predicate Star query."""
        txn = self.client.txn()
        assigned = txn.mutate_obj({"name": "Manish", "friend": [{"name": "Jan"}]})
        uid1 = assigned.uids["blank-0"]
        self.assertEqual(2, len(assigned.uids), "Expecting 2 uids to be created")
        txn.commit()

        txn2 = self.client.txn()
        assigned2 = txn2.mutate_obj(delobj={"uid": uid1, "friend": None})
        self.assertEqual(0, len(assigned2.uids))

        assigned3 = txn2.mutate_obj({
            "uid": uid1,
            "name": "Manish",
            "friend": [{"name": "Jan2"}]
        })
        self.assertEqual(1, len(assigned3.uids))
        uid2 = assigned3.uids["blank-0"]

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
            "uid": uid1,
            "friend": [{"name": "Jan2", "uid": uid2}]
        }], json.loads(resp.json).get("me"))

    def test_SPStar2(self):
        """Second test of Subject Predicate Star"""
        txn = self.client.txn()
        # Add edge to Jan
        assigned = txn.mutate_obj({"name": "Manish", "friend": [{"name": "Jan"}]})
        self.assertEqual(2, len(assigned.uids))
        uid1, uid2 = assigned.uids["blank-0"], assigned.uids["blank-1"]
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
            "uid": uid1,
            "friend": [{"name": "Jan", "uid": uid2}]
        }], json.loads(resp.json).get("me"))
        # Delete S P *
        deleted = txn.mutate_obj(delobj={"uid": uid1, "friend": None})
        self.assertEqual(0, len(deleted.uids))

        resp = txn.query(query)
        self.assertEqual([{"uid": uid1}], json.loads(resp.json).get("me"))

        # Add an edge to Jan2
        assigned2 = txn.mutate_obj({
            "uid": uid1,
            "name": "Manish",
            "friend": [{"name": "Jan2"}]
        })
        self.assertEqual(1, len(assigned2.uids))
        uid2 = assigned2.uids["blank-0"]
        resp = txn.query(query)
        self.assertEqual([{
            "uid": uid1,
            "friend": [{"name": "Jan2", "uid": uid2}]
        }], json.loads(resp.json).get("me"))
        # Delete S P *
        deleted2 = txn.mutate_obj(delobj={"uid": uid1, "friend": None})
        self.assertEqual(0, len(deleted2.uids))
        resp = txn.query(query)
        self.assertEqual([{"uid": uid1}], json.loads(resp.json).get("me"))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
