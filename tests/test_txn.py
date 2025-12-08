# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

__author__ = "Garvit Pahal"
__maintainer__ = "Istari Digital, Inc. <dgraph-admin@istaridigital.com>"

import json
import logging
import time
import unittest

import pydgraph

from tests import helper


class TestTxn(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestTxn, self).setUp()

        helper.drop_all(self.client)
        helper.set_schema(self.client, "name: string @index(fulltext) @upsert .")

    def test_query_after_commit(self):
        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid

        txn.commit()

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )

        with self.assertRaises(Exception):
            txn.query(query)

    def test_mutate_after_commit(self):
        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        txn.commit()

        with self.assertRaises(Exception):
            txn.mutate(set_obj={"name": "Manish2"})

    def test_commit_now(self):
        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"}, commit_now=True)
        self.assertEqual(1, len(response.uids), "Nothing was assigned")
        for _, uid in response.uids.items():
            uid = uid

        self.assertRaises(Exception, txn.commit)

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )
        resp = self.client.txn(read_only=True).query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp.json).get("me"))

    def test_discard(self):
        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")
        txn.commit()

        for _, uid in response.uids.items():
            uid = uid

        txn2 = self.client.txn()
        _ = txn2.mutate(set_obj={"uid": uid, "name": "Manish2"})

        txn.discard()
        self.assertRaises(Exception, txn.commit)

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )
        resp = self.client.txn(read_only=True).query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp.json).get("me"))

    def test_mutate_error(self):
        txn = self.client.txn()
        with self.assertRaises(Exception):
            # Following N-Quad is invalid
            _ = txn.mutate(set_nquads="_:node <name> Manish")

        self.assertRaises(Exception, txn.commit)

    def test_read_at_start_ts(self):
        """Tests read after write when readTs == startTs"""

        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )
        resp = txn.query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp.json).get("me"))

    def test_read_before_start_ts(self):
        """Tests read before write when readTs < startTs"""

        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )

        resp = self.client.txn(read_only=True).query(query)
        self.assertEqual([], json.loads(resp.json).get("me"))

    def test_read_after_start_ts(self):
        """Tests read after committing a write when readTs > startTs"""

        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid
        txn.commit()

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )

        resp = self.client.txn(read_only=True).query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp.json).get("me"))

    def test_read_before_and_after_start_ts(self):
        """Test read before and after committing a transaction when
        readTs1 < startTs and readTs2 > startTs"""

        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid
        txn.commit()

        txn2 = self.client.txn()

        # start a new txn and mutate the object
        txn3 = self.client.txn()
        _ = txn3.mutate(set_obj={"uid": uid, "name": "Manish2"})

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )

        # object is unchanged since txn3 is uncommitted
        resp2 = txn2.query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp2.json).get("me"))

        # once txn3 is committed, other txns observe the update
        txn3.commit()

        resp4 = self.client.txn(read_only=True).query(query)
        self.assertEqual([{"name": "Manish2"}], json.loads(resp4.json).get("me"))

    def test_read_from_new_client(self):
        """Tests committed reads from a new client with startTs == 0."""

        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid
        txn.commit()

        client2 = helper.create_client(self.TEST_SERVER_ADDR)
        client2.login("groot", "password")
        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )

        resp2 = client2.txn(read_only=True).query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp2.json).get("me"))
        self.assertTrue(resp2.txn.start_ts > 0)

        txn2 = client2.txn()
        response = txn2.mutate(set_obj={"uid": uid, "name": "Manish2"})
        self.assertTrue(response.txn.start_ts > 0)
        txn2.commit()

        resp = self.client.txn(read_only=True).query(query)
        self.assertEqual([{"name": "Manish2"}], json.loads(resp.json).get("me"))

    def test_read_only_txn(self):
        """Tests read-only transactions. Read-only transactions should
        not advance the start ts nor should allow mutations or commits."""

        # We sleep here so that rollups do not move the MaxAssigned.
        # Starting Dgraph v23, rollups can move the MaxAssigned too.
        # PR: https://github.com/dgraph-io/dgraph/pull/8774
        time.sleep(1)

        query = "{ me() {} }"
        resp1 = self.client.txn(read_only=True).query(query)
        start_ts1 = resp1.txn.start_ts
        resp2 = self.client.txn(read_only=True).query(query)
        start_ts2 = resp2.txn.start_ts
        self.assertEqual(start_ts1, start_ts2)

        txn = self.client.txn(read_only=True)
        resp1 = txn.query(query)
        start_ts1 = resp1.txn.start_ts
        resp2 = txn.query(query)
        start_ts2 = resp2.txn.start_ts
        self.assertEqual(start_ts1, start_ts2)

        with self.assertRaises(Exception):
            txn.mutate(set_obj={"name": "Manish"})
        with self.assertRaises(Exception):
            txn.commit()

    def test_best_effort_txn(self):
        """Tests best-effort transactions."""

        helper.drop_all(self.client)
        helper.set_schema(self.client, "name: string @index(exact) .")

        with self.assertRaises(Exception):
            self.client.txn(read_only=False, best_effort=True)

        query = "{ me(func: has(name)) {name} }"
        rtxn = self.client.txn(read_only=True, best_effort=True)
        resp = rtxn.query(query)
        self.assertEqual([], json.loads(resp.json).get("me"))

        txn = self.client.txn()
        resp = txn.mutate(set_obj={"name": "Manish"})
        txn.commit()
        mu_ts = resp.txn.commit_ts

        resp = rtxn.query(query)
        self.assertEqual([], json.loads(resp.json).get("me"))

        while True:
            txn = self.client.txn(read_only=True)
            resp = txn.query(query)
            if resp.txn.start_ts < mu_ts:
                continue
            self.assertEqual([{"name": "Manish"}], json.loads(resp.json).get("me"))
            break

    def test_conflict(self):
        """Tests committing two transactions which conflict."""

        helper.drop_all(self.client)

        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid

        txn2 = self.client.txn()
        _ = txn2.mutate(set_obj={"uid": uid, "name": "Manish"})

        txn.commit()
        self.assertRaises(pydgraph.AbortedError, txn2.commit)

        txn3 = self.client.txn()
        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )

        resp3 = txn3.query(query)
        self.assertEqual([{"name": "Manish"}], json.loads(resp3.json).get("me"))

    def test_conflict_reverse_order(self):
        """Tests committing a transaction after a newer transaction has been
        committed."""

        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid

        txn2 = self.client.txn()
        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )

        resp = txn2.query(query)
        self.assertEqual([], json.loads(resp.json).get("me"))

        _ = txn2.mutate(set_obj={"uid": uid, "name": "Jan the man"})
        txn2.commit()

        self.assertRaises(pydgraph.AbortedError, txn.commit)

        txn3 = self.client.txn()
        resp = txn3.query(query)
        self.assertEqual([{"name": "Jan the man"}], json.loads(resp.json).get("me"))

    def test_commit_conflict(self):
        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid

        txn2 = self.client.txn()
        _ = txn2.mutate(set_obj={"uid": uid, "name": "Jan the man"})

        txn.commit()
        self.assertRaises(pydgraph.AbortedError, txn2.commit)

        txn3 = self.client.txn()
        _ = txn3.mutate(set_obj={"uid": uid, "name": "Jan the man"})
        txn3.commit()

        query = """{{
            me(func: uid("{uid:s}")) {{
                name
            }}
        }}""".format(
            uid=uid
        )

        resp4 = self.client.txn(read_only=True).query(query)
        self.assertEqual([{"name": "Jan the man"}], json.loads(resp4.json).get("me"))

    def test_mutate_conflict(self):
        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid

        txn2 = self.client.txn()
        _ = txn2.mutate(set_obj={"uid": uid, "name": "Jan the man"})

        txn2.commit()

        _ = txn.mutate(set_obj={"uid": uid, "name": "Manish2"})
        self.assertRaises(pydgraph.AbortedError, txn.commit)

    def test_read_index_key_same_txn(self):
        """Tests reading an indexed field within a transaction. The read
        should return the results from before any writes of the same
        txn."""

        helper.drop_all(self.client)
        helper.set_schema(self.client, "name: string @index(exact) .")

        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Manish"})
        self.assertEqual(1, len(response.uids), "Nothing was assigned")

        for _, uid in response.uids.items():
            uid = uid

        query = """{
            me(func: le(name, "Manish")) {
                uid
            }
        }"""

        resp = txn.query(query)
        self.assertEqual(
            [], json.loads(resp.json).get("me"), "Expected 0 nodes read from index"
        )

    def test_non_string_variable(self):
        """Tests sending a variable map with non-string values or keys results
        in an Exception."""
        helper.drop_all(self.client)
        helper.set_schema(self.client, "name: string @index(exact) .")

        txn = self.client.txn()
        query = """
            query node($a: string) {
                node(func: eq(name, $a))
                {
                    expand(_all_)
                }
            }
        """
        variables = {"$a": 1234}
        with self.assertRaises(Exception):
            _ = txn.query(query, variables=variables)

    def test_finished(self):
        txn = self.client.txn()
        txn.mutate(set_nquads='_:animesh <name> "Animesh" .', commit_now=True)

        with self.assertRaises(Exception):
            txn.mutate(set_nquads='_:aman <name> "Aman" .', commit_now=True)

    def test_mutate_facet(self):
        """Tests mutations that include facets work as expected."""
        helper.drop_all(self.client)
        helper.set_schema(
            self.client,
            """
name: string .
friend: uid .
""",
        )

        nquads = """
_:a <name> "aaa" (close_friend=true) .
_:b <name> "bbb" .
_:a <friend> _:b (close_friend=true).
"""

        txn = self.client.txn()
        _ = txn.mutate(set_nquads=nquads, commit_now=True)

        query = """
{
  q1(func: has(name), orderasc: name) {
    name @facets(close_friend)
  }

  q2(func: has(friend)) {
    friend @facets(close_friend) {
      name
    }
  }
}
"""
        txn = self.client.txn()
        resp = txn.query(query)
        self.assertEqual(
            [{"name": "aaa", "name|close_friend": True}, {"name": "bbb"}],
            json.loads(resp.json).get("q1"),
        )
        self.assertEqual(
            [{"friend": {"name": "bbb", "friend|close_friend": True}}],
            json.loads(resp.json).get("q2"),
        )


class TestSPStar(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestSPStar, self).setUp()

        helper.drop_all(self.client)
        helper.set_schema(self.client, "friend: [uid] .")

    def test_sp_star(self):
        """Tests a Subject Predicate Star query."""

        txn = self.client.txn()
        response = txn.mutate(
            set_obj={"uid": "_:manish", "name": "Manish", "friend": [{"name": "Jan"}]}
        )
        uid1 = response.uids["manish"]
        self.assertEqual(2, len(response.uids), "Expected 2 nodes to be created")

        txn.commit()

        txn2 = self.client.txn()
        response2 = txn2.mutate(del_obj={"uid": uid1, "friend": None})
        self.assertEqual(0, len(response2.uids))

        response3 = txn2.mutate(
            set_obj={
                "uid": uid1,
                "name": "Manish",
                "friend": [{"uid": "_:jan2", "name": "Jan2"}],
            }
        )
        self.assertEqual(1, len(response3.uids))
        uid2 = response3.uids["jan2"]

        query = """{{
            me(func: uid("{uid:s}")) {{
                uid
                friend {{
                    uid
                    name
                }}
            }}
        }}""".format(
            uid=uid1
        )

        resp = txn2.query(query)
        self.assertEqual(
            [{"uid": uid1, "friend": [{"name": "Jan2", "uid": uid2}]}],
            json.loads(resp.json).get("me"),
        )

    def test_sp_star2(self):
        """Second test of Subject Predicate Star"""

        txn = self.client.txn()
        response = txn.mutate(
            set_obj={
                "uid": "_:manish",
                "name": "Manish",
                "friend": [{"uid": "_:jan", "name": "Jan"}],
            }
        )
        self.assertEqual(2, len(response.uids))
        uid1, uid2 = response.uids["manish"], response.uids["jan"]

        query = """{{
            me(func: uid("{uid:s}")) {{
                uid
                friend {{
                    uid
                    name
                }}
            }}
        }}""".format(
            uid=uid1
        )

        resp = txn.query(query)
        self.assertEqual(
            [{"uid": uid1, "friend": [{"name": "Jan", "uid": uid2}]}],
            json.loads(resp.json).get("me"),
        )

        deleted = txn.mutate(del_obj={"uid": uid1, "friend": None})
        self.assertEqual(0, len(deleted.uids))

        resp = txn.query(query)
        self.assertEqual([{"uid": uid1}], json.loads(resp.json).get("me"))

        # add an edge to Jan2
        response2 = txn.mutate(
            set_obj={
                "uid": uid1,
                "name": "Manish",
                "friend": [{"uid": "_:jan2", "name": "Jan2"}],
            }
        )
        self.assertEqual(1, len(response2.uids))
        uid2 = response2.uids["jan2"]

        resp = txn.query(query)
        self.assertEqual(
            [{"uid": uid1, "friend": [{"name": "Jan2", "uid": uid2}]}],
            json.loads(resp.json).get("me"),
        )

        deleted2 = txn.mutate(del_obj={"uid": uid1, "friend": None})
        self.assertEqual(0, len(deleted2.uids))
        resp = txn.query(query)
        self.assertEqual([{"uid": uid1}], json.loads(resp.json).get("me"))


class TestContextManager(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestContextManager, self).setUp()
        helper.drop_all(self.client)
        helper.set_schema(self.client, "name: string @index(fulltext) .")

    def test_context_manager_by_contextlib(self):
        """Test context manager via client.begin() for read-only transactions."""
        q = '''
        {
            company(func: type(x.Company), first: 10){
                    expand(_all_)
            }
        }
        '''
        with self.client.begin(read_only=True, best_effort=True) as tx:
            response = tx.query(q)
        self.assertIsNotNone(response)
        data = json.loads(response.json)

    def test_context_manager_by_class(self):
        """Test context manager using Txn class directly for read-only transactions."""
        q = '''
        {
            company(func: type(x.Company), first: 10){
                    expand(_all_)
            }
        }
        '''
        with pydgraph.Txn(self.client, read_only=True, best_effort=True) as tx:
            response = tx.query(q)
        self.assertIsNotNone(response)
        data = json.loads(response.json)

    def test_context_manager_auto_commit(self):
        """Test that write transactions automatically commit on successful completion."""
        with self.client.txn() as txn:
            response = txn.mutate(set_obj={"name": "Alice"})
            self.assertEqual(1, len(response.uids), "Nothing was assigned")
            uid = list(response.uids.values())[0]

        # Verify the data was committed by querying in a new transaction
        query = '''{{
            me(func: uid("{uid}")) {{
                name
            }}
        }}'''.format(uid=uid)

        resp = self.client.txn(read_only=True).query(query)
        self.assertEqual([{"name": "Alice"}], json.loads(resp.json).get("me"))

    def test_context_manager_read_only_auto_discard(self):
        """Test that read-only transactions automatically discard."""
        # Create some data first
        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Bob"})
        uid = list(response.uids.values())[0]
        txn.commit()

        # Read-only transaction should auto-discard (not commit)
        query = '''{{
            me(func: uid("{uid}")) {{
                name
            }}
        }}'''.format(uid=uid)

        with self.client.txn(read_only=True) as txn:
            resp = txn.query(query)
            self.assertEqual([{"name": "Bob"}], json.loads(resp.json).get("me"))

        # Transaction should be finished after context manager exits
        self.assertTrue(txn._finished)

    def test_context_manager_exception_handling(self):
        """Test that exceptions cause automatic discard and are re-raised."""
        with self.assertRaises(ValueError):
            with self.client.txn() as txn:
                response = txn.mutate(set_obj={"name": "Charlie"})
                uid = list(response.uids.values())[0]
                raise ValueError("Test exception")

        # Verify transaction was discarded - data should not exist
        query = '''{{
            me(func: has(name)) {{
                name
            }}
        }}'''

        resp = self.client.txn(read_only=True).query(query)
        results = json.loads(resp.json).get("me")
        # Should be empty or not contain Charlie
        if results:
            names = [r.get("name") for r in results]
            self.assertNotIn("Charlie", names)

    def test_context_manager_transaction_finished_after_exit(self):
        """Test that transaction is marked as finished after exiting context manager."""
        with self.client.txn() as txn:
            txn.mutate(set_obj={"name": "David"})
            self.assertFalse(txn._finished)

        # Should be finished after exit
        self.assertTrue(txn._finished)

        # Should not be able to use transaction after context manager
        with self.assertRaises(Exception):
            txn.query("{ me() {} }")

    def test_context_manager_multiple_mutations(self):
        """Test multiple mutations within a single context manager."""
        with self.client.txn() as txn:
            response1 = txn.mutate(set_obj={"name": "Eve"})
            uid1 = list(response1.uids.values())[0]

            response2 = txn.mutate(set_obj={"name": "Frank"})
            uid2 = list(response2.uids.values())[0]

        # Verify both mutations were committed
        query = '''{{
            me(func: has(name), orderasc: name) {{
                name
            }}
        }}'''

        resp = self.client.txn(read_only=True).query(query)
        results = json.loads(resp.json).get("me")
        names = [r.get("name") for r in results]
        self.assertIn("Eve", names)
        self.assertIn("Frank", names)

    def test_context_manager_query_and_mutate(self):
        """Test both query and mutate operations within a context manager."""
        # Create initial data
        txn = self.client.txn()
        response = txn.mutate(set_obj={"name": "Grace"})
        uid = list(response.uids.values())[0]
        txn.commit()

        # Query and update in context manager
        with self.client.txn() as txn:
            query = '''{{
                me(func: uid("{uid}")) {{
                    name
                }}
            }}'''.format(uid=uid)

            resp = txn.query(query)
            self.assertEqual([{"name": "Grace"}], json.loads(resp.json).get("me"))

            # Update the name
            txn.mutate(set_obj={"uid": uid, "name": "Grace Updated"})

        # Verify the update was committed
        resp = self.client.txn(read_only=True).query(query)
        self.assertEqual([{"name": "Grace Updated"}], json.loads(resp.json).get("me"))

    def test_context_manager_invalid_nquad_exception(self):
        """Test that invalid operations cause proper exception handling and discard."""
        with self.assertRaises(Exception):
            with self.client.txn() as txn:
                # This should fail with invalid N-Quad syntax
                txn.mutate(set_nquads="_:node <name> InvalidWithoutQuotes")

        # Transaction should be finished
        self.assertTrue(txn._finished)

    def test_context_manager_read_only_cannot_mutate(self):
        """Test that read-only transactions cannot mutate within context manager."""
        with self.assertRaises(Exception):
            with self.client.txn(read_only=True) as txn:
                txn.mutate(set_obj={"name": "Should Fail"})

    def test_context_manager_no_mutations_auto_commit(self):
        """Test that transactions with no mutations don't error on auto-commit."""
        with self.client.txn() as txn:
            query = "{ me(func: has(name)) { name } }"
            resp = txn.query(query)

        # Should complete without errors even though no mutations were made
        self.assertTrue(txn._finished)


def suite():
    s = unittest.TestSuite()
    s.addTest(TestTxn())
    s.addTest(TestSPStar())
    s.addTest(TestContextManager())
    return s


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
