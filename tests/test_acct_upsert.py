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

"""Tests to verify upsert directive."""

__author__ = 'Shailesh Kochhar <shailesh.kochhar@gmail.com>'
__maintainer__ = 'Martin Martinez Rivera <martinmr@dgraph.io>'

import unittest
import logging
import json
import time
import multiprocessing
import multiprocessing.dummy as mpd

import pydgraph

from . import helper

CONCURRENCY = 5
FIRSTS = ['Paul', 'Eric', 'Jack', 'John', 'Martin']
LASTS = ['Brown', 'Smith', 'Robinson', 'Waters', 'Taylor']
AGES = [20, 25, 30, 35]


class TestAccountUpsert(helper.ClientIntegrationTestCase):
    """Tests to verify upsert directive."""
    def setUp(self):
        super(TestAccountUpsert, self).setUp()

        self.accounts = [
            {'first': f, 'last': l, 'age': a}
            for f in FIRSTS for l in LASTS for a in AGES
        ]
        logging.info(len(self.accounts))

        helper.drop_all(self.client)
        helper.set_schema(self.client, """
            first:  string   @index(term) @upsert .
            last:   string   @index(hash) @upsert .
            age:    int      @index(int)  @upsert .
            when:   int                   .
        """)

    def test_account_upsert(self):
        """Run upserts concurrently."""
        self.do_upserts(self.accounts, CONCURRENCY, upsert_account)
        self.assert_changes(FIRSTS, self.accounts)

    def test_account_upsert_block(self):
        """Run upserts concurrently using upsert block."""
        self.do_upserts(self.accounts, CONCURRENCY, upsert_account_upsert_block)
        self.assert_changes(FIRSTS, self.accounts)

    def do_upserts(self, account_list, concurrency, upsert_func):
        """Runs the upsert command for the accounts in `account_list`. Execution
        happens in concurrent processes."""

        success_ctr = multiprocessing.Value('i', 0, lock=True)
        retry_ctr = multiprocessing.Value('i', 0, lock=True)

        def _updater(acct):
            upsert_func(addr=self.TEST_SERVER_ADDR, account=acct,
                        success_ctr=success_ctr, retry_ctr=retry_ctr)

        pool = mpd.Pool(concurrency)
        results = [
            pool.apply_async(_updater, (acct,))
            for acct in account_list for _ in range(concurrency)
        ]

        _ = [res.get() for res in results]
        pool.close()

    def assert_changes(self, firsts, accounts):
        """Will check to see changes have been made."""

        query = """{{
            all(func: anyofterms(first, "{}")) {{
                first
                last
                age
            }}
        }}""".format(' '.join(firsts))
        logging.debug(query)
        result = json.loads(self.client.txn(read_only=True).query(query).json)

        account_set = set()
        for acct in result['all']:
            self.assertTrue(acct['first'] is not None)
            self.assertTrue(acct['last'] is not None)
            self.assertTrue(acct['age'] is not None)
            account_set.add('{first}_{last}_{age}'.format(**acct))

        self.assertEqual(len(account_set), len(accounts))
        for acct in accounts:
            self.assertTrue('{first}_{last}_{age}'.format(**acct) in account_set)


def upsert_account(addr, account, success_ctr, retry_ctr):
    """Runs upsert operation."""
    client = helper.create_client(addr)
    client.login("groot", "password")
    query = """{{
        acct(func:eq(first, "{first}")) @filter(eq(last, "{last}") AND eq(age, {age})) {{
            uid
        }}
    }}""".format(**account)

    last_update_time = time.time() - 10000
    while True:
        if time.time() > last_update_time + 10000:
            logging.debug('Success: %d Retries: %d', success_ctr.value,
                          retry_ctr.value)
            last_update_time = time.time()

        txn = client.txn()
        try:
            result = json.loads(txn.query(query).json)
            assert len(result['acct']) <= 1, ('Lookup of account %s found '
                                              'multiple accounts' % account)

            if not result['acct']:
                # account does not exist, so create it
                nquads = """
                    _:acct <first> "{first}" .
                    _:acct <last> "{last}" .
                    _:acct <age>  "{age}"^^<xs:int> .
                """.format(**account)
                created = txn.mutate(set_nquads=nquads)
                uid = created.uids.get('acct')
                assert uid is not None and uid != '', 'Account with uid None'
            else:
                # account exists, read the uid
                acct = result['acct'][0]
                uid = acct['uid']
                assert uid is not None, 'Account with uid None'

            updatequads = '<{0}> <when> "{1:d}"^^<xs:int> .'.format(
                uid, int(time.time()))
            txn.mutate(set_nquads=updatequads)
            txn.commit()

            with success_ctr.get_lock():
                success_ctr.value += 1

            # txn successful, break the loop
            return
        except pydgraph.AbortedError:
            with retry_ctr.get_lock():
                retry_ctr.value += 1
            # txn failed, retry the loop
        finally:
            txn.discard()


def upsert_account_upsert_block(addr, account, success_ctr, retry_ctr):
    """Runs upsert operation."""
    client = helper.create_client(addr)
    client.login("groot", "password")
    query = """{{
        acct(func:eq(first, "{first}")) @filter(eq(last, "{last}") AND eq(age, {age})) {{
            u as uid
        }}
    }}""".format(**account)

    last_update_time = time.time() - 10000
    while True:
        if time.time() > last_update_time + 10000:
            logging.debug('Success: %d Retries: %d', success_ctr.value,
                          retry_ctr.value)
            last_update_time = time.time()

        txn = client.txn()
        try:
            nquads = """
                uid(u) <first> "{first}" .
                uid(u) <last> "{last}" .
                uid(u) <age>  "{age}"^^<xs:int> .
            """.format(**account)
            mutation = txn.create_mutation(set_nquads=nquads)
            request = txn.create_request(query=query, mutations=[mutation], commit_now=True)
            txn.do_request(request)

            updatequads = 'uid(u) <when> "{0:d}"^^<xs:int> .'.format(int(time.time()))
            txn = client.txn()
            mutation = txn.create_mutation(set_nquads=updatequads)
            request = txn.create_request(query=query, mutations=[mutation], commit_now=True)
            txn.do_request(request)

            with success_ctr.get_lock():
                success_ctr.value += 1

            # txn successful, break the loop
            return
        except pydgraph.AbortedError:
            with retry_ctr.get_lock():
                retry_ctr.value += 1
            # txn failed, retry the loop
        finally:
            txn.discard()


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestAccountUpsert())
    return suite_obj


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
