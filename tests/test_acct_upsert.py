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

import grpc
import functools
import json
import logging
import multiprocessing
import multiprocessing.dummy as mpd
import time
import unittest

from pydgraph.txn import AbortedError

from . import helper


class TestAcountUpsert(helper.ClientIntegrationTestCase):
    """Account upsert integration test."""

    def setUp(self):
        """Drops existing schema and loads new schema for the test."""
        super(TestAcountUpsert, self).setUp()
        self.concurrency = 5

        self.firsts = ['Paul', 'Eric', 'Jack', 'John', 'Martin']
        self.lasts = ['Brown', 'Smith', 'Robinson', 'Waters', 'Taylor']
        self.ages = [20, 25, 30, 35]
        self.accounts = [
            {'first': f, 'last': l, 'age': a}
            for f in self.firsts for l in self.lasts for a in self.ages
        ]
        logging.info(len(self.accounts))

        helper.drop_all(self.client)
        helper.set_schema(self.client, """
            first:  string   @index(term) .
            last:   string   @index(hash) .
            age:    int      @index(int)  .
            when:   int                   .
        """)

    def test_acount_upsert(self):
        """Account upsert integration. Will run upserts concurrently."""
        self.do_upserts(self.accounts, self.concurrency)
        self.assert_changes(self.firsts, self.accounts)

    def do_upserts(self, account_list, concurrency):
        """Will run the upsert command for the accouts in `account_list`. Execution
        happens in concurrent processes."""
        success_ctr = multiprocessing.Value('i', 0, lock=True)
        retry_ctr = multiprocessing.Value('i', 0, lock=True)

        pool = mpd.Pool(concurrency)
        updater = lambda acct: upsert_account(addr=self.TEST_SERVER_ADDR,
                                              account=acct,
                                              success_ctr=success_ctr,
                                              retry_ctr=retry_ctr)
        results = [ pool.apply_async(updater, (acct,))
                    for acct in account_list for _ in range(concurrency)]
        [res.get() for res in results]

    def assert_changes(self, firsts, accounts):
        """Will check to see changes have been made."""
        q = '''
        {{
            all(func: anyofterms(first, "{}")) {{
                first
                last
                age
            }}
        }}'''.format(' '.join(firsts))
        logging.debug(q)
        result = json.loads(self.client.query(q=q).json)
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
    c = helper.create_client(addr)
    q = '''
    {{
        acct(func:eq(first, "{first}")) @filter(eq(last, "{last}") AND eq(age, {age})) {{
            uid
            first
            last
            age
            when
        }}
    }}'''.format(**account)

    last_update_time = time.time() - 10000
    while True:
        if time.time() > last_update_time + 10000:
            logging.debug('Success: %d Retries: %d', success_ctr.value, retry_ctr.value)
            last_update_time = time.time()

        txn = c.txn()
        try:
            result = json.loads(txn.query(q=q).json)
            assert len(result['acct']) <= 1, ('Lookup of account %s found '
                                              'multiple accounts' % account)

            if not result['acct']:
                if account['first'] == 'Paul':
                    print('creating')
                # Account does not exist, so create it
                nquads = '''
                    _:acct <first> "{first}" .
                    _:acct <last> "{last}" .
                    _:acct <age>  "{age}"^^<xs:int> .
                '''.format(**account)
                created = txn.mutate(set_nquads=nquads)
                uid = created.uids.get('acct')
                assert uid is not None and uid != '', 'Account with uid None/""'
            else:
                # Account exists, read the uid
                acct = result['acct'][0]
                uid = acct['uid']
                assert uid is not None, 'Account with uid None'

            updatequads = '''
                <{0}> <when> "{1:d}"^^<xs:int> .
            '''.format(uid, int(time.time()))

            prevresult = json.loads(txn.query(q=q).json)['acct']

            updated = txn.mutate(set_nquads=updatequads)

            txn.commit()

            # result = json.loads(c.query(q=q).json)['acct']
            # if len(result) > 1 and result[0]['first'] == 'Paul':
            #     print('1:', prevresult)
            #     print('2:', updatequads)
            #     print('3:', result)
            #     print()

            with success_ctr.get_lock():
                success_ctr.value += 1
            # txn successful, break the loop
            return
        except (AbortedError, grpc._channel._Rendezvous):
            with retry_ctr.get_lock():
                retry_ctr.value += 1
            # txn failed, retry the loop
        finally:
            txn.discard()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
