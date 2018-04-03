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

__author__ = 'Garvit Pahal <garvit@dgraph.io>'
__maintainer__ = 'Garvit Pahal <garvit@dgraph.io>'

import unittest
import logging
import json
import random
import time
import multiprocessing as mp
import multiprocessing.dummy as mpd

from . import helper

USERS = 100
CONCURRENCY = 10
TRANSFER_COUNT = 1000


class TestBank(helper.ClientIntegrationTestCase):
    def setUp(self):
        super(TestBank, self).setUp()

        self.accounts = [
            {'bal': 100} for _ in range(USERS)
        ]
        self.uids = []

        logging.debug(len(self.accounts))

    def test_bank_transfer(self):
        """Run transfers concurrently."""
        self.create_accounts()

        try:
            total_watcher = self.start_total_watcher()

            success_ctr = mp.Value('i', 0, lock=True)
            retry_ctr = mp.Value('i', 0, lock=True)

            pool = mpd.Pool(CONCURRENCY)
            results = [pool.apply_async(
                run_transfers,
                (self.TEST_SERVER_ADDR, TRANSFER_COUNT, self.uids, success_ctr, retry_ctr)
            ) for _ in range(CONCURRENCY)]

            [res.get() for res in results]
            pool.close()
        finally:
            total_watcher.terminate()
            time.sleep(0.1)

    def create_accounts(self):
        """Creates the default set of accounts."""

        helper.drop_all(self.client)
        helper.set_schema(self.client, 'bal: int .')

        txn = self.client.txn()
        assigned = txn.mutate(set_obj=self.accounts)
        txn.commit()

        self.uids.extend(assigned.uids.values())
        logging.debug('Created %d accounts', len(assigned.uids))

    def start_total_watcher(self):
        """Watcher keeps an eye on the total account balances."""
        total_watch = looper(run_total, self.client, self.uids)
        process = mp.Process(target=total_watch, name='total_watcher')
        process.start()
        return process


def looper(func, *args, **kwargs):
    def _looper():
        while True:
            func(*args, **kwargs)
            time.sleep(1)

    return _looper


def run_total(c, uids):
    """Calculates the total amount in the accounts."""

    q = """{{
        var(func: uid("{uids:s}")) {{
            b as bal
        }}
        total() {{
            bal: sum(val(b))
        }}
    }}""".format(uids='", "'.join(uids))

    resp = c.query(q)
    total = json.loads(resp.json)['total']
    logging.info('Response: %s', total)
    assert total[0]['bal'] == 10000


def run_transfers(addr, transfer_count, account_ids, success_ctr, retry_ctr):
    pname = mpd.current_process().name
    log = logging.getLogger('test_bank.run_transfers[%s]' % (pname,))
    c = helper.create_client(addr)

    while True:
        from_acc, to_acc = select_account_pair(account_ids)
        query = """{{
            me(func: uid("{uid1:s}", "{uid2:s}")) {{
                uid,
                bal
            }}
        }}""".format(uid1=from_acc, uid2=to_acc)

        txn = c.txn()
        try:
            accounts = load_from_query(txn, query, 'me')
            accounts[0]['bal'] += 5
            accounts[1]['bal'] -= 5
            dump_from_obj(txn, accounts)
            with success_ctr.get_lock():
                success_ctr.value += 1

            if not success_ctr.value % 100:
                log.info('Runs %d. Aborts: %d', success_ctr.value, retry_ctr.value)
            if success_ctr.value >= transfer_count:
                break
        except:
            with retry_ctr.get_lock():
                retry_ctr.value += 1
    
    with success_ctr.get_lock(), retry_ctr.get_lock():
        log.info('success: %d, retries: %d', success_ctr.value, retry_ctr.value)


def select_account_pair(accounts):
    """Selects a pair of accounts at random from accounts ensuring they are not
    the same."""
    while True:
        from_acc = random.choice(accounts)
        to_acc = random.choice(accounts)
        if from_acc != to_acc:
            return from_acc, to_acc


def load_from_query(txn, query, field):
    """Loads a field from the results of a query executed in a txn."""
    resp = txn.query(query)
    return json.loads(resp.json)[field]


def dump_from_obj(txn, obj, commit=False):
    assigned = txn.mutate(set_obj=obj)

    if not commit:
        return assigned
    return txn.commit()


def suite():
    s = unittest.TestSuite()
    s.addTest(TestBank())
    return s


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
