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
import grpc
import json
import logging
import multiprocessing as mp
import multiprocessing.dummy as mpd
import os
import random
import time

from pydgraph.client import DgraphClient
from pydgraph.txn import AbortedError

from . import helper

USERS = 100
CONCURRENCY = 10
XFER_COUNT = 1000

class TestBank(helper.ClientIntegrationTestCase):
    """Bank transfer integration test."""

    def setUp(self):
        """Drops existing schema and sets up schema for new test."""
        super(TestBank, self).setUp()
        self.concurrency = CONCURRENCY
        self.accounts = [
            {'bal': 100} for _ in range(USERS)
        ]
        self.uids = []
        logging.debug(len(self.accounts))

    def test_bank_xfer(self):
        """Transfers, will run them concurrently."""
        self.create_accounts()

        try:
            total_watcher = self.start_total_watcher()

            success_ctr = mp.Value('i', 0, lock=True)
            retry_ctr = mp.Value('i', 0, lock=True)
            pool = mpd.Pool(self.concurrency)
            results = [pool.apply_async(run_xfers, (self.TEST_SERVER_ADDR,
                                                    XFER_COUNT, self.uids,
                                                    success_ctr, retry_ctr))
                        for _ in range(self.concurrency)]
            [res.get() for res in results]
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
        """Watcher, will keep an eye on the total account balances."""
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

def run_total(c, account_uids):
    """Calculates the total ammount in the accounts."""
    q = """{{
        var(func: uid("{uids:s}")) {{
            b as bal
        }}
        total() {{
            bal: sum(val(b))
        }}
    }}
    """.format(uids='", "'.join(account_uids))
    resp = c.query(q)
    total = json.loads(resp.json)['total']
    logging.info('Response: %s', total)
    assert total[0]['bal'] == 10000

def run_xfers(addr, xfer_count, account_ids, success_ctr, retry_ctr):
    pname = mpd.current_process().name
    log = logging.getLogger('test_bank.run_txfers[%s]' % (pname,))
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
            if success_ctr.value >= xfer_count:
                break
        except (AbortedError, grpc._channel._Rendezvous):
            with retry_ctr.get_lock():
                retry_ctr.value += 1
    
    with success_ctr.get_lock(), retry_ctr.get_lock():
        log.info("success: %d, retries: %d", success_ctr.value, retry_ctr.value)

def select_account_pair(accounts):
    """Selects a pair of accounts at random from accounts ensuring they are not
    the same."""
    while True:
        from_acc = random.choice(accounts)
        to_acc = random.choice(accounts)
        if not from_acc == to_acc:
            return (from_acc, to_acc)

def load_from_query(txn, query, field):
    """Loads a field from the results of a query executed in a txn."""
    resp = txn.query(query)
    return json.loads(resp.json)[field]

def dump_from_obj(txn, obj, commit=False):
    assigned = txn.mutate(set_obj=obj)

    if not commit:
        return assigned
    return txn.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
