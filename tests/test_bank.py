"""
test_bank.py

implements test case for running transfer transactions between bank
accounts. runs concurrent writers running multiple transactions in parallel.
"""
import grpc
import json
import logging
import multiprocessing as mp
import multiprocessing.dummy as mpd
import os
import random
import time
import unittest

from pydgraph import client
import test_integrations as integ

USERS = 100
CONCURRENCY = 10
XFER_COUNT = 1000


class TestBankXfer(integ.DgraphClientIntegrationTestCase):
    """Bank transfer integration test."""
    def setUp(self):
        """Drops existing schema and sets up schema for new test."""
        super(TestBankXfer, self).setUp()
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
            results = [pool.apply_async(run_xfers, (self.TEST_HOSTNAME, self.TEST_PORT,
                                                    XFER_COUNT, self.uids,
                                                    success_ctr, retry_ctr))
                        for _ in range(self.concurrency)]
            [res.get() for res in results]
        finally:
            total_watcher.terminate()
            time.sleep(0.1)

    def create_accounts(self):
        """Creates the default set of accounts."""
        _ = self.client.drop_all()
        _ = self.client.alter(schema="""bal: int .""")

        txn = self.client.txn()
        assigned = txn.mutate_obj(setobj=self.accounts)
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
    resp = c.query(q=q)
    total = json.loads(resp.json)['total']
    logging.debug("Response: %s", total)
    assert total[0]['bal'] == 10000


def run_xfers(hostname, port, xfer_count, account_ids, success_ctr, retry_ctr):
    pname = mpd.current_process().name
    log = logging.getLogger('test_bank.run_txfers[%s]' % (pname,))
    c = client.DgraphClient(hostname, port)

    while True:
        from_acc, to_acc = select_account_pair(account_ids)
        query = """{{
            me(func: uid("{uid1:s}", "{uid2:s}")) {{
                uid,
                bal
            }}
        }}""".format(uid1=from_acc, uid2=to_acc)
        txn = c.txn()
        accounts = load_from_query(txn, query, 'me')
        accounts[0]['bal'] += 5
        accounts[1]['bal'] -= 5
        try:
            dump_from_obj(txn, accounts)
            with success_ctr.get_lock():
                success_ctr.value += 1

            if not success_ctr.value % 100:
                log.debug('Runs %d. Aborts: %d', success_ctr.value, retry_ctr.value)
            if success_ctr.value >= xfer_count:
                break
        except grpc._channel._Rendezvous as e:
            logging.warn(e)
            with retry_ctr.get_lock():
                retry_ctr.value += 1


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
    resp = txn.query(q=query)
    return json.loads(resp.json)[field]


def dump_from_obj(txn, obj, commit=False):
    assigned = txn.mutate_obj(setobj=obj)

    if not commit:
        return assigned
    return txn.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
