# SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Live cross-language end-to-end tests for the transaction-abort reason.

Unlike tests/test_abort_reason.py, which feeds synthetic messages into the parser, these
drive a real (locally patched) Dgraph cluster and prove that each abort category travels
all the way to AbortedError.reason:

- conflict        — two concurrent transactions write the same predicate/uid.
- stale-startts    — a transaction's start ts is invalidated by a Zero leader change; we
                     force that by restarting Zero (TEST_ZERO_CONTAINER) mid-transaction.
- predicate-move   — a predicate's tablet is moved to another group after a transaction
                     mutated it; the post-move commit is rejected. Requires a multi-group
                     cluster and the Zero admin HTTP endpoint (TEST_ZERO_HTTP).

Each test skips cleanly when the infrastructure it needs is not configured, so the file is
safe to include in the default suite. Configure via env vars:

  TEST_SERVER_ADDR   alpha gRPC (default localhost:9180)
  TEST_ZERO_HTTP     zero HTTP admin, e.g. localhost:6180 (enables predicate-move)
  TEST_ZERO_CONTAINER  docker/podman container name of Zero (enables stale-startts restart)
  TEST_ZERO_RESTART_CMD  shell command that restarts Zero (alternative to the container
                         name, e.g. for a manually-launched cluster)
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import unittest
import urllib.request

import pydgraph

SERVER_ADDR = os.getenv("TEST_SERVER_ADDR", "localhost:9180")
ZERO_HTTP = os.getenv("TEST_ZERO_HTTP")
ZERO_CONTAINER = os.getenv("TEST_ZERO_CONTAINER")
ZERO_RESTART_CMD = os.getenv("TEST_ZERO_RESTART_CMD")
DOCKER = os.getenv("TEST_DOCKER_CMD", "docker")


def _restart_zero() -> None:
    if ZERO_RESTART_CMD:
        subprocess.run(ZERO_RESTART_CMD, shell=True, check=True, timeout=60)  # noqa: S602
    else:
        subprocess.run([DOCKER, "restart", ZERO_CONTAINER], check=True, timeout=60)  # noqa: S603


def _client() -> pydgraph.DgraphClient:
    return pydgraph.DgraphClient(pydgraph.DgraphClientStub(SERVER_ADDR))


def _zero_state() -> dict:
    with urllib.request.urlopen(f"http://{ZERO_HTTP}/state", timeout=5) as resp:
        return json.loads(resp.read().decode())


def _find_tablet(pred: str) -> tuple[str, str] | None:
    """Returns (group_id, tablet_key) for the given predicate, or None.

    Tablet keys are namespace-prefixed (e.g. "0-name"), so we match either the bare
    predicate or a "<ns>-<pred>" key.
    """
    for gid, group in _zero_state().get("groups", {}).items():
        for tablet in group.get("tablets") or {}:
            if tablet == pred or tablet.endswith("-" + pred):
                return gid, tablet
    return None


def _group_of(pred: str) -> str | None:
    found = _find_tablet(pred)
    return found[0] if found else None


def _move_tablet(pred: str, group: str) -> None:
    # moveTablet takes the bare predicate (namespace handled server-side), not the
    # namespace-prefixed tablet key that /state reports.
    url = f"http://{ZERO_HTTP}/moveTablet?tablet={pred}&group={group}"
    with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310
        resp.read()


class TestAbortReasonLive(unittest.TestCase):
    def setUp(self) -> None:
        self.client = _client()
        self.client.alter(pydgraph.Operation(drop_all=True))

    def test_conflict_reports_conflict_reason(self) -> None:
        txn = self.client.txn()
        resp = txn.mutate(set_obj={"name": "Manish"})
        uid = next(iter(resp.uids.values()))

        txn2 = self.client.txn()
        txn2.mutate(set_obj={"uid": uid, "name": "Manish"})

        txn.commit()  # winner

        with self.assertRaises(pydgraph.AbortedError) as ctx:
            txn2.commit()
        assert ctx.exception.reason == pydgraph.AbortReason.CONFLICT
        assert "conflict" in str(ctx.exception)

    @unittest.skipUnless(
        ZERO_CONTAINER or ZERO_RESTART_CMD,
        "set TEST_ZERO_CONTAINER or TEST_ZERO_RESTART_CMD to restart Zero",
    )
    def test_stale_startts_reports_stale_reason(self) -> None:
        # Open a transaction so it gets a start ts that the restart will invalidate.
        txn = self.client.txn()
        txn.mutate(set_obj={"name": "Manish"})

        # Restart Zero; on coming back it renews its lease and advances startTxnTs past the
        # start ts above, making this transaction stale. Sleeps give the leader time to
        # re-establish (lease renewal runs on becoming leader).
        _restart_zero()
        time.sleep(8)

        with self.assertRaises(pydgraph.AbortedError) as ctx:
            txn.commit()
        assert ctx.exception.reason == pydgraph.AbortReason.STALE_STARTTS
        assert "stale-startts" in str(ctx.exception)

    @unittest.skipUnless(
        ZERO_HTTP, "set TEST_ZERO_HTTP and run a multi-group cluster for predicate-move"
    )
    def test_predicate_move_reports_predicate_move_reason(self) -> None:
        self.client.alter(pydgraph.Operation(schema="name: string @index(exact) ."))

        # Seed so the "name" tablet exists and settles on some group.
        seed = self.client.txn()
        seed.mutate(set_obj={"name": "seed"})
        seed.commit()
        time.sleep(1)

        found = _find_tablet("name")
        groups = sorted(_zero_state().get("groups", {}).keys())
        if found is None or len(groups) < 2:
            self.skipTest("need a multi-group cluster serving predicate 'name'")
        src, _tablet_key = found
        dst = next(g for g in groups if g != src)

        # Mutate "name" while it is on `src` (the txn's Preds will reference `src`), but
        # do not commit yet.
        txn = self.client.txn()
        txn.mutate(set_obj={"name": "Manish"})

        # Move the tablet to another group and wait for the move to complete.
        _move_tablet("name", dst)
        deadline = time.time() + 60
        while time.time() < deadline and _group_of("name") != dst:
            time.sleep(1)
        assert _group_of("name") == dst, "tablet move did not complete"

        # Committing now: the txn mutated on `src` but the tablet is on `dst`, so Zero's
        # checkPreds rejects the commit with the predicate-move category.
        with self.assertRaises(pydgraph.AbortedError) as ctx:
            txn.commit()
        assert ctx.exception.reason == pydgraph.AbortReason.PREDICATE_MOVE
        assert "predicate-move" in str(ctx.exception)


if __name__ == "__main__":
    unittest.main()
