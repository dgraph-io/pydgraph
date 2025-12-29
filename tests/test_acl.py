# SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests to verify ACL."""

from __future__ import annotations

import logging
import shutil
import subprocess  # nosec B404
import time
import unittest
from typing import Any

import grpc

from . import helper

__author__ = "Animesh Pathak <animesh@dgrpah.io>"
__maintainer__ = "Istari Digital <contact@istaridigital.com>"


class ACLTestBase(helper.ClientIntegrationTestCase):
    """Base class with shared ACL helper methods."""

    user_id: str | None = None
    group_id: str | None = None
    user_password: str | None = None

    @staticmethod
    def run_acl_command(bash_command: str) -> None:
        """Run dgraph ACL commands in Docker container."""
        docker_command = [
            "docker",
            "compose",
            "-p",
            "pydgraph",
            "exec",
            "-T",
            "alpha1",
        ] + bash_command.split()

        try:
            subprocess.check_output(docker_command, stderr=subprocess.STDOUT)  # nosec B603
        except subprocess.CalledProcessError as e:
            output_msg = ""
            if e.output:
                output_msg = "\nOutput: " + e.output.decode()
            raise RuntimeError(
                "Acl command failed: Unable to execute command "
                + " ".join(docker_command)
                + "\n"
                + str(e)
                + output_msg
            ) from e

    @staticmethod
    def acl_add_user(namespace: int, user_id: str, password: str) -> None:
        """Add a user via dgraph acl command."""
        bash_command = (
            "dgraph acl -a alpha1:9080"
            + " add -u "
            + user_id
            + " -p "
            + password
            + f" --guardian-creds user=groot;password=password;namespace={namespace}"
        )
        ACLTestBase.run_acl_command(bash_command)

    @staticmethod
    def acl_add_group(namespace: int, group_id: str) -> None:
        """Add a group via dgraph acl command."""
        bash_command = (
            "dgraph acl -a alpha1:9080"
            + " add -g "
            + group_id
            + f" --guardian-creds user=groot;password=password;namespace={namespace}"
        )
        ACLTestBase.run_acl_command(bash_command)

    @staticmethod
    def acl_add_user_to_group(namespace: int, user_id: str, group_id: str) -> None:
        """Add a user to a group via dgraph acl command."""
        bash_command = (
            "dgraph acl -a alpha1:9080"
            + " mod -u "
            + user_id
            + " -l "
            + group_id
            + f" --guardian-creds user=groot;password=password;namespace={namespace}"
        )
        ACLTestBase.run_acl_command(bash_command)

    @staticmethod
    def acl_change_permission(namespace: int, group_id: str, permission: int) -> None:
        """Change permission for a group on the 'name' predicate."""
        bash_command = (
            "dgraph acl -a alpha1:9080"
            + " mod -g "
            + group_id
            + " -p name -m "
            + str(permission)
            + f" --guardian-creds user=groot;password=password;namespace={namespace}"
        )
        ACLTestBase.run_acl_command(bash_command)
        # wait for ACL cache to be refreshed.
        time.sleep(2)

    # Instance method wrappers for convenience (use class defaults)
    def add_user(
        self,
        namespace: int = 0,
        user_id: str | None = None,
        password: str | None = None,
    ) -> None:
        if user_id is None:
            user_id = self.user_id
        if password is None:
            password = self.user_password
        self.acl_add_user(namespace, user_id, password)  # type: ignore[arg-type]

    def add_group(self, namespace: int = 0, group_id: str | None = None) -> None:
        if group_id is None:
            group_id = self.group_id
        self.acl_add_group(namespace, group_id)  # type: ignore[arg-type]

    def add_user_to_group(
        self,
        namespace: int = 0,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        if user_id is None:
            user_id = self.user_id
        if group_id is None:
            group_id = self.group_id
        self.acl_add_user_to_group(namespace, user_id, group_id)  # type: ignore[arg-type]

    def change_permission(
        self, permission: int, namespace: int = 0, group_id: str | None = None
    ) -> None:
        if group_id is None:
            group_id = self.group_id
        self.acl_change_permission(namespace, group_id, permission)  # type: ignore[arg-type]


@unittest.skipIf(shutil.which("docker") is None, "Docker not found.")
class TestACL(ACLTestBase):
    user_id = "alice"
    group_id = "dev"
    user_password = "simplepassword"  # nosec B105

    def setUp(self) -> None:
        super(TestACL, self).setUp()
        helper.drop_all(self.client)
        helper.set_schema(self.client, "name: string .")
        self.insert_sample_data()
        self.add_user()
        self.add_group()
        self.add_user_to_group()
        self.alice_client = helper.create_client(self.TEST_SERVER_ADDR)
        time.sleep(2)
        self.alice_client.login(self.user_id, self.user_password)

    def test_read(self) -> None:
        self.change_permission(4)
        self.try_reading(True)
        self.try_writing(False)
        self.try_altering(False)
        self.change_permission(0)

    def test_write(self) -> None:
        self.change_permission(2)
        self.try_reading(False)
        self.try_writing(True)
        self.try_altering(False)
        self.change_permission(0)

    def test_alter(self) -> None:
        self.change_permission(1)
        self.try_reading(False)
        self.try_writing(False)
        self.try_altering(True)
        self.change_permission(0)

    def insert_sample_data(self) -> None:
        txn = self.client.txn()
        txn.mutate(set_nquads='_:animesh <name> "Animesh" .', commit_now=True)

    def try_reading(self, expected: bool) -> None:
        txn = self.alice_client.txn()
        query = """
                {
                    me(func: has(name)) {
                        uid
                        name
                    }
                }
                """

        try:
            txn.query(query)
            if not expected:
                self.fail("Acl test failed: Read successful without permission")
        except Exception as e:
            if expected:
                self.fail(
                    "Acl test failed: Read failed for readable predicate.\n" + str(e)
                )

    def try_writing(self, expected: bool) -> None:
        txn = self.alice_client.txn()

        try:
            txn.mutate(set_nquads='_:aman <name> "Aman" .', commit_now=True)
            if not expected:
                self.fail("Acl test failed: Write successful without permission")
        except Exception as e:
            if expected:
                self.fail(
                    "Acl test failed: Write failed for writable predicate.\n" + str(e)
                )

    def try_altering(self, expected: bool) -> None:
        try:
            helper.set_schema(self.alice_client, "name: string @index(exact, term) .")
            if not expected:
                self.fail("Acl test failed: Alter successful without permission")
        except Exception as e:
            if expected:
                self.fail(
                    "Acl test failed: Alter failed for alterable predicate.\n" + str(e)
                )


@unittest.skipIf(shutil.which("docker") is None, "Docker not found.")
class TestNamespaceACL(ACLTestBase):
    """Tests ACL functionality across multiple namespaces with different users."""

    bob_id: str = "bob"
    bob_password: str = "bobpassword"  # nosec B105
    bob_group: str = "bobgroup"

    alice_id: str = "alice"
    alice_password: str = "alicepassword"  # nosec B105
    alice_group: str = "alicegroup"

    bob_namespace: int | None = None
    alice_namespace: int | None = None
    root_client: Any

    @classmethod
    def setUpClass(cls) -> None:
        """Set up namespaces and users once for all tests in this class."""
        super(TestNamespaceACL, cls).setUpClass()

        # Get server address from environment
        import os

        cls.TEST_SERVER_ADDR = os.environ.get("TEST_SERVER_ADDR", helper.SERVER_ADDR)

        # Create root client as groot
        cls.root_client = helper.create_client(
            cls.TEST_SERVER_ADDR,
            username="groot",
            password="password",  # nosec B106
        )
        helper.drop_all(cls.root_client)

        # Create a new namespace for bob
        cls.bob_namespace = cls.root_client.create_namespace()

        # Set up schema in bob's namespace
        bob_groot_client = helper.create_client(
            cls.TEST_SERVER_ADDR,
            username="groot",
            password="password",  # nosec B106
            namespace=cls.bob_namespace,
        )
        helper.set_schema(bob_groot_client, "name: string .")

        # Insert test data in bob's namespace
        txn = bob_groot_client.txn()
        txn.mutate(set_nquads='_:bob_data <name> "BobData" .', commit_now=True)

        # Create bob user and group in the new namespace
        cls.acl_add_user(cls.bob_namespace, cls.bob_id, cls.bob_password)
        cls.acl_add_group(cls.bob_namespace, cls.bob_group)
        cls.acl_add_user_to_group(cls.bob_namespace, cls.bob_id, cls.bob_group)

        bob_groot_client.close()

        # Create a new namespace for alice
        cls.alice_namespace = cls.root_client.create_namespace()

        # Set up schema in alice's namespace
        alice_groot_client = helper.create_client(
            cls.TEST_SERVER_ADDR,
            username="groot",
            password="password",  # nosec B106
            namespace=cls.alice_namespace,
        )
        helper.set_schema(alice_groot_client, "name: string .")

        # Insert test data in alice's namespace
        txn = alice_groot_client.txn()
        txn.mutate(set_nquads='_:alice_data <name> "AliceData" .', commit_now=True)

        # Create alice user and group in the new namespace
        cls.acl_add_user(cls.alice_namespace, cls.alice_id, cls.alice_password)
        cls.acl_add_group(cls.alice_namespace, cls.alice_group)
        cls.acl_add_user_to_group(cls.alice_namespace, cls.alice_id, cls.alice_group)

        alice_groot_client.close()

        time.sleep(2)

    @classmethod
    def tearDownClass(cls) -> None:
        """Clean up after all tests."""
        if hasattr(cls, "root_client"):
            cls.root_client.close()
        super(TestNamespaceACL, cls).tearDownClass()

    def test_bob_in_own_namespace(self) -> None:
        """Test bob can connect to his own namespace."""
        bob_client = helper.create_client(
            self.TEST_SERVER_ADDR,
            username=self.bob_id,
            password=self.bob_password,
            namespace=self.bob_namespace,
        )

        # Give bob read permission in his namespace
        assert self.bob_namespace is not None
        self.change_permission(4, namespace=self.bob_namespace, group_id=self.bob_group)

        # Bob should be able to read in his namespace
        txn = bob_client.txn()
        query = """
            {
                me(func: has(name)) {
                    uid
                    name
                }
            }
        """
        result = txn.query(query)
        self.assertIn("BobData", result.json.decode())

        bob_client.close()

    def test_bob_cannot_access_root_namespace(self) -> None:
        """Test bob cannot connect to root namespace with his credentials."""

        # Bob's credentials should not work in namespace 0
        with self.assertRaises(grpc.RpcError):
            helper.create_client(
                self.TEST_SERVER_ADDR,
                username=self.bob_id,
                password=self.bob_password,
                namespace=0,
            )

    def test_alice_in_own_namespace(self) -> None:
        """Test alice can connect to her own namespace."""
        alice_client = helper.create_client(
            self.TEST_SERVER_ADDR,
            username=self.alice_id,
            password=self.alice_password,
            namespace=self.alice_namespace,
        )

        # Give alice read permission in her namespace
        assert self.alice_namespace is not None
        self.change_permission(
            4,
            namespace=self.alice_namespace,
            group_id=self.alice_group,
        )

        # Alice should be able to read in her namespace
        txn = alice_client.txn()
        query = """
            {
                me(func: has(name)) {
                    uid
                    name
                }
            }
        """
        result = txn.query(query)
        self.assertIn("AliceData", result.json.decode())

        alice_client.close()

    def test_bob_cannot_access_alice_namespace(self) -> None:
        """Test bob cannot connect to alice's namespace."""

        with self.assertRaises(grpc.RpcError):
            helper.create_client(
                self.TEST_SERVER_ADDR,
                username=self.bob_id,
                password=self.bob_password,
                namespace=self.alice_namespace,
            )

    def test_alice_cannot_access_bob_namespace(self) -> None:
        """Test alice cannot connect to bob's namespace."""

        with self.assertRaises(grpc.RpcError):
            helper.create_client(
                self.TEST_SERVER_ADDR,
                username=self.alice_id,
                password=self.alice_password,
                namespace=self.bob_namespace,
            )

    def test_namespace_data_isolation(self) -> None:
        """Test that bob only sees his data and alice only sees her data."""
        # Give both users read permission
        assert self.bob_namespace is not None
        assert self.alice_namespace is not None
        self.change_permission(4, namespace=self.bob_namespace, group_id=self.bob_group)
        self.change_permission(
            4,
            namespace=self.alice_namespace,
            group_id=self.alice_group,
        )

        # Bob queries his namespace
        bob_client = helper.create_client(
            self.TEST_SERVER_ADDR,
            username=self.bob_id,
            password=self.bob_password,
            namespace=self.bob_namespace,
        )
        txn = bob_client.txn()
        query = "{me(func: has(name)) {name}}"
        result = txn.query(query)
        bob_result = result.json.decode()

        # Bob should see BobData but not AliceData
        self.assertIn("BobData", bob_result)
        self.assertNotIn("AliceData", bob_result)
        bob_client.close()

        # Alice queries her namespace
        alice_client = helper.create_client(
            self.TEST_SERVER_ADDR,
            username=self.alice_id,
            password=self.alice_password,
            namespace=self.alice_namespace,
        )
        txn = alice_client.txn()
        result = txn.query(query)
        alice_result = result.json.decode()

        # Alice should see AliceData but not BobData
        self.assertIn("AliceData", alice_result)
        self.assertNotIn("BobData", alice_result)
        alice_client.close()


def suite() -> unittest.TestSuite:
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestACL())
    suite_obj.addTest(TestNamespaceACL())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
