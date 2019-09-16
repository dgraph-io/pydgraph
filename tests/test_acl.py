# Copyright 2019 Dgraph Labs, Inc.
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

"""Tests to verify ACL."""
import subprocess
import time

__author__ = 'Animesh Pathak <animesh@dgrpah.io>'
__maintainer__ = 'Animesh Pathak <animesh@dgrpah.io>'

import logging
import unittest

from . import helper


class TestACL(helper.ClientIntegrationTestCase):
    user_id = 'alice'
    group_id = 'dev'
    user_password = 'simplepassword'
    server_addr = 'localhost:9180'

    def setUp(self):
        super(TestACL, self).setUp()
        helper.drop_all(self.client)
        helper.set_schema(self.client, 'name: string .')
        self.insert_sample_data()
        self.add_user()
        self.add_group()
        self.add_user_to_group()
        self.alice_client = helper.create_client(self.server_addr)
        time.sleep(6)
        self.alice_client.login(self.user_id, self.user_password)

    def test_read(self):
        self.change_permission(4)
        self.try_reading(True)
        self.try_writing(False)
        self.try_altering(False)
        self.change_permission(0)

    def test_write(self):
        self.change_permission(2)
        self.try_reading(False)
        self.try_writing(True)
        self.try_altering(False)
        self.change_permission(0)

    def test_alter(self):
        self.change_permission(1)
        self.try_reading(False)
        self.try_writing(False)
        self.try_altering(True)
        self.change_permission(0)

    def change_permission(self, permission):
        bash_command = "dgraph acl -a " + self.server_addr + " mod -g " + self.group_id + \
                       " -p name -m " + str(permission) + " -x password"
        self.run_command(bash_command)

    def insert_sample_data(self):
        txn = self.client.txn()
        txn.mutate(set_nquads='_:animesh <name> "Animesh" .', commit_now=True)

    def add_user(self):
        bash_command = "dgraph acl -a " + self.server_addr + " add -u " + self.user_id + \
                       " -p " + self.user_password + " -x password"
        self.run_command(bash_command)

    def add_group(self):
        bash_command = "dgraph acl -a " + self.server_addr + " add -g " + self.group_id + " -x password"
        self.run_command(bash_command)

    def add_user_to_group(self):
        bash_command = "dgraph acl -a " + self.server_addr + " mod -u " + \
                       self.user_id + " -l " + self.group_id + " -x password"
        self.run_command(bash_command)

    def run_command(self, bash_command):
        try:
            subprocess.check_output(bash_command.split())
        except subprocess.CalledProcessError as e:
            self.fail("Acl test failed: Unable to execute command " + bash_command + "\n" + str(e))

    def try_reading(self, expected):
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
                self.fail("Acl test failed: Read failed for readable predicate.\n" + str(e))

    def try_writing(self, expected):
        txn = self.alice_client.txn()

        try:
            txn.mutate(set_nquads='_:aman <name> "Aman" .', commit_now=True)
            if not expected:
                self.fail("Acl test failed: Write successful without permission")
        except Exception as e:
            if expected:
                self.fail("Acl test failed: Write failed for writable predicate.\n" + str(e))

    def try_altering(self, expected):
        try:
            helper.set_schema(self.alice_client, 'name: string @index(exact, term) .')
            if not expected:
                self.fail("Acl test failed: Alter successful without permission")
        except Exception as e:
            if expected:
                self.fail("Acl test failed: Alter failed for altreble predicate.\n" + str(e))


def suite():
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(TestACL())
    return suite_obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = unittest.TextTestRunner()
    runner.run(suite())
