# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0

__author__ = "Hypermode Inc."
__maintainer__ = "Hypermode Inc. <hello@hypermode.com>"

import json
import unittest

from . import helper


class TestAlter(helper.ClientIntegrationTestCase):
    """Tests convenience methods for altering the database."""

    def test_drop_all(self):
        """Tests drop_all() method."""
        helper.drop_all(self.client)

        # First, set a schema and add some data
        schema = """
            name: string @index(exact) .
            email: string @index(exact) .
            age: int .
        """
        self.client.set_schema(schema)

        # Add some data
        txn = self.client.txn()
        _ = txn.mutate(
            set_nquads="""
            <_:alice> <name> "Alice" .
            <_:alice> <email> "alice@example.com" .
            <_:alice> <age> "29" .
        """
        )
        txn.commit()

        # Verify data exists
        query = '{alice(func: eq(name, "Alice")) { name email age }}'
        resp = self.client.run_dql(query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("alice", [])), 1)
        self.assertEqual(result["alice"][0]["name"], "Alice")

        # Drop all
        self.client.drop_all()
        import grpc

        with self.assertRaises(grpc.RpcError) as cm:
            self.client.run_dql(query, read_only=True)
        self.assertIn("Schema not defined for predicate", str(cm.exception))

        schema_query = "schema(pred: [name, email, age]) {type}"
        resp = self.client.run_dql(schema_query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("schema", [])), 0)

    def test_drop_data(self):
        """Tests drop_data() method preserves schema but removes data."""
        helper.drop_all(self.client)

        # Set schema
        schema = """
            name: string @index(exact) .
            email: string @index(exact) .
            age: int .
        """
        self.client.set_schema(schema)

        # Add some data
        txn = self.client.txn()
        _ = txn.mutate(
            set_nquads="""
            <_:alice> <name> "Alice" .
            <_:alice> <email> "alice@example.com" .
            <_:alice> <age> "29" .
        """
        )
        txn.commit()

        # Verify data exists
        query = '{alice(func: eq(name, "Alice")) { name email age }}'
        resp = self.client.run_dql(query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("alice", [])), 1)

        # Drop data (but keep schema)
        self.client.drop_data()

        # Verify data is gone
        resp = self.client.run_dql(query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("alice", [])), 0)

        # Verify schema is still there
        schema_query = "schema(pred: [name, email, age]) {type}"
        resp = self.client.run_dql(schema_query, read_only=True)
        result = json.loads(resp.json)
        self.assertGreater(len(result.get("schema", [])), 0)

        # Verify we can still add data with the same schema
        txn = self.client.txn()
        _ = txn.mutate(
            set_nquads="""
            <_:bob> <name> "Bob" .
            <_:bob> <email> "bob@example.com" .
            <_:bob> <age> "30" .
        """
        )
        txn.commit()

        query = '{bob(func: eq(name, "Bob")) { name email age }}'
        resp = self.client.run_dql(query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("bob", [])), 1)
        self.assertEqual(result["bob"][0]["name"], "Bob")

    def test_drop_predicate(self):
        """Tests drop_predicate() method."""
        helper.drop_all(self.client)

        # Set schema with multiple predicates
        schema = """
            name: string @index(exact) .
            email: string @index(exact) .
            age: int .
        """
        self.client.set_schema(schema)

        # Add data
        txn = self.client.txn()
        _ = txn.mutate(
            set_nquads="""
            <_:alice> <name> "Alice" .
            <_:alice> <email> "alice@example.com" .
            <_:alice> <age> "29" .
        """
        )
        txn.commit()

        # Verify all predicates exist
        query = '{alice(func: eq(name, "Alice")) { name email age }}'
        resp = self.client.run_dql(query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("alice", [])), 1)
        self.assertIn("name", result["alice"][0])
        self.assertIn("email", result["alice"][0])
        self.assertIn("age", result["alice"][0])

        # Drop the age predicate
        self.client.drop_predicate("age")

        # Verify age is gone but name and email remain
        resp = self.client.run_dql(query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("alice", [])), 1)
        self.assertIn("name", result["alice"][0])
        self.assertIn("email", result["alice"][0])
        self.assertNotIn("age", result["alice"][0])

        # Verify schema no longer has age
        schema_query = "schema(pred: [name, email, age]) {type}"
        resp = self.client.run_dql(schema_query, read_only=True)
        result = json.loads(resp.json)
        schema_preds = [p["predicate"] for p in result.get("schema", [])]
        self.assertIn("name", schema_preds)
        self.assertIn("email", schema_preds)
        self.assertNotIn("age", schema_preds)

    def test_drop_predicate_empty_string(self):
        """Tests that drop_predicate() raises error for empty predicate."""
        with self.assertRaises(ValueError) as context:
            self.client.drop_predicate("")
        self.assertIn("predicate cannot be empty", str(context.exception))

    def test_drop_type(self):
        """Tests drop_type() method."""
        helper.drop_all(self.client)

        # Set schema with a type
        schema = """
            name: string @index(exact) .
            email: string @index(exact) .
            age: int .

            type Person {
                name: string
                email: string
                age: int
            }
        """
        self.client.set_schema(schema)

        # Verify type exists in schema
        type_query = "schema(type: Person) {type}"
        resp = self.client.run_dql(type_query, read_only=True)
        result = json.loads(resp.json)
        self.assertGreater(len(result.get("types", [])), 0)

        # Drop the type
        self.client.drop_type("Person")

        # Verify type is gone from schema
        resp = self.client.run_dql(type_query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("types", [])), 0)

        # Verify predicates still exist (drop_type doesn't drop predicates)
        schema_query = "schema(pred: [name, email, age]) {type}"
        resp = self.client.run_dql(schema_query, read_only=True)
        result = json.loads(resp.json)
        self.assertGreater(len(result.get("schema", [])), 0)

    def test_drop_type_empty_string(self):
        """Tests that drop_type() raises error for empty type_name."""
        with self.assertRaises(ValueError) as context:
            self.client.drop_type("")
        self.assertIn("type_name cannot be empty", str(context.exception))

    def test_set_schema(self):
        """Tests set_schema() method."""
        helper.drop_all(self.client)

        # Set initial schema
        schema1 = """
            name: string @index(exact) .
            email: string @index(exact) .
        """
        self.client.set_schema(schema1)

        # Verify schema
        schema_query = "schema(pred: [name, email]) {type}"
        resp = self.client.run_dql(schema_query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("schema", [])), 2)

        # Update schema with additional predicate
        schema2 = """
            name: string @index(exact) .
            email: string @index(exact) .
            age: int .
        """
        self.client.set_schema(schema2)

        # Verify updated schema
        schema_query = "schema(pred: [name, email, age]) {type}"
        resp = self.client.run_dql(schema_query, read_only=True)
        result = json.loads(resp.json)
        self.assertEqual(len(result.get("schema", [])), 3)

    def test_set_schema_empty_string(self):
        """Tests that set_schema() raises error for empty schema."""
        with self.assertRaises(ValueError) as context:
            self.client.set_schema("")
        self.assertIn("schema cannot be empty", str(context.exception))


def suite():
    """Returns a test suite object."""
    suite_obj = unittest.TestSuite()
    suite_obj.addTest(unittest.makeSuite(TestAlter))
    return suite_obj


if __name__ == "__main__":
    unittest.main()
