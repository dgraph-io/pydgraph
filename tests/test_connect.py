import json
import os
import unittest

import pytest

from pydgraph import open


class TestOpen(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Get connection details from environment or use defaults
        host = os.environ.get("TEST_SERVER_ADDR", "localhost")
        host, port = host.split(":")
        cls.dgraph_host = host
        cls.dgraph_port = port
        cls.username = os.environ.get("DGRAPH_USERNAME", "groot")
        cls.password = os.environ.get("DGRAPH_PASSWORD", "password")

        # Base URL for tests
        cls.base_url = f"dgraph://{cls.dgraph_host}:{cls.dgraph_port}"

    def test_connection_with_auth(self):
        """Test connection with username and password."""
        if not self.username or not self.password:
            self.skipTest("Username and password not configured")

        url = f"dgraph://{self.username}:{self.password}@{self.dgraph_host}:{self.dgraph_port}"
        print("URL", url)
        client = open(url)

        # Verify connection works with credentials
        query = """
        {
            me(func: uid(1)) {
                uid
            }
        }
        """
        response = client.txn(read_only=True).query(query)
        self.assertIsNotNone(response)
        parsed_json = json.loads(response.json)
        self.assertEqual(parsed_json["me"][0]["uid"], "0x1")

        client.close()

    def test_invalid_scheme(self):
        """Test that invalid scheme raises ValueError."""
        invalid_url = f"http://{self.dgraph_host}:{self.dgraph_port}"
        with pytest.raises(ValueError, match="scheme must be 'dgraph'"):
            open(invalid_url)

    def test_missing_hostname(self):
        """Test that missing hostname raises ValueError."""
        with pytest.raises(ValueError, match="hostname required"):
            open(f"dgraph://:{self.dgraph_port}")

    def test_missing_port(self):
        """Test that missing port raises ValueError."""
        with pytest.raises(ValueError, match="port required"):
            open(f"dgraph://{self.dgraph_host}")

    def test_username_without_password(self):
        """Test that username without password raises ValueError."""
        with pytest.raises(
            ValueError, match="password required when username is provided"
        ):
            open(f"dgraph://{self.username}@{self.dgraph_host}:{self.dgraph_port}")

    def test_invalid_sslmode(self):
        """Test that invalid sslmode raises ValueError."""
        with pytest.raises(ValueError, match="Invalid sslmode"):
            open(f"dgraph://{self.dgraph_host}:{self.dgraph_port}?sslmode=invalid")

    def test_unsupported_require_sslmode(self):
        """Test that sslmode=require raises appropriate error."""
        with pytest.raises(ValueError, match="sslmode=require is not supported"):
            open(f"dgraph://{self.dgraph_host}:{self.dgraph_port}?sslmode=require")


if __name__ == "__main__":
    unittest.main()
