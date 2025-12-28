import json
import os
import unittest
from unittest.mock import patch

import pytest

from pydgraph import open


class TestOpen(unittest.TestCase):
    def _setup_jwt_mock(self, mock_stub_class):
        """Helper method to set up JWT mock for open() tests."""

        from pydgraph.proto import api_pb2 as api

        mock_stub = mock_stub_class.return_value
        mock_response = mock_stub.login.return_value

        # Create a proper JWT protobuf and serialize it
        jwt = api.Jwt()
        jwt.access_jwt = "test_jwt"
        jwt.refresh_jwt = "test_refresh"
        mock_response.json = jwt.SerializeToString()
        return mock_stub

    @classmethod
    def setUpClass(cls):
        # Get connection details from environment or use defaults
        server_addr = os.environ.get("TEST_SERVER_ADDR", "localhost:9080")
        if ":" in server_addr:
            host, port = server_addr.split(":", 1)
        else:
            host = server_addr
            port = "9080"
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

    @patch("pydgraph.client_stub.DgraphClientStub")
    def test_open_with_valid_integer_namespace(self, mock_stub_class):
        """Test that open() accepts valid integer namespace with credentials."""

        self._setup_jwt_mock(mock_stub_class)

        # Should not raise an exception with valid integer and credentials
        try:
            open("dgraph://user:pass@localhost:9080?namespace=123")
        except (TypeError, ValueError):
            self.fail("open() raised exception with valid integer namespace")

    def test_open_with_string_namespace_raises_error(self):
        """Test that open() raises TypeError with non-numeric namespace."""

        with pytest.raises(TypeError, match="namespace must be an integer"):
            open("dgraph://localhost:9080?namespace=abc")

    def test_open_with_float_namespace_raises_error(self):
        """Test that open() raises TypeError with float namespace."""

        with pytest.raises(TypeError, match="namespace must be an integer"):
            open("dgraph://localhost:9080?namespace=123.5")

    @patch("pydgraph.client_stub.DgraphClientStub")
    def test_open_with_zero_namespace(self, mock_stub_class):
        """Test that open() accepts zero as valid namespace with credentials."""

        self._setup_jwt_mock(mock_stub_class)

        # Should not raise an exception with zero and credentials
        try:
            open("dgraph://user:pass@localhost:9080?namespace=0")
        except (TypeError, ValueError) as e:
            self.fail(
                f"open() raised exception with zero namespace: {type(e).__name__}: {e}"
            )

    def test_open_with_negative_namespace_raises_error(self):
        """Test that open() raises ValueError with negative namespace."""

        with pytest.raises(ValueError, match="namespace must be >= 0"):
            open("dgraph://localhost:9080?namespace=-1")

    def test_namespace_without_username_raises_error(self):
        """Test that namespace without username/password raises ValueError."""

        with pytest.raises(
            ValueError, match="username/password required when namespace is provided"
        ):
            open("dgraph://localhost:9080?namespace=123")

    @patch("pydgraph.client_stub.DgraphClientStub")
    def test_namespace_with_username_password_succeeds(self, mock_stub_class):
        """Test that namespace with username/password is accepted."""

        self._setup_jwt_mock(mock_stub_class)

        # Should not raise an exception with username/password
        try:
            open("dgraph://user:pass@localhost:9080?namespace=123")
        except ValueError as e:
            if "username/password required" in str(e):
                self.fail(
                    "open() raised ValueError even with username/password provided"
                )
            # Re-raise other ValueErrors (like connection errors)
            raise


if __name__ == "__main__":
    unittest.main()
