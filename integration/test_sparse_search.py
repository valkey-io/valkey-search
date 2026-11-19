"""Exact sparse dot product over externally generated packed posting lists."""

import struct

import pytest
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase


def postings(*rows):
    """Encode ordered (point ID, weight) pairs as little-endian packed records."""
    return b"".join(struct.pack("<Ie", *row) for row in rows)


class TestSparseSearch(ValkeySearchTestCaseBase):
    def test_text_binary_and_key_discovery(self):
        """Both reply formats preserve ranking and expose all keys to Valkey."""
        client = self.server.get_new_client()
        client.set("sparse:{index}:a", postings((1, 1), (8192, 2)))
        client.set("sparse:{index}:b", postings((1, 2), (0xFFFFFFFF, 3)))
        args = (3, 2, "sparse:{index}:a", 2, "sparse:{index}:b", 3)
        expected = [(0xFFFFFFFF, 9.0), (1, 8.0), (8192, 4.0)]
        for command in ("FT.SPARSESEARCH", "FT.SPARSESEARCHBIN"):
            reply = client.execute_command(command, *args)
            if command.endswith("BIN"):
                actual = list(struct.iter_unpack("<Id", reply))
            else:
                actual = [(point, float(score)) for point, score in reply]
            assert actual == expected
            assert client.execute_command("COMMAND GETKEYS", command, *args) == [
                "sparse:{index}:a", "sparse:{index}:b"
            ]

    def test_missing_empty_and_invalid_inputs(self):
        """Empty lists succeed; malformed lists fail without breaking the server."""
        client = self.server.get_new_client()
        assert client.execute_command("FT.SPARSESEARCH", 10, 1, "missing", 1) == []
        client.set("empty", b"")
        assert client.execute_command("FT.SPARSESEARCHBIN", 10, 1, "empty", 1) == b""
        client.hset("wrongtype", "field", "value")
        client.set("truncated", b"\x00")
        client.set("unordered", postings((2, 1), (1, 2)))
        client.set("duplicate", postings((1, 1), (1, 2)))
        client.set("negative", postings((1, -1)))
        client.set("infinity", postings((1, float("inf"))))
        for command in ("FT.SPARSESEARCH", "FT.SPARSESEARCHBIN"):
            for key in ("wrongtype", "truncated", "unordered", "duplicate", "negative", "infinity"):
                with pytest.raises(ResponseError):
                    client.execute_command(command, 10, 1, key, 1)
        assert client.ping()

    @pytest.mark.parametrize("args", [
        (0, 1, "empty", 1), (1, 0), (1, 1, "empty"),
        (1, 1, "empty", -1), (1, 1, "empty", "nan"),
        (1, 1, "empty", "inf"),
    ])
    def test_invalid_query(self, args):
        """Both commands reject invalid arity, bounds, and query weights."""
        client = self.server.get_new_client()
        for command in ("FT.SPARSESEARCH", "FT.SPARSESEARCHBIN"):
            with pytest.raises(ResponseError):
                client.execute_command(command, *args)

    def test_key_acl(self):
        """Command permission does not bypass the caller's key-pattern ACL."""
        client = self.server.get_new_client()
        client.execute_command("ACL", "SETUSER", "sparse-reader", "on", ">password",
                               "+ft.sparsesearch", "+ft.sparsesearchbin", "~allowed:*")
        restricted = self.server.get_new_client()
        restricted.execute_command("AUTH", "sparse-reader", "password")
        for command in ("FT.SPARSESEARCH", "FT.SPARSESEARCHBIN"):
            with pytest.raises(ResponseError, match="NOPERM|permission"):
                restricted.execute_command(command, 1, 1, "forbidden:a", 1)
            assert restricted.execute_command(command, 1, 1, "allowed:a", 1) in ([], b"")

    def test_string_persistence_and_replacement(self):
        """Search reads restored and replaced strings without a separate index."""
        client = self.server.get_new_client()
        client.set("a", postings((1, 1)))
        client.restore("copy", 0, client.dump("a"))
        assert client.execute_command("FT.SPARSESEARCH", 1, 1, "copy", 2) == [[1, b"2"]]
        client.set("copy", postings((2, 3)))
        assert client.execute_command("FT.SPARSESEARCH", 1, 1, "copy", 2) == [[2, b"6"]]
        client.delete("copy")
        assert client.execute_command("FT.SPARSESEARCH", 1, 1, "copy", 2) == []
