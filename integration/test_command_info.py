"""Integration tests for COMMAND INFO and COMMAND DOCS responses.

Verifies that command metadata registered via ValkeyModule_SetCommandInfo
in module_loader.cc is correctly returned by the Valkey server.
"""

from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


# ── Expected results table ──────────────────────────────────────────────
#
# Each entry maps a command name to its expected COMMAND INFO / COMMAND DOCS
# fields.  Commands with has_info=False are expected to have *no* metadata
# registered (no summary, since, complexity, or arguments).
#
# first_key: position of the index-name argument in COMMAND INFO (0 = none).

COMMAND_TABLE = [
    {
        "name": "FT.CREATE",
        "has_info": True,
        "arity": -3,
        "summary": "Create an index",
        "complexity": "O(1)",
        "since": "1.0.0",
        "args": [
            "index", "on", "prefix", "score", "language",
            "skipinitialscan", "minstemsize",
            "offsets", "stopwords-policy",
            "punctuation", "schema", "field",
        ],
    },
    {
        "name": "FT.DROPINDEX",
        "has_info": True,
        "arity": 2,
        "summary": "Delete an index",
        "complexity": "O(1)",
        "since": "1.0.0",
        "args": ["index"],
    },
    {
        "name": "FT.INFO",
        "has_info": True,
        "arity": -2,
        "summary": "Return information about an index",
        "complexity": "O(1)",
        "since": "1.0.0",
        "args": [
            "index",
            "scope", "shards", "consistency",
        ],
    },
    {
        "name": "FT._LIST",
        "has_info": True,
        "arity": 1,
        "summary": "List current index names",
        "complexity": "O(1)",
        "since": "1.0.0",
        "args": [],
    },
    {
        "name": "FT.SEARCH",
        "has_info": True,
        "arity": -3,
        "summary": "Search an index",
        "complexity": "O(N)",
        "since": "1.0.0",
        "args": [
            "index", "query",
            "shards", "consistency",
            "dialect", "inorder", "limit", "nocontent", "params", "return",
            "slop", "sortby", "timeout", "verbatim", "withsortkeys",
        ],
    },
    {
        "name": "FT.AGGREGATE",
        "has_info": True,
        "arity": -3,
        "summary": "Perform aggregate operations on an index",
        "complexity": "O(N)",
        "since": "1.1.0",
        "args": [
            "index", "query", "dialect", "inorder", "load",
            "params", "slop", "timeout", "verbatim", "stage",
        ],
    },
    {
        "name": "FT._DEBUG",
        "has_info": False,
    },
    {
        "name": "FT.INTERNAL_UPDATE",
        "has_info": False,
    },
]


# ── Response parsing helpers ────────────────────────────────────────────

def decode(val):
    """Decode bytes to str if needed."""
    if isinstance(val, bytes):
        return val.decode()
    return val


def parse_kv_list(flat_list):
    """Parse a flat [key, value, key, value, ...] list into a dict."""
    result = {}
    for i in range(0, len(flat_list), 2):
        key = decode(flat_list[i])
        result[key] = flat_list[i + 1]
    return result


def parse_docs_response(response):
    """Parse COMMAND DOCS response into a dict keyed by uppercase command name.

    COMMAND DOCS returns alternating [name, info_list, name, info_list, ...].
    Each info_list is a flat [key, value, key, value, ...] list.
    """
    result = {}
    for i in range(0, len(response), 2):
        cmd_name = decode(response[i]).upper()
        result[cmd_name] = parse_kv_list(response[i + 1])
    return result


def extract_arg_names(args_list):
    """Extract top-level argument names from COMMAND DOCS arguments list."""
    if not args_list:
        return []
    return [decode(parse_kv_list(arg).get("name", "")) for arg in args_list]


# ── Test class ──────────────────────────────────────────────────────────

class TestCommandInfo(ValkeySearchTestCaseBase):
    """Test that COMMAND INFO and COMMAND DOCS return correct metadata."""

    def _raw_command(self, *args):
        """Send a command and return the raw unparsed response.

        Bypasses valkey-py's response callbacks which can mangle
        responses for subcommands like COMMAND DOCS.
        """
        pool = self.client.connection_pool
        conn = pool.get_connection("_")
        try:
            conn.send_command(*args)
            return conn.read_response()
        finally:
            pool.release(conn)

    def test_command_info(self):
        """Verify command metadata for all module commands."""
        all_names = [row["name"] for row in COMMAND_TABLE]

        # Fetch both responses in bulk
        info_response = self._raw_command("COMMAND", "INFO", *all_names)
        docs_response = self._raw_command("COMMAND", "DOCS", *all_names)

        # Index the responses by uppercase command name
        info_by_name = {}
        for entry in info_response:
            info_by_name[decode(entry[0]).upper()] = entry

        docs_by_name = parse_docs_response(docs_response)

        # Drive every check from the table
        for row in COMMAND_TABLE:
            cmd = row["name"]

            if row["has_info"]:
                # -- COMMAND INFO: arity --
                assert cmd in info_by_name, (
                    f"{cmd} not found in COMMAND INFO response"
                )
                entry = info_by_name[cmd]
                actual_arity = entry[1]
                assert actual_arity == row["arity"], (
                    f"{cmd}: expected arity {row['arity']}, "
                    f"got {actual_arity}"
                )

                # -- COMMAND DOCS: summary, since, complexity, args --
                assert cmd in docs_by_name, (
                    f"{cmd} not found in COMMAND DOCS response"
                )
                cmd_docs = docs_by_name[cmd]

                for field in ("summary", "since", "complexity"):
                    actual = decode(cmd_docs.get(field, ""))
                    assert actual == row[field], (
                        f"{cmd}: expected {field} '{row[field]}', "
                        f"got '{actual}'"
                    )

                actual_args = extract_arg_names(cmd_docs.get("arguments", []))
                assert actual_args == row["args"], (
                    f"{cmd}: expected args {row['args']}, "
                    f"got {actual_args}"
                )
            else:
                # Command should have no metadata registered
                if cmd in docs_by_name:
                    summary = decode(docs_by_name[cmd].get("summary", ""))
                    assert summary == "", (
                        f"{cmd} should have no summary, got '{summary}'"
                    )
