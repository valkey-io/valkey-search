from typing import List
from indexes import *
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import struct
import pytest
from valkey.exceptions import ResponseError

INDEX_NAME = "myIndex"


class TestCommandsACLs(ValkeySearchTestCaseBase):

    def _verify_user_permissions(
        self, client: Valkey, cmd: List[str], should_access: bool
    ):
        try:
            client.execute_command(*cmd)
        except ResponseError as e:
            if should_access:
                # Make sure the error is not related to permission issues
                assert "has no permissions to run" not in str(e)
        except Exception as e:
            # Any other error is acceptable. This is done to avoid errors
            # Of missing index
            assert True

    @pytest.mark.parametrize(
        "user,should_access_read,should_access_write",
        [
            ("ACL SETUSER user1 on >search_pass -@search", False, False),
            ("ACL SETUSER user1 on >search_pass -@all", False, False),
            ("ACL SETUSER user1 on >search_pass ~* &* +@all", True, True),
            ("ACL SETUSER user1 on >search_pass ~* &* -@all +@search", True, True),
            (
                "ACL SETUSER user1 on >search_pass ~* &* -@all +@write +@read",
                True,
                True,
            ),
            ("ACL SETUSER user1 on >search_pass ~* &* -@all +@write", False, True),
            ("ACL SETUSER user1 on >search_pass ~* &* -@all +@read", True, False),
        ],
    )
    def test_acl_category_permissions(
        self, user, should_access_read, should_access_write
    ):
        search_vector = struct.pack("<3f", *[1.0, 2.0, 3.0])
        # List of search commands
        valkey_search_commands = [
            (
                [
                    "FT.CREATE",
                    INDEX_NAME,
                    "SCHEMA",
                    "vector",
                    "VECTOR",
                    "HNSW",
                    "6",
                    "TYPE",
                    "FLOAT32",
                    "DIM",
                    "3",
                    "DISTANCE_METRIC",
                    "COSINE",
                ],
                should_access_write,
            ),
            (
                [
                    "FT.SEARCH",
                    INDEX_NAME,
                    "*=>[KNN 2 @vector $query_vector]",
                    "PARAMS",
                    "2",
                    "query_vector",
                    search_vector,
                ],
                should_access_read,
            ),
            (["FT.INFO", INDEX_NAME], should_access_read),
            (["FT._LIST"], should_access_read),
            (["FT._DEBUG", "SHOW_INFO"], should_access_read),
            (["FT.DROPINDEX", INDEX_NAME], should_access_write),
        ]
        client: Valkey = self.server.get_new_client()
        # Create the user in test and switch to it
        client.execute_command(user)
        user_name = user.split(" ")[2]
        client.execute_command(f"AUTH {user_name} search_pass")
        # For each command assert if user should or shouldn't access
        for cmd, should_access in valkey_search_commands:
            self._verify_user_permissions(client, cmd, should_access)

    @pytest.mark.parametrize(
        "user,allowed_command",
        [
            ("ACL SETUSER user1 on >search_pass ~* &* -@all +fT.SeArCh", "FT.SEARCH"),
            ("ACL SETUSER user1 on >search_pass ~* &* -@all +Ft.CrEaTe", "FT.CREATE"),
            ("ACL SETUSER user1 on >search_pass ~* &* -@all +fT.InFo", "FT.INFO"),
            ("ACL SETUSER user1 on >search_pass ~* &* -@all +Ft._LiSt", "FT._LIST"),
            ("ACL SETUSER user1 on >search_pass ~* &* -@all +Ft.DrOpInDeX", "FT.DROPINDEX"),
        ],
    )
    def test_acl_specific_search_commands_permissions(
        self, user, allowed_command
    ):
        search_vector = struct.pack("<3f", *[1.0, 2.0, 3.0])
        # List of search commands
        valkey_search_commands = [
            (
                [
                    "Ft.cReAtE",
                    INDEX_NAME,
                    "SCHEMA",
                    "vector",
                    "VECTOR",
                    "HNSW",
                    "6",
                    "TYPE",
                    "FLOAT32",
                    "DIM",
                    "3",
                    "DISTANCE_METRIC",
                    "COSINE",
                ]
            ),
            (
                [
                    "FT.SEARCH",
                    INDEX_NAME,
                    "*=>[KNN 2 @vector $query_vector]",
                    "PARAMS",
                    "2",
                    "query_vector",
                    search_vector,
                ]
            ),
            (["FT.INFO", INDEX_NAME]),
            (["FT._LIST"]),
            (["FT._DEBUG", "SHOW_INFO"]),
            (["FT.DROPINDEX", INDEX_NAME]),
        ]
        client: Valkey = self.server.get_new_client()
        # Create the user in test and switch to it
        client.execute_command(user)
        user_name = user.split(" ")[2]
        client.execute_command(f"AUTH {user_name} search_pass")
        # For each command assert if user should or shouldn't access
        for cmd in valkey_search_commands:
            should_access = allowed_command.lower() == cmd[0].lower()
            self._verify_user_permissions(client, cmd, should_access)

        info_client: Valkey = self.server.get_new_client()
        assert info_client.info("STATS")["acl_access_denied_cmd"] == 5

    def test_index_with_several_prefixes_permissions(self):
        client: Valkey = self.server.get_new_client()
        # Create index with two prefixes
        hnsw_index: Index = Index(
            INDEX_NAME,
            [Vector("vector", 3, type="HNSW", m=2, efc=1), Numeric("n")],
            prefixes=["vec:", "vector:"],
        )
        hnsw_index.create(client)
        # Create a user which can access one of the key prefixes
        client.execute_command("ACL SETUSER user1 on >search_pass ~vector:* &* +@all")
        # Load data in the index
        hnsw_index.load_data(client, 100)
        # Switch to that user
        client.execute_command(f"AUTH user1 search_pass")
        assert client.execute_command(f"ACL whoami") == "user1"
        # Make sure user cannot read one of those keys
        with pytest.raises(ResponseError, match="No permissions to access a key"):
            client.get("vec:1")
        # Make sure user cannot access index as well even though user has access to "vector:" keys
        search_vector = struct.pack("<3f", *[1.0, 2.0, 3.0])
        with pytest.raises(
            ResponseError,
            match="The user does not have permission to access the key prefix",
        ):
            client.execute_command(
                "FT.SEARCH",
                INDEX_NAME,
                "*=>[KNN 2 @vector $query_vector]",
                "PARAMS",
                "2",
                "query_vector",
                search_vector,
            )

    def test_valkey_search_cmds_categories(self):
        client: Valkey = self.server.get_new_client()
        valkey_search_commands = [
            (
                "FT.CREATE",
                [b"write", b"denyoom", b"module", b"fast"],
                [b"@write", b"@fast", b"@search"],
            ),
            (
                "FT.SEARCH",
                [b"readonly", b"module"],
                [b"@read", b"@slow", b"@search"],
            ),
            (
                "FT.AGGREGATE",
                [b"readonly", b"module"],
                [b"@read", b"@slow", b"@search"],
            ),
            (
                "FT.DROPINDEX",
                [b"write", b"module", b"fast"],
                [b"@write", b"@fast", b"@search"],
            ),
            (
                "FT.INFO",
                [b"readonly", b"module", b"fast"],
                [b"@read", b"@fast", b"@search"],
            ),
            (
                "FT._LIST",
                [b"readonly", b"module", b"admin"],
                [b"@read", b"@slow", b"@search", b"@admin"],
            ),
            (
                "FT._DEBUG",
                [b"readonly", b"module", b"admin"],
                [b"@read", b"@slow", b"@search", b"@admin"],
            ),
        ]
        for cmd in valkey_search_commands:
            # Get the info of the commands and compare the acl categories
            cmd_info = client.execute_command(f"COMMAND INFO {cmd[0]}")
            for flag in cmd[1]:
                import logging

                logging.info(cmd_info[0][2])
                assert flag in cmd_info[0][2]
            for category in cmd[2]:
                assert category in cmd_info[0][6]
