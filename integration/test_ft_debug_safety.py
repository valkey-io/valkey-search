import pytest
from indexes import Index, Tag
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters


class TestFTDebugSafety(ValkeySearchTestCaseBase):

    @pytest.mark.parametrize("subcommand", [
        ["PREFIX", "a"],
        ["SUFFIX", "a"],
        ["STEM", "a"],
        ["LEXER", "a"],
    ])
    def test_textinfo_without_text_field_errors(self, subcommand):
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "t", "TAG")
        with pytest.raises(ResponseError, match="no TEXT fields"):
            client.execute_command("FT._DEBUG", "TEXTINFO", "idx", *subcommand)
        assert client.ping()

    def test_show_metadata_without_coordinator_errors(self):
        client: Valkey = self.server.get_new_client()
        with pytest.raises(ResponseError, match="Coordinator is not enabled"):
            client.execute_command("FT._DEBUG", "SHOW_METADATA")
        assert client.ping()

    @pytest.mark.parametrize("args", [["NOSUCH"], ["NOSUCH", "extra"]])
    def test_unknown_subcommand_errors(self, args):
        client: Valkey = self.server.get_new_client()
        with pytest.raises(ResponseError, match="Unknown subcommand: NOSUCH"):
            client.execute_command("FT._DEBUG", *args)
        assert client.ping()

    def test_search_user_cannot_run_debug(self):
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "ACL SETUSER u on >p ~public:* -@all +@search")
        client.execute_command("AUTH u p")
        with pytest.raises(ResponseError, match="(?i)no permissions"):
            client.execute_command("FT._DEBUG", "SHOW_INDEXSCHEMAS")

    @pytest.mark.parametrize("pausepoint", ["unrelated_point", "mutation_processing"])
    def test_multi_write_during_fork_after_pausepoint(self, pausepoint):
        """While a fork child is alive the writer pool is suspended and FT
        commands drain the MULTI queue on the main thread. A pause point that
        was set (and, for an unrelated one, reset) must not abort the server
        there."""
        client: Valkey = self.server.get_new_client()
        Index("idx", [Tag("t")], prefixes=["k:"]).create(
            client, wait_for_backfill=True)
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", pausepoint)
        if pausepoint == "unrelated_point":
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", pausepoint)

        # Keep the fork child alive until BGSAVE CANCEL below, so the writer
        # pool stays suspended: with a set mutation_processing pause point and
        # a resumed pool, FT.SEARCH would wait forever on the parked mutation.
        for i in range(10):
            client.hset(f"pad:{i}", "v", "x")
        client.config_set("rdb-key-save-delay", 1000000000)
        # The server cron resumes the pool after this limit even while the
        # child is alive; use the maximum so it cannot expire mid-test.
        client.config_set("search.max-worker-suspension-secs", 3600)
        client.bgsave()
        waiters.wait_for_true(
            lambda: client.info("persistence")["rdb_bgsave_in_progress"] == 1)

        with client.pipeline(transaction=True) as pipe:
            pipe.hset("k:1", "t", "a")
            pipe.execute()
        assert client.execute_command("FT.SEARCH", "idx", "@t:{a}") == [
            1, b"k:1", [b"t", b"a"]]
        assert client.ping()

        if pausepoint == "mutation_processing":
            client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", pausepoint)
        client.execute_command("BGSAVE", "CANCEL")
        client.config_set("rdb-key-save-delay", 0)
        waiters.wait_for_true(
            lambda: client.info("persistence")["rdb_bgsave_in_progress"] == 0,
            timeout=30)
