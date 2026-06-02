import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
from ft_info_parser import FTInfoParser
import threading
from util import waiters

class TestPostFilter(ValkeySearchTestCaseDebugMode):

    def get_post_filter_count(self):
        return int(self.client.info("SEARCH")["search_predicate_revalidation"])
    
    def queue_modification(self, index, f):
        self.client.execute_command("ft._debug PAUSEPOINT SET block_mutation_queue")
        self.thread = threading.Thread(target=f)
        self.thread.start()
        waiters.wait_for_true(lambda: index.info(self.client).mutation_queue_size > 0)
        assert int(self.client.execute_command("ft._debug PAUSEPOINT test block_mutation_queue")) > 0

    def release_modification(self, index):
        self.client.execute_command("ft._debug PAUSEPOINT RESET block_mutation_queue")
        waiters.wait_for_true(lambda: index.info(self.client).mutation_queue_size == 0)

    def test_postfilter_hash(self):
        self.client.config_set("search.info-developer-visible", "yes")
        # self.client.execute_command("ft._debug PAUSEPOINT SET block_mutation_queue")
        index = Index("index", [Numeric("n"), Tag("t")])

        index.create(self.client, True)
        index.load_data(self.client, 10)

        # Ensure that nothing is filtered when no mutations are active

        r = index.query(self.client, "@n:[0 1]")
        assert self.get_post_filter_count() == 0
        print("Query1:", r)
        assert r[index.keyname(0).encode()][b"n"] == b'0'
        assert r[index.keyname(1).encode()][b"n"] == b'1'
        assert self.get_post_filter_count() == 0

        # Case 1, overwrite with another in-range value
        self.queue_modification(index, lambda: self.server.get_new_client().hset(index.keyname(0), "n", "1"))
        r = index.query(self.client, "@n:[0 1]")
        print("Query2:", r)
        assert r[index.keyname(0).encode()][b"n"] == b'1'
        assert r[index.keyname(1).encode()][b"n"] == b'1'
        assert self.get_post_filter_count() == 1
        self.release_modification(index)

        # Case 2, overwrite with out-of-range value
        self.queue_modification(index, lambda: self.server.get_new_client().hset(index.keyname(0), "n", "100"))
        r = index.query(self.client, "@n:[0 1]")
        print("Query3:", r)
        assert len(r) == 1
        assert r[index.keyname(1).encode()][b"n"] == b'1'
        assert self.get_post_filter_count() == 2
        self.release_modification(index)

        # Case 3 overwrite with invalid value
        self.queue_modification(index, lambda: self.server.get_new_client().hset(index.keyname(0), "n", "badvalue"))
        r = index.query(self.client, "@n:[0 1]")
        print("Query3:", r)
        assert len(r) == 1
        assert r[index.keyname(1).encode()][b"n"] == b'1'
        assert self.get_post_filter_count() == 2
        self.release_modification(index)

        # Case 4 delete good value
        self.client.hset(index.keyname(0), "n", "0") # Back to a good value
        self.queue_modification(index, lambda: self.server.get_new_client().delete(index.keyname(0)))
        r = index.query(self.client, "@n:[0 1]")
        print("Query3:", r)
        assert len(r) == 1
        assert r[index.keyname(1).encode()][b"n"] == b'1'
        assert self.get_post_filter_count() == 2 # Delete isn't filtered.
        self.release_modification(index)

    def test_postfilter_json(self):
        self.client.config_set("search.info-developer-visible", "yes")
        # self.client.execute_command("ft._debug PAUSEPOINT SET block_mutation_queue")
        index = Index("index", [Numeric("n"), Tag("t")], [], type = KeyDataType.JSON)

        index.create(self.client, True)
        index.load_data(self.client, 10)

        # Ensure that nothing is filtered when no mutations are active

        r = index.query(self.client, "@n:[0 1]")
        assert self.get_post_filter_count() == 0
        print("Query1:", r)
        assert r[index.keyname(0).encode()]["n"] == 0
        assert r[index.keyname(1).encode()]["n"] == 1
        assert self.get_post_filter_count() == 0

        # Case 1, overwrite with another in-range value
        self.queue_modification(index, lambda: self.server.get_new_client().execute_command("json.set", index.keyname(0), "$.n", "1"))
        r = index.query(self.client, "@n:[0 1]")
        print("Query2:", r)
        assert r[index.keyname(0).encode()]["n"] == 1
        assert r[index.keyname(1).encode()]["n"] == 1
        assert self.get_post_filter_count() == 1
        self.release_modification(index)

        # Case 2, overwrite with out-of-range value
        self.queue_modification(index, lambda: self.server.get_new_client().execute_command("json.set", index.keyname(0), "$.n", "100"))
        r = index.query(self.client, "@n:[0 1]")
        print("Query3:", r)
        assert len(r) == 1
        assert r[index.keyname(1).encode()]["n"] == 1
        assert self.get_post_filter_count() == 2
        self.release_modification(index)

        # Case 2 set missing value
        self.client.execute_command("json.set", index.keyname(0), "$.n", "0")
        self.queue_modification(index, lambda: self.server.get_new_client().execute_command("json.del", index.keyname(0), "$.n"))
        r = index.query(self.client, "@n:[0 1]")
        print("Query3:", r)
        assert len(r) == 1
        assert r[index.keyname(1).encode()]["n"] == 1
        assert self.get_post_filter_count() == 3
        self.release_modification(index)

        # Case 4 delete good value
        self.client.execute_command("json.set", index.keyname(0), "$.n", "0")
        self.queue_modification(index, lambda: self.server.get_new_client().execute_command("json.del", index.keyname(0)))
        r = index.query(self.client, "@n:[0 1]")
        print("Query3:", r)
        assert len(r) == 1
        assert r[index.keyname(1).encode()]["n"] == 1
        assert self.get_post_filter_count() == 3 # Delete isn't filtered.
        self.release_modification(index)
