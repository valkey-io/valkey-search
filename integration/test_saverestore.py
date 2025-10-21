import base64
import os
import tempfile
import time
import subprocess
import shutil
import socket
from valkey import ResponseError, Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode, ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
import pytest
import logging
from util import waiters
import threading
from ft_info_parser import FTInfoParser

index = Index("index", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n"), Tag("t")])
NUM_VECTORS = 10

# Keys that are in all results
full_key_names = [index.keyname(i).encode() for i in range(NUM_VECTORS)]

def check_keys(received_keys, expected_keys):
    received_set = set(received_keys)
    expected_set = set(expected_keys)
    print("Result.keys ", received_set)
    print("expected.keys", expected_set)
    assert received_set == expected_set

def do_search(client: Valkey.client, query: str, extra: list[str] = []) -> dict[str, dict[str, str]]:
    cmd = ["ft.search index", query, "limit", "0", "100"] + extra
    print("Cmd: ", cmd)
    res = client.execute_command(*cmd)[1:]
    result = dict()
    for i in range(0, len(res), 2):
        row = res[i+1]
        row_dict = dict()
        for j in range(0, len(row), 2):
            row_dict[row[j]] = row[j+1]
        result[res[i]] = row_dict
    print("Result is ", result)
    return result

def make_data():
    records = []
    for i in range(0, NUM_VECTORS):
        records += [index.make_data(i)]

    data = index.make_data(len(records))
    data["v"] = "0"
    records += [data]

    data = index.make_data(len(records))
    data["n"] = "fred"
    records += [data]

    data = index.make_data(len(records))
    data["t"] = ""
    records += [data]
    return records

def load_data(client: Valkey.client):
    records = make_data()
    for i in range(0, len(records)):
        index.write_data(client, i, records[i])
    return len(records)

def verify_data(client: Valkey.client):
    '''
    Do query operations against each index to ensure that all keys are present
    '''

    res = do_search(client, "@n:[0 100]")
    check_keys(res.keys(), full_key_names + [index.keyname(NUM_VECTORS+0).encode(), index.keyname(NUM_VECTORS+2).encode()])
    res = do_search(client, "@t:{Tag*}")
    check_keys(res.keys(), full_key_names + [index.keyname(NUM_VECTORS+0).encode(), index.keyname(NUM_VECTORS+1).encode()])

def do_save_restore_test(test, write_v2: bool, read_v2: bool):
    index.create(test.client, True)
    key_count = load_data(test.client)
    verify_data(test.client)
    test.client.config_set("search.rdb-validate-on-write", "yes")
    test.client.execute_command("save")
    os.environ["SKIPLOGCLEAN"] = "1"
    test.client.execute_command("CONFIG SET search.info-developer-visible yes")
    i = test.client.info("search")
    print("Info after save: ", i)
    writes = [
        i["search_rdb_save_sections"],
        i["search_rdb_save_keys"],
        i["search_rdb_save_mutation_entries"],
     ]
    if write_v2:
        assert writes == [5, key_count, 0]
    else:
        assert writes == [4, 0, 0]
    test.server.restart(remove_rdb=False)
    time.sleep(5)
    print(test.client.ping())
    verify_data(test.client)
    test.client.execute_command("CONFIG SET search.info-developer-visible yes")

    i = test.client.info("search")
    print("Info after load: ", i)
    reads = [
        i["search_rdb_load_sections"],
        i["search_rdb_load_sections_skipped"],
        i["search_rdb_load_keys"],
        i["search_rdb_load_mutation_entries"],
     ]
    if not write_v2:
        assert reads == [4, 0, 0, 0]
    elif read_v2:
        assert reads == [5, 0, key_count, 0]
    else:
        assert reads == [5, 1, 0, 0]
    

class TestSaveRestore_v1_v1(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "no"
        return args

    def test_saverestore_v1_v1(self):
        do_save_restore_test(self, False, False)

class TestSaveRestore_v1_v2(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "yes"
        return args

    def test_saverestore_v1_v2(self):
        do_save_restore_test(self, False, True)

class TestSaveRestore_v2_v1(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "no"
        return args

    def test_saverestore_v2_v1(self):
        do_save_restore_test(self, True, False)

class TestSaveRestore_v2_v2(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "yes"
        return args

    def test_saverestore_v2_v2(self):
        do_save_restore_test(self, True, True)

class TestMutationQueue(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "yes"
        return args
    
    def mutation_queue_size(self):
        info = FTInfoParser(self.client.execute_command("ft.info ", index.name))
        return info.mutation_queue_size

    def test_mutation_queue(self):
        self.client.execute_command("ft._debug PAUSEPOINT SET block_mutation_queue")
        index.create(self.client, True)
        records = make_data()
        #
        # Now, load the data.... But since the mutation queue is blocked it will be stopped....
        #
        client_threads = []
        for i in range(len(records)):
            new_client = self.server.get_new_client()
            t = threading.Thread(target = index.write_data, args=(new_client, i, records[i]) )
            t.start()
            client_threads += [t]
        
        #
        # Now, wait for the mutation queue to get fully loaded
        #
        waiters.wait_for_true(lambda: self.mutation_queue_size() == len(records))
        print("MUTATION QUEUE LOADED")

        self.client.execute_command("save")

        self.client.execute_command("ft._debug pausepoint reset block_mutation_queue")

        for t in client_threads:
            t.join()

        verify_data(self.client)
        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        verify_data(self.client)
        self.client.execute_command("CONFIG SET search.info-developer-visible yes")
        i = self.client.info("search")
        print("Info: ", i)
        reads = [
            i["search_rdb_load_mutation_entries"],
        ]
        assert reads == [len(records)]

    def test_saverestore_backfill(self):
        #
        # Delay the backfill and ensure that with new format we will trigger the backfill....
        #
        self.client.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET StopBackfill yes")
        load_data(self.client)
        index.create(self.client, False)
        self.client.execute_command("save")

        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        verify_data(self.client)
        self.client.execute_command("CONFIG SET search.info-developer-visible yes")
        i = self.client.info("search")
        print("Info: ", i)
        reads = [
            i["search_backfill_hash_keys"],
        ]
        assert reads == [len(make_data())]
