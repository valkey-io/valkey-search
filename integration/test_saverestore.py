import base64
from math import exp
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
from typing import Any, List

index = Index("index", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n"), Tag("t")])
vector_only_index = Index("vector_only_index", [Vector("v", 3, type="HNSW", m=2, efc=1)])
non_vector_index = Index("non_vector_index", [Numeric("n"), Tag("t")])

NUM_VECTORS = 10

# Keys that are in all results
full_key_names = [index.keyname(i).encode() for i in range(NUM_VECTORS)]

def check_keys(received_keys, expected_keys):
    received_set = set(received_keys)
    expected_set = set(expected_keys)
    if received_set != expected_set:
        print("Result.keys ", received_set)
        print("expected.keys", expected_set)
        print("Difference(extra received): ", received_set - expected_set)
        print("Difference(extra expected): ", expected_set - received_set)
    assert received_set == expected_set

def do_search(client: Valkey.client, index: Index, query: str, extra: list[str] = []) -> dict[str, dict[str, str]]:
    cmd = ["ft.search", index.name, query, "limit", "0", "100"] + extra
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

KEY_COUNT = len(make_data())   

def load_data(client: Valkey.client):
    records = make_data()
    for i in range(0, len(records)):
        index.write_data(client, i, records[i])
    return len(records)

def verify_data(client: Valkey.client, this_index: Index):
    '''
    Do query operations against each index to ensure that all keys are present
    '''
    if this_index.has_field("n"):
        res = do_search(client, this_index, "@n:[0 100]")
        check_keys(res.keys(), full_key_names + [index.keyname(NUM_VECTORS+0).encode(), index.keyname(NUM_VECTORS+2).encode()])
    if this_index.has_field("t"):
        res = do_search(client, this_index, "@t:{Tag*}")
        check_keys(res.keys(), full_key_names + [index.keyname(NUM_VECTORS+0).encode(), index.keyname(NUM_VECTORS+1).encode()])

def do_save_restore_test(test, index: Index, expected_writes: List[int], expected_reads: List[int]):
    index.create(test.client, True)
    key_count = load_data(test.client)
    verify_data(test.client, index)
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
    assert writes == expected_writes
    '''     
    if write_v2:
        assert writes == [5, key_count, 0]
    else:
        assert writes == [4, 0, 0]
    '''
    test.server.restart(remove_rdb=False)
    time.sleep(5)
    print(test.client.ping())
    verify_data(test.client, index)
    test.client.execute_command("CONFIG SET search.info-developer-visible yes")

    i = test.client.info("search")
    print("Info after load: ", i)
    reads = [
        i["search_rdb_load_sections"],
        i["search_rdb_load_sections_skipped"],
        i["search_rdb_load_keys"],
        i["search_rdb_load_mutation_entries"],
     ]
    assert reads == expected_reads
    '''
    if not write_v2:
        assert reads == [4, 0, 0, 0]
    elif read_v2:
        assert reads == [5, 0, key_count, 0]
    else:
        assert reads == [5, 1, 0, 0]
    
    '''

class TestSaveRestore_v1_v1(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "no"
        return args
    @pytest.mark.parametrize("parameters", [
        [index, [4, 0, 0], [4, 0, 0, 0]],
        [vector_only_index, [2, 0, 0], [2, 0, 0, 0]],
        [non_vector_index, [2, 0, 0], [2, 0, 0, 0]],
        ])
    def test_saverestore_v1_v1(self, parameters):
        do_save_restore_test(self, parameters[0], parameters[1], parameters[2])

class TestSaveRestore_v1_v2(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "yes"
        return args

    @pytest.mark.parametrize("parameters", [
        [index, [4, 0, 0], [4, 0, 0, 0]],
        [vector_only_index, [2, 0, 0], [2, 0, 0, 0]],
        [non_vector_index, [2, 0, 0], [2, 0, 0, 0]],
        ])
    def test_saverestore_v1_v2(self, parameters):
        do_save_restore_test(self, parameters[0], parameters[1], parameters[2])

class TestSaveRestore_v2_v1(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "no"
        return args

    @pytest.mark.parametrize("parameters", [
        [index, [5, KEY_COUNT, 0], [5, 1, 0, 0]],
        [vector_only_index, [3, 0, 0], [3, 1, 0, 0]],
        [non_vector_index, [3, KEY_COUNT, 0], [3, 1, 0, 0]],
        ])
    def test_saverestore_v2_v1(self, parameters):
        do_save_restore_test(self, parameters[0], parameters[1], parameters[2])

class TestSaveRestore_v2_v2(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "yes"
        return args

    @pytest.mark.parametrize("parameters", [
        [index, [5, KEY_COUNT, 0], [5, 0, KEY_COUNT, 0]],
        [vector_only_index, [3, 0, 0], [3, 0, 0, 0]],
        [non_vector_index, [3, KEY_COUNT, 0], [3, 0, KEY_COUNT, 0]],
        ])
    def test_saverestore_v2_v2(self, parameters):
        do_save_restore_test(self, parameters[0], parameters[1], parameters[2])

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

        verify_data(self.client, index)
        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        verify_data(self.client, index)
        self.client.execute_command("CONFIG SET search.info-developer-visible yes")
        i = self.client.info("search")
        print("Info: ", i)
        reads = [
            i["search_rdb_load_mutation_entries"],
        ]
        assert reads == [len(records)]

    def block(self, label):
        x = self.client.execute_command("ft._debug pausepoint test block_mutation_queue")
        self.client.execute_command(*["debug", "log",f"pausepoint block:{x} @ {label}"])
        
    def test_multi_exec_queue(self):
        self.client.execute_command("ft._debug PAUSEPOINT SET block_mutation_queue")
        self.client.execute_command("CONFIG SET search.info-developer-visible yes")
        index.create(self.client, True)
        records = make_data()
        num_records = 2 # len(records)
        #
        # Now, load the data as a multi/exec... But this won't block us.
        #
        self.block("Before multi")
        self.client.execute_command("MULTI")
        for i in range(2):
            index.write_data(self.client, i, records[i])
        self.client.execute_command("EXEC")
        self.block("after exec")



        self.client.execute_command("save")
        self.block("after save")
        self.client.execute_command("ft._debug pausepoint reset block_mutation_queue")


        i = self.client.info("search")
        assert i["search_rdb_save_multi_exec_entries"] == num_records

        # verify_data(self.client, index)
        os.environ["SKIPLOGCLEAN"] = "1"
        self.block("BEFORE RESTART")
        self.server.restart(remove_rdb=False)
        # verify_data(self.client, index)
        self.client.execute_command("CONFIG SET search.info-developer-visible yes")
        i = self.client.info("search")
        print("Info: ", i)
        reads = [
            i["search_rdb_load_multi_exec_entries"],
        ]
        assert reads == [num_records]

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
        verify_data(self.client, index)
        self.client.execute_command("CONFIG SET search.info-developer-visible yes")
        i = self.client.info("search")
        print("Info: ", i)
        reads = [
            i["search_backfill_hash_keys"],
        ]
        assert reads == [len(make_data())]
