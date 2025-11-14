from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from valkeytestframework.util import waiters
from indexes import *
from valkeytestframework.conftest import resource_port_tracker
import logging
from pprint import pprint
from ft_info_parser import FTInfoParser
from utils import IndexingTestHelper

def GetStringPollStatus(client):
    do_row = lambda row: {row[i+0].decode():row[i+1] for i in range(0, len(row), 2)}
    stats = client.execute_command("FT._DEBUG StringPoolStats")
    inline_total = do_row(stats[0])
    outofline_total = do_row(stats[1])
    byrefs = stats[2]
    byref = {byrefs[i][0]:do_row(byrefs[i][1]) for i in range(len(byrefs))}
    bysizes = stats[3]
    bysize = {bysizes[i][0]:do_row(bysizes[i][1]) for i in range(len(bysizes))}

    return inline_total, outofline_total, byref, bysize    
    

class TestFtDebugCommand(ValkeySearchTestCaseDebugMode):

    def test_StringPoolStats(self):
        """
        Test CMD timeout logic
        """
        client: Valkey = self.server.get_new_client()
        # po
        assert (
            client.execute_command(
                "CONFIG SET search.info-developer-visible yes"
            )
            == b"OK"
        )
        hist = client.execute_command("FT._DEBUG StringPoolStats")
        print("Hist: ", hist)
        print(GetStringPollStatus(client))
        assert hist[0] == [
            b'Count', 0, 
            b'Bytes', 0,
            b'AvgSize', b'0',
            b'Allocated', 0,
            b'AvgAllocated', b'0',
            b'Utilization', 0]
        assert hist[1] == [
            b'Count', 0, 
            b'Bytes', 0,
            b'AvgSize', b'0',
            b'Allocated', 0,
            b'AvgAllocated', b'0',
            b'Utilization', 0]
        assert hist[2] == []
        assert hist[3] == []
        hnsw_index = Index(
            "hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")]
        )
        hnsw_index.create(client)
        waiters.wait_for_true(lambda: IndexingTestHelper.is_indexing_complete_on_node(client, hnsw_index.name))
        hnsw_index.load_data(client, 10)
        print("Executing debug stats")
        hist = client.execute_command("FT._DEBUG StringPoolStats")
        print("Hist: ", hist)
        inline, outofline, byref, bysize = GetStringPollStatus(client)
        pprint(inline)
        pprint(outofline)
        pprint(byref)
        pprint(bysize)

        assert inline == {'Count': 10, 'Bytes': 140, 'AvgSize': b'14', 'Allocated': 320, 'AvgAllocated': b'32', 'Utilization': 43}
        assert outofline == {'Count': 10, 'Bytes': 120, 'AvgSize': b'12', 'Allocated': 280, 'AvgAllocated': b'28', 'Utilization': 42}
        assert byref[-1] == {'Count': 10, 'Bytes': 120, 'AvgSize': b'12', 'Allocated': 280, 'AvgAllocated': b'28', 'Utilization': 42}
        assert byref[4] == {'Count': 10, 'Bytes': 140, 'AvgSize': b'14', 'Allocated': 320, 'AvgAllocated': b'32', 'Utilization': 43}
        assert bysize[-12] == {'Count': 10, 'Bytes': 120, 'AvgSize': b'12', 'Allocated': 280, 'AvgAllocated': b'28', 'Utilization': 42}
        assert bysize[14] == {'Count': 10, 'Bytes': 140, 'AvgSize': b'14', 'Allocated': 320, 'AvgAllocated': b'32', 'Utilization': 43}
