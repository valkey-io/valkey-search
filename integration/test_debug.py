from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from indexes import *
from valkeytestframework.conftest import resource_port_tracker
import logging

def GetStringPollStatus(client):
    do_row = lambda row: [{row[i+0].decode():row[i+1]} for i in range(0, len(row), 2)]
    stats = client.execute_command("FT._DEBUG StringPoolStats")
    inline_total = do_row(stats[0])
    outofline_total = do_row(stats[1])
    byrefs = stats[2]
    byref = [{byrefs[i][0]:do_row(byrefs[i][1])} for i in range(len(byrefs))]
    bysizes = stats[3]
    bysize = [{bysizes[i][0]:do_row(bysizes[i][1])} for i in range(len(bysizes))]

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
            b'AvgSize', b'0']
        assert hist[1] == [
            b'Count', 0, 
            b'Bytes', 0,
            b'AvgSize', b'0']
        assert hist[2] == []
        assert hist[3] == []
        hnsw_index = Index(
            "hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")]
        )
        hnsw_index.create(client)
        hnsw_index.load_data(client, 10)
        print("Executing debug stats")
        hist = client.execute_command("FT._DEBUG StringPoolStats")
        print("Hist: ", hist)
        print(GetStringPollStatus(client))
        assert False
