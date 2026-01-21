from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from valkey.exceptions import ResponseError, ConnectionError
import pytest, time, logging
from indexes import *

class TestDBNum(ValkeySearchClusterTestCaseDebugMode):
    def setup_connections(self):
        self.client00 = self.get_primary(0).connect();
        self.client00.select(0)
        self.client10 = self.get_primary(1).connect();
        self.client10.select(0)
        self.client20 = self.get_primary(2).connect();
        self.client20.select(0)
        self.client01 = self.get_primary(0).connect();
        self.client01.select(1)
        self.client11 = self.get_primary(1).connect();
        self.client11.select(1)
        self.client21 = self.get_primary(2).connect();
        self.client21.select(1)
        self.clients = [[self.client00, self.client01], [self.client10, self.client11], [self.client20, self.client21]]

    def test_dbnum(self):
        """
        Test for dbnumbers in cluster mode, a Valkey 9 feature.
        """
        index0 = Index('index0', [Tag('t')])
        index1 = Index('index1', [Tag('t')])

        self.setup_connections()

        def show(msg):
            for i in range(3):
                self.clients[i][0].execute_command("DEBUG LOG", f"{i}:{msg}")
                print(self.clients[i][0].execute_command("FT._DEBUG SHOW_METADATA"))

        def exec(dbnum, l):
            for i in range(3):
                l(self.clients[i][dbnum])

        #show("Before create index1")
        index1.create(self.client11)
        #show("After create index1")
        index0.create(self.client10)
        #show("After create index0")
        assert(self.client00.execute_command("FT._LIST") == [b'index0'])
        assert(self.client10.execute_command("FT._LIST") == [b'index0'])
        assert(self.client20.execute_command("FT._LIST") == [b'index0'])
        assert(self.client01.execute_command("FT._LIST") == [b'index1'])
        assert(self.client11.execute_command("FT._LIST") == [b'index1'])
        assert(self.client21.execute_command("FT._LIST") == [b'index1'])
        try:
            self.client00.execute_command("debug restart")
        except:
            pass
        self.setup_connections()
        assert(self.client00.execute_command("FT._LIST") == [b'index0'])
        assert(self.client10.execute_command("FT._LIST") == [b'index0'])
        assert(self.client20.execute_command("FT._LIST") == [b'index0'])
        assert(self.client01.execute_command("FT._LIST") == [b'index1'])
        assert(self.client11.execute_command("FT._LIST") == [b'index1'])
        assert(self.client21.execute_command("FT._LIST") == [b'index1'])
        #
        # Load some data and do some queries....
        #
        #cluster0 = self.new_cluster_client()
        #cluster0.select(0)
        #cluster1 = self.new_cluster_client()
        #cluster1.select(1)
        self.client20.hset("0", mapping={"t":"tag0"})
        self.client21.hset("0", mapping={"t":"tag1"})
        answer0 = index0.query(self.client20,"@t:{tag*}")
        assert answer0 == {b"0": {b"t":b"tag0"}}
        answer0 = index0.aggregate(self.client20, "@t:{tag*}", "load", "*")
        print("Answer0", answer0)
        assert answer0 == [{"t":b"tag0"}]
        self.client21.execute_command("DEBUG LOG", "Doing query 1")
        answer1 = index1.query(self.client21,"@t:{tag*}")
        assert answer1 == {b"0": {b"t":b"tag1"}}
        answer1 = index1.aggregate(self.client21, "@t:{tag*}", "load", "*")
        print("Answer1", answer1)
        assert answer1 == [{"t":b"tag1"}]

        index0.drop(self.client00)
        show("After drop index0")
        assert(self.client00.execute_command("FT._LIST") == [])
        assert(self.client10.execute_command("FT._LIST") == [])
        assert(self.client20.execute_command("FT._LIST") == [])
        assert(self.client01.execute_command("FT._LIST") == [b'index1'])
        assert(self.client11.execute_command("FT._LIST") == [b'index1'])
        assert(self.client21.execute_command("FT._LIST") == [b'index1'])
        index1.drop(self.client01)
        show("after drop index1")
        assert(self.client00.execute_command("FT._LIST") == [])
        assert(self.client10.execute_command("FT._LIST") == [])
        assert(self.client20.execute_command("FT._LIST") == [])
        assert(self.client01.execute_command("FT._LIST") == [])
        assert(self.client11.execute_command("FT._LIST") == [])
        assert(self.client21.execute_command("FT._LIST") == [])
