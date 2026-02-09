"""
Multi DB tests for FT.SEARCH - checks that different databases are isolated.
"""

from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode
)
from valkeytestframework.conftest import resource_port_tracker
import pytest
from indexes import Index, Text, Tag, Numeric, Vector, KeyDataType, float_to_bytes


class TestMultiDBCMD(ValkeySearchTestCaseDebugMode):
    """Standalone mode tests"""

    def test_multidb_isolation_CMD(self):
        """Make sure DB0 and DB1 don't see each other's data"""
        num_dbs = 4
        clients = {}
        
        for db_num in range(num_dbs):
            client = self.server.get_new_client()
            client.select(db_num)
            clients[db_num] = client
        
        index = Index('idx', [
            Text('name'),
            Numeric('price'),
            Tag('category'),
            Vector('vec', dim=4)
        ], prefixes=['p:'])
        
        for db_num in range(num_dbs):
            index.create(clients[db_num])
        
        # Add unique data to each DB
        for db_num in range(num_dbs):
            vec = [0.0, 0.0, 0.0, 0.0]
            vec[db_num % 4] = 1.0
            clients[db_num].hset('p:1', mapping={
                'name': f'product{db_num}',
                'price': str(db_num * 100),
                'category': f'cat{db_num}',
                'vec': float_to_bytes(vec)
            })
        
        # Check text search isolation
        for db_num in range(num_dbs):
            result = index.query(clients[db_num], f"@name:product{db_num}")
            assert len(result) == 1, f"DB {db_num} should find its own data"
            
            other_db = (db_num + 1) % num_dbs
            result = index.query(clients[db_num], f"@name:product{other_db}")
            assert len(result) == 0, f"DB {db_num} should not see DB {other_db}"
        
        # Check numeric search isolation
        for db_num in range(num_dbs):
            price = db_num * 100
            count = clients[db_num].execute_command('FT.SEARCH', 'idx', f'@price:[{price-1} {price+1}]')[0]
            assert count == 1
        
        # Check tag search isolation
        for db_num in range(num_dbs):
            count = clients[db_num].execute_command('FT.SEARCH', 'idx', f'@category:{{cat{db_num}}}')[0]
            assert count == 1
            
            other_db = (db_num + 1) % num_dbs
            count = clients[db_num].execute_command('FT.SEARCH', 'idx', f'@category:{{cat{other_db}}}')[0]
            assert count == 0
        
        # Check vector KNN search isolation
        query_vec = float_to_bytes([1.0, 0.0, 0.0, 0.0])
        for db_num in range(num_dbs):
            result = clients[db_num].execute_command('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $vec]', 'PARAMS', '2', 'vec', query_vec)
            assert result[0] == 1
        
        # Update DB0, verify other DBs unchanged
        clients[0].hset('p:1', mapping={
            'name': 'updated',
            'price': '9999',
            'category': 'updated',
            'vec': float_to_bytes([9.0, 9.0, 9.0, 9.0])
        })
        
        assert len(index.query(clients[0], "@name:updated")) == 1
        assert len(index.query(clients[0], "@name:product0")) == 0
        
        for db_num in range(1, num_dbs):
            result = index.query(clients[db_num], f"@name:product{db_num}")
            assert len(result) == 1
        
        # Delete from DB0, verify other DBs still have data
        clients[0].delete('p:1')
        
        assert len(index.query(clients[0], "@name:updated")) == 0
        
        for db_num in range(1, num_dbs):
            result = index.query(clients[db_num], f"@name:product{db_num}")
            assert len(result) == 1


class TestMultiDBCME(ValkeySearchClusterTestCaseDebugMode):
    """Cluster mode tests"""

    def test_multidb_isolation_CME(self):
        """Test DB isolation in cluster mode"""
        num_dbs = 4
        clients = {}
        
        # Connect to shard 2 where key "0" hashes
        for db_num in range(num_dbs):
            client = self.get_primary(2).connect()
            client.select(db_num)
            clients[db_num] = client
        
        index = Index('idx', [
            Text('name'),
            Numeric('price'),
            Tag('category'),
            Vector('vec', dim=4)
        ])
        
        for db_num in range(num_dbs):
            index.create(clients[db_num])
        
        # Add unique data to each DB
        for db_num in range(num_dbs):
            vec = [0.0, 0.0, 0.0, 0.0]
            vec[db_num % 4] = 1.0
            clients[db_num].hset('0', mapping={
                'name': f'product{db_num}',
                'price': str(db_num * 100),
                'category': f'cat{db_num}',
                'vec': float_to_bytes(vec)
            })
        
        # Check text search isolation
        for db_num in range(num_dbs):
            result = index.query(clients[db_num], f"@name:product{db_num}")
            assert len(result) == 1, f"DB {db_num} should find its own data"
            
            other_db = (db_num + 1) % num_dbs
            result = index.query(clients[db_num], f"@name:product{other_db}")
            assert len(result) == 0, f"DB {db_num} should not see DB {other_db}"
        
        # Check numeric search isolation
        for db_num in range(num_dbs):
            price = db_num * 100
            count = clients[db_num].execute_command('FT.SEARCH', 'idx', f'@price:[{price-1} {price+1}]')[0]
            assert count == 1
        
        # Check tag search isolation
        for db_num in range(num_dbs):
            count = clients[db_num].execute_command('FT.SEARCH', 'idx', f'@category:{{cat{db_num}}}')[0]
            assert count == 1
            
            other_db = (db_num + 1) % num_dbs
            count = clients[db_num].execute_command('FT.SEARCH', 'idx', f'@category:{{cat{other_db}}}')[0]
            assert count == 0
        
        # Check vector KNN search isolation
        query_vec = float_to_bytes([1.0, 0.0, 0.0, 0.0])
        for db_num in range(num_dbs):
            result = clients[db_num].execute_command('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $vec]', 'PARAMS', '2', 'vec', query_vec)
            assert result[0] == 1
        
        # Update DB0, verify other DBs unchanged
        clients[0].hset('0', mapping={
            'name': 'updated',
            'price': '9999',
            'category': 'updated',
            'vec': float_to_bytes([9.0, 9.0, 9.0, 9.0])
        })
        
        assert len(index.query(clients[0], "@name:updated")) == 1
        assert len(index.query(clients[0], "@name:product0")) == 0
        
        for db_num in range(1, num_dbs):
            result = index.query(clients[db_num], f"@name:product{db_num}")
            assert len(result) == 1
        
        # Delete from DB0, verify other DBs still have data
        clients[0].delete('0')
        
        assert len(index.query(clients[0], "@name:updated")) == 0
        
        for db_num in range(1, num_dbs):
            result = index.query(clients[db_num], f"@name:product{db_num}")
            assert len(result) == 1
