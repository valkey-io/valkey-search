"""
Multi DB tests for FT.SEARCH - checks that different databases are isolated.
"""

import os
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode
)
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from indexes import Index, Text, Tag, Numeric, Vector, float_to_bytes


def verify_common_keys_results(clients, num_dbs, key_names, index):
    """Verify search isolation across all DBs for text, numeric, tag, and vector searches."""
    for db_num in range(num_dbs):
        # Text search
        for key_idx, key_name in enumerate(key_names):
            result = clients[db_num].execute_command(
                'FT.SEARCH', 'idx', f'@name:product{db_num}_key{key_idx}'
            )
            assert result[0] == 1, f"DB {db_num} text search failed for key {key_idx}"
            assert result[1] == key_name.encode()
        
        # Numeric search
        price = db_num * 100
        result = clients[db_num].execute_command(
            'FT.SEARCH', 'idx', f'@price:[{price} {price}]'
        )
        assert result[0] == 1, f"DB {db_num} numeric search failed"
        
        # Tag search
        result = clients[db_num].execute_command(
            'FT.SEARCH', 'idx', f'@category:{{cat{db_num}}}', 'LIMIT', '0', '100'
        )
        assert result[0] == len(key_names), f"DB {db_num} tag search failed"
        
        # Vector KNN search
        query_vec = float_to_bytes([1.0, 0.0, 0.0, 0.0])
        result = clients[db_num].execute_command(
            'FT.SEARCH', 'idx', '*=>[KNN 1 @vec $vec]', 'PARAMS', '2', 'vec', query_vec
        )
        assert result[0] == 1, f"DB {db_num} vector search failed"
        
        # Verify can't see other DB's data
        other_db = (db_num + 1) % num_dbs
        other_result = clients[db_num].execute_command(
            'FT.SEARCH', 'idx', f'@name:product{other_db}_key0'
        )
        assert other_result[0] == 0, f"DB {db_num} should not see DB {other_db}'s data"


def verify_unique_keys_results(clients, num_dbs, db_keys, index):
    """Verify search isolation when each DB has unique key names."""
    for db_num in range(num_dbs):
        # Text search for unique keys
        for key_idx, key_name in enumerate(db_keys[db_num]):
            result = clients[db_num].execute_command(
                'FT.SEARCH', 'idx', f'@name:product{db_num}_unique{key_idx}'
            )
            assert result[0] == 1, f"DB {db_num} text search failed"
            assert result[1] == key_name.encode()
        
        # Numeric search
        price = 10000 + db_num * 100
        result = clients[db_num].execute_command(
            'FT.SEARCH', 'idx', f'@price:[{price} {price}]'
        )
        assert result[0] == 1, f"DB {db_num} numeric search failed"
        
        # Tag search - uses unique category to only find unique keys for this DB
        result = clients[db_num].execute_command(
            'FT.SEARCH', 'idx', f'@category:{{cat{db_num}_unique}}', 'LIMIT', '0', '100'
        )
        assert result[0] == len(db_keys[db_num]), f"DB {db_num} tag search failed"
        
        # Vector KNN search
        query_vec = float_to_bytes([1.0, 0.0, 0.0, 0.0])
        result = clients[db_num].execute_command(
            'FT.SEARCH', 'idx', '*=>[KNN 1 @vec $vec]', 'PARAMS', '2', 'vec', query_vec
        )
        assert result[0] == 1, f"DB {db_num} vector search failed"
        
        # Verify can't see other DB's unique keys
        other_db = (db_num + 1) % num_dbs
        other_result = clients[db_num].execute_command(
            'FT.SEARCH', 'idx', f'@name:product{other_db}_unique0'
        )
        assert other_result[0] == 0, f"DB {db_num} should not see DB {other_db}'s unique keys"


def create_clients(num_dbs, get_client_func):
    """Create clients for multiple databases."""
    clients = {}
    for db_num in range(num_dbs):
        client = get_client_func()
        client.select(db_num)
        clients[db_num] = client
    return clients


def create_indexes(clients, num_dbs, prefixes=None):
    """Create search indexes on all DBs."""
    index = Index('idx', [
        Text('name'),
        Numeric('price'),
        Tag('category'),
        Vector('vec', dim=4)
    ], prefixes=prefixes)
    
    for db_num in range(num_dbs):
        index.create(clients[db_num])
    return index


def add_common_keys_data(clients, num_dbs, key_prefix, num_keys=3):
    """Add same key names to all DBs with different data values."""
    key_names = [f'{key_prefix}{i}' for i in range(num_keys)]
    
    for db_num in range(num_dbs):
        for key_idx, key_name in enumerate(key_names):
            vec = [0.0, 0.0, 0.0, 0.0]
            vec[db_num % 4] = 1.0
            clients[db_num].hset(key_name, mapping={
                'name': f'product{db_num}_key{key_idx}',
                'price': str(db_num * 100 + key_idx),
                'category': f'cat{db_num}',
                'vec': float_to_bytes(vec)
            })
    
    return key_names


def add_unique_keys_data(clients, num_dbs, key_prefix, keys_per_db=3):
    """Add unique key names per DB."""
    db_keys = {}
    
    for db_num in range(num_dbs):
        db_keys[db_num] = []
        for key_idx in range(keys_per_db):
            key_name = f'{key_prefix}db{db_num}_{key_idx}'
            db_keys[db_num].append(key_name)
            
            vec = [0.0, 0.0, 0.0, 0.0]
            vec[db_num % 4] = 1.0
            clients[db_num].hset(key_name, mapping={
                'name': f'product{db_num}_unique{key_idx}',
                'price': str(10000 + db_num * 100 + key_idx),
                'category': f'cat{db_num}_unique',
                'vec': float_to_bytes(vec)
            })
    
    return db_keys


class TestMultiDBCMD(ValkeySearchTestCaseDebugMode):
    """Standalone mode tests"""

    def test_multidb_keys_isolation_CMD(self):
        """Test isolation with both common and unique key names across DBs."""
        num_dbs = 4
        clients = create_clients(num_dbs, self.server.get_new_client)
        index = create_indexes(clients, num_dbs, prefixes=['p:'])
        
        # Test common keys (same key names across all DBs)
        common_key_names = add_common_keys_data(clients, num_dbs, 'p:', num_keys=3)
        verify_common_keys_results(clients, num_dbs, common_key_names, index)
        
        # Test unique keys (different key names per DB)
        db_keys = add_unique_keys_data(clients, num_dbs, 'p:unique_', keys_per_db=3)
        verify_unique_keys_results(clients, num_dbs, db_keys, index)
        
        # Update DB0 common key, verify other DBs unchanged
        clients[0].hset(common_key_names[0], mapping={
            'name': 'updated',
            'price': '9999',
            'category': 'updated',
            'vec': float_to_bytes([9.0, 9.0, 9.0, 9.0])
        })
        assert len(index.query(clients[0], "@name:updated")) == 1
        assert len(index.query(clients[0], "@name:product0_key0")) == 0
        for db_num in range(1, num_dbs):
            assert len(index.query(clients[db_num], f"@name:product{db_num}_key0")) == 1
        
        # Delete common key from DB0, verify other DBs still have data
        clients[0].delete(common_key_names[0])
        assert len(index.query(clients[0], "@name:updated")) == 0
        for db_num in range(1, num_dbs):
            assert len(index.query(clients[db_num], f"@name:product{db_num}_key0")) == 1
        
        # Update DB0 unique key, verify other DBs unchanged
        clients[0].hset(db_keys[0][0], mapping={
            'name': 'updated_unique',
            'price': '8888',
            'category': 'updated',
            'vec': float_to_bytes([8.0, 8.0, 8.0, 8.0])
        })
        assert len(index.query(clients[0], "@name:updated_unique")) == 1
        assert len(index.query(clients[0], "@name:product0_unique0")) == 0
        for db_num in range(1, num_dbs):
            assert len(index.query(clients[db_num], f"@name:product{db_num}_unique0")) == 1
        
        # Delete unique key from DB0, verify other DBs still have data
        clients[0].delete(db_keys[0][0])
        assert len(index.query(clients[0], "@name:updated_unique")) == 0
        for db_num in range(1, num_dbs):
            assert len(index.query(clients[db_num], f"@name:product{db_num}_unique0")) == 1

    def test_multidb_rdb_save_load_CMD(self):
        """Test that multi-DB isolation persists after RDB save/load."""
        num_dbs = 4
        clients = create_clients(num_dbs, self.server.get_new_client)
        index = create_indexes(clients, num_dbs, prefixes=['p:'])
        
        key_names = add_common_keys_data(clients, num_dbs, 'p:', num_keys=3)
        
        # Verify data before save
        verify_common_keys_results(clients, num_dbs, key_names, index)
        
        clients[0].execute_command('SAVE')
        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        
        # Wait for server to be ready
        def server_ready():
            try:
                client = self.server.get_new_client()
                client.ping()
                return True
            except Exception:
                return False
        waiters.wait_for_true(server_ready)
        
        clients = create_clients(num_dbs, self.server.get_new_client)
        for db_num in range(num_dbs):
            waiters.wait_for_true(lambda db=db_num: index.backfill_complete(clients[db]))
        
        verify_common_keys_results(clients, num_dbs, key_names, index)


class TestMultiDBCME(ValkeySearchClusterTestCaseDebugMode):
    """Cluster mode tests"""

    def test_multidb_keys_isolation_CME(self):
        """Test isolation with both common and unique key names across DBs in cluster mode."""
        num_dbs = 4
        clients = create_clients(num_dbs, self.get_primary(2).connect)
        index = create_indexes(clients, num_dbs, prefixes=['p:'])
        
        # Test common keys (same key names across all DBs)
        common_key_names = add_common_keys_data(clients, num_dbs, 'p:{0}', num_keys=3)
        verify_common_keys_results(clients, num_dbs, common_key_names, index)
        
        # Test unique keys (different key names per DB)
        db_keys = add_unique_keys_data(clients, num_dbs, 'p:{0}unique_', keys_per_db=3)
        verify_unique_keys_results(clients, num_dbs, db_keys, index)
        
        # Update DB0 common key, verify other DBs unchanged
        clients[0].hset(common_key_names[0], mapping={
            'name': 'updated',
            'price': '9999',
            'category': 'updated',
            'vec': float_to_bytes([9.0, 9.0, 9.0, 9.0])
        })
        assert len(index.query(clients[0], "@name:updated")) == 1
        assert len(index.query(clients[0], "@name:product0_key0")) == 0
        for db_num in range(1, num_dbs):
            assert len(index.query(clients[db_num], f"@name:product{db_num}_key0")) == 1
        
        # Delete common key from DB0, verify other DBs still have data
        clients[0].delete(common_key_names[0])
        assert len(index.query(clients[0], "@name:updated")) == 0
        for db_num in range(1, num_dbs):
            assert len(index.query(clients[db_num], f"@name:product{db_num}_key0")) == 1
        
        # Update DB0 unique key, verify other DBs unchanged
        clients[0].hset(db_keys[0][0], mapping={
            'name': 'updated_unique',
            'price': '8888',
            'category': 'updated',
            'vec': float_to_bytes([8.0, 8.0, 8.0, 8.0])
        })
        assert len(index.query(clients[0], "@name:updated_unique")) == 1
        assert len(index.query(clients[0], "@name:product0_unique0")) == 0
        for db_num in range(1, num_dbs):
            assert len(index.query(clients[db_num], f"@name:product{db_num}_unique0")) == 1
        
        # Delete unique key from DB0, verify other DBs still have data
        clients[0].delete(db_keys[0][0])
        assert len(index.query(clients[0], "@name:updated_unique")) == 0
        for db_num in range(1, num_dbs):
            assert len(index.query(clients[db_num], f"@name:product{db_num}_unique0")) == 1

    def test_multidb_rdb_save_load_CME(self):
        """Test that multi-DB isolation persists after RDB save/load in cluster mode."""
        num_dbs = 4
        clients = create_clients(num_dbs, self.get_primary(2).connect)
        index = create_indexes(clients, num_dbs, prefixes=['p:'])
        
        key_names = add_common_keys_data(clients, num_dbs, 'p:{0}', num_keys=3)
        
        # Verify data before save
        verify_common_keys_results(clients, num_dbs, key_names, index)
        
        # Save on the primary we're connected to
        clients[0].execute_command('BGSAVE')
        waiters.wait_for_true(
            lambda: clients[0].info('persistence')['rdb_bgsave_in_progress'] == 0
        )
        
        # Restart the primary node using restart which preserves RDB
        os.environ["SKIPLOGCLEAN"] = "1"
        primary = self.get_primary(2)
        primary.restart(remove_rdb=False)
        
        # Wait for server to be ready and accept connections
        def server_ready():
            try:
                client = primary.connect()
                client.ping()
                return True
            except Exception:
                return False
        waiters.wait_for_true(server_ready)
        
        clients = create_clients(num_dbs, self.get_primary(2).connect)
        for db_num in range(num_dbs):
            waiters.wait_for_true(lambda db=db_num: index.backfill_complete(clients[db]))
        
        verify_common_keys_results(clients, num_dbs, key_names, index)

    def test_multidb_slot_migration_CME(self):
        """Test that multi-DB isolation is maintained after slot migration."""
        num_dbs = 4
        clients = create_clients(num_dbs, self.get_primary(2).connect)
        index = create_indexes(clients, num_dbs, prefixes=['p:'])
        
        key_names = add_common_keys_data(clients, num_dbs, 'p:{0}', num_keys=3)
        
        # Get slot for our keys (they all use {0} hash tag)
        slot = clients[0].execute_command('CLUSTER KEYSLOT', '{0}')
        
        # Get source (shard 2) and destination (shard 0) node IDs
        source_client = self.get_primary(2).connect()
        dest_client = self.get_primary(0).connect()
        source_id = source_client.execute_command('CLUSTER MYID').decode()
        dest_id = dest_client.execute_command('CLUSTER MYID').decode()
        
        # Migrate slot from shard 2 to shard 0
        dest_client.execute_command('CLUSTER SETSLOT', slot, 'IMPORTING', source_id)
        source_client.execute_command('CLUSTER SETSLOT', slot, 'MIGRATING', dest_id)
        
        # Migrate all keys in the slot for all DBs
        for db_num in range(num_dbs):
            source_client.select(db_num)
            dest_client.select(db_num)
            
            keys = source_client.execute_command('CLUSTER GETKEYSINSLOT', slot, 100)
            if keys:
                source_client.execute_command(
                    'MIGRATE', dest_client.connection_pool.connection_kwargs['host'],
                    dest_client.connection_pool.connection_kwargs['port'],
                    '', db_num, 5000, 'KEYS', *keys
                )
        
        # Finalize migration
        for node_client in self.get_all_primary_clients():
            node_client.execute_command('CLUSTER SETSLOT', slot, 'NODE', dest_id)
        
        # Verify isolation on destination shard
        dest_clients = {}
        for db_num in range(num_dbs):
            client = self.get_primary(0).connect()
            client.select(db_num)
            dest_clients[db_num] = client
        
        verify_common_keys_results(dest_clients, num_dbs, key_names, index)
