import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

class TestCommandInfo(ValkeySearchTestCaseBase):
    """Integration tests for command metadata using COMMAND INFO and COMMAND DOCS"""

    def test_ft_create_command_info(self):
        """Test FT.CREATE command info"""
        client: Valkey = self.server.get_new_client()
        
        info = client.execute_command("COMMAND", "INFO", "FT.CREATE")
        assert info is not None
        assert isinstance(info, dict)

        ft_create_info = info.get('FT.CREATE')
        assert ft_create_info is not None
        assert isinstance(ft_create_info, dict)
        
        # Verify command name
        assert ft_create_info['name'] == 'FT.CREATE'
        
        # Verify arity
        assert ft_create_info['arity'] == -2
        
        # Verify command flags
        flags = ft_create_info['flags']
        assert isinstance(flags, list)
        assert 'write' in flags
        assert 'denyoom' in flags
        assert 'module' in flags
        assert 'fast' in flags
        
        # Verify first key position
        assert ft_create_info['first_key_pos'] == 0

        # Verify last key position
        assert ft_create_info['last_key_pos'] == 0
        
        # Verify step count
        assert ft_create_info['step_count'] == 0
        
        # Verify tips (should be empty list)
        assert ft_create_info['tips'] == []
        
        # Verify key specifications (should be empty list)
        assert ft_create_info['key_specifications'] == []
        
        # Verify subcommands (should be empty list)
        assert ft_create_info['subcommands'] == []

    def test_ft_create_command_docs(self):
        """Test FT.CREATE command docs"""
        client: Valkey = self.server.get_new_client()
        
        # Bypass the default parser by using raw connection
        conn = client.connection_pool.get_connection('COMMAND')
        try:
            conn.send_command("COMMAND", "DOCS", "FT.CREATE")
            docs = conn.read_response()
        finally:
            client.connection_pool.release(conn)
        assert docs is not None
        assert isinstance(docs, list)
        assert len(docs) >= 2

        assert docs[0] == b'FT.CREATE'
        
        # docs[1] is a flat list of key-value pairs
        kv_list = docs[1]
        assert kv_list is not None
        assert isinstance(kv_list, list)
        
        # Helper function to parse nested structures
        def parse_kv_list(kv_list):
            result = {}
            i = 0
            while i < len(kv_list):
                key = kv_list[i]
                value = kv_list[i + 1] if i + 1 < len(kv_list) else None
                
                if isinstance(value, list) and value and isinstance(value[0], list):
                    parsed_value = []
                    for item in value:
                        if isinstance(item, list):
                            parsed_value.append(parse_kv_list(item))
                        else:
                            parsed_value.append(item)
                    result[key] = parsed_value
                else:
                    result[key] = value
                i += 2
            return result
        
        ft_create_docs = parse_kv_list(kv_list)
        
        # Verify summary
        assert ft_create_docs[b"summary"] == b"Creates an empty search index and initiates the backfill process"
        
        # Verify complexity
        assert ft_create_docs[b"complexity"] == b"O(N log N), where N is the number of indexed items"
        
        # Verify since version
        assert ft_create_docs[b"since"] == b"1.0.0"
        
        # Verify arguments structure
        arguments = ft_create_docs[b"arguments"]
        assert isinstance(arguments, list)
        assert len(arguments) == 4
        
        # Verify first argument (index_name)
        index_name_arg = arguments[0]
        assert isinstance(index_name_arg, dict)
        assert index_name_arg[b"name"] == b"index_name"
        assert index_name_arg[b"type"] == b"string"
        assert index_name_arg[b"summary"] == b"Name of the index"
        assert index_name_arg[b"since"] == b"1.0.0"
        
        # Verify second argument (on_data_type)
        on_arg = arguments[1]
        assert isinstance(on_arg, dict)
        assert on_arg[b"name"] == b"on_data_type"
        assert on_arg[b"type"] == b"oneof"
        assert on_arg[b"token"] == b"ON"
        assert on_arg[b"summary"] == b"Data type to index"
        assert on_arg[b"since"] == b"1.0.0"
        assert b"optional" in on_arg[b"flags"]
        
        # Verify ON subarguments (HASH and JSON)
        on_subargs = on_arg[b"arguments"]
        assert isinstance(on_subargs, list)
        assert len(on_subargs) == 2
        
        hash_arg = on_subargs[0]
        assert isinstance(hash_arg, dict)
        assert hash_arg[b"name"] == b"hash"
        assert hash_arg[b"type"] == b"pure-token"
        assert hash_arg[b"token"] == b"HASH"
        assert hash_arg[b"summary"] == b"Index HASH data type"
        
        json_arg = on_subargs[1]
        assert isinstance(json_arg, dict)
        assert json_arg[b"name"] == b"json"
        assert json_arg[b"type"] == b"pure-token"
        assert json_arg[b"token"] == b"JSON"
        assert json_arg[b"summary"] == b"Index JSON data type"
        
        # Verify third argument (prefix)
        prefix_arg = arguments[2]
        assert isinstance(prefix_arg, dict)
        assert prefix_arg[b"name"] == b"prefix"
        assert prefix_arg[b"type"] == b"block"
        assert prefix_arg[b"token"] == b"PREFIX"
        assert prefix_arg[b"summary"] == b"Key prefixes to index"
        assert prefix_arg[b"since"] == b"1.0.0"
        assert b"optional" in prefix_arg[b"flags"]
        
        # Verify PREFIX subarguments
        prefix_subargs = prefix_arg[b"arguments"]
        assert isinstance(prefix_subargs, list)
        assert len(prefix_subargs) == 2
        
        count_arg = prefix_subargs[0]
        assert isinstance(count_arg, dict)
        assert count_arg[b"name"] == b"count"
        assert count_arg[b"type"] == b"integer"
        assert count_arg[b"summary"] == b"Number of prefixes"
        assert count_arg[b"since"] == b"1.0.0"
        
        prefix_value_arg = prefix_subargs[1]
        assert isinstance(prefix_value_arg, dict)
        assert prefix_value_arg[b"name"] == b"prefix"
        assert prefix_value_arg[b"type"] == b"string"
        assert prefix_value_arg[b"summary"] == b"Key prefix to index"
        assert prefix_value_arg[b"since"] == b"1.0.0"
        assert b"multiple" in prefix_value_arg[b"flags"]
        
        # Verify fourth argument (schema)
        schema_arg = arguments[3]
        assert isinstance(schema_arg, dict)
        assert schema_arg[b"name"] == b"schema"
        assert schema_arg[b"type"] == b"block"
        assert schema_arg[b"token"] == b"SCHEMA"
        assert schema_arg[b"summary"] == b"Schema definition"
        assert schema_arg[b"since"] == b"1.0.0"
        assert b"multiple" in schema_arg[b"flags"]
        
        # Verify SCHEMA subarguments
        schema_subargs = schema_arg[b"arguments"]
        assert isinstance(schema_subargs, list)
        assert len(schema_subargs) == 3
        
        field_id_arg = schema_subargs[0]
        assert isinstance(field_id_arg, dict)
        assert field_id_arg[b"name"] == b"field_identifier"
        assert field_id_arg[b"type"] == b"string"
        assert field_id_arg[b"summary"] == b"Field identifier"
        assert field_id_arg[b"since"] == b"1.0.0"
        
        as_arg = schema_subargs[1]
        assert isinstance(as_arg, dict)
        assert as_arg[b"name"] == b"as"
        assert as_arg[b"type"] == b"string"
        assert as_arg[b"token"] == b"AS"
        assert as_arg[b"summary"] == b"Field alias"
        assert as_arg[b"since"] == b"1.0.0"
        assert b"optional" in as_arg[b"flags"]
        
        field_type_arg = schema_subargs[2]
        assert isinstance(field_type_arg, dict)
        assert field_type_arg[b"name"] == b"field_type"
        assert field_type_arg[b"type"] == b"oneof"
        assert field_type_arg[b"summary"] == b"Field type (NUMERIC, TAG, VECTOR)"
        assert field_type_arg[b"since"] == b"1.0.0"
        
        # Verify field types
        field_types = field_type_arg[b"arguments"]
        assert isinstance(field_types, list)
        assert len(field_types) == 3
        
        numeric_type = field_types[0]
        assert isinstance(numeric_type, dict)
        assert numeric_type[b"name"] == b"numeric"
        assert numeric_type[b"type"] == b"pure-token"
        assert numeric_type[b"token"] == b"NUMERIC"
        assert numeric_type[b"summary"] == b"Numeric field type"
        assert numeric_type[b"since"] == b"1.0.0"
        
        tag_type = field_types[1]
        assert isinstance(tag_type, dict)
        assert tag_type[b"name"] == b"tag"
        assert tag_type[b"type"] == b"block"
        assert tag_type[b"token"] == b"TAG"
        assert tag_type[b"summary"] == b"Tag field type"
        assert tag_type[b"since"] == b"1.0.0"
        
        vector_type = field_types[2]
        assert isinstance(vector_type, dict)
        assert vector_type[b"name"] == b"vector"
        assert vector_type[b"type"] == b"block"
        assert vector_type[b"token"] == b"VECTOR"
        assert vector_type[b"summary"] == b"Vector field type"
        assert vector_type[b"since"] == b"1.0.0"

        # Verify TAG subarguments
        tag_subargs = tag_type[b"arguments"]
        assert isinstance(tag_subargs, list)
        assert len(tag_subargs) == 2
        
        separator_arg = tag_subargs[0]
        assert isinstance(separator_arg, dict)
        assert separator_arg[b"name"] == b"separator"
        assert separator_arg[b"type"] == b"string"
        assert separator_arg[b"token"] == b"SEPARATOR"
        assert separator_arg[b"summary"] == b"Tag separator character"
        assert separator_arg[b"since"] == b"1.0.0"
        assert b"optional" in separator_arg[b"flags"]
        
        casesensitive_arg = tag_subargs[1]
        assert isinstance(casesensitive_arg, dict)
        assert casesensitive_arg[b"name"] == b"casesensitive"
        assert casesensitive_arg[b"type"] == b"pure-token"
        assert casesensitive_arg[b"token"] == b"CASESENSITIVE"
        assert casesensitive_arg[b"summary"] == b"Make tag matching case sensitive"
        assert casesensitive_arg[b"since"] == b"1.0.0"
        assert b"optional" in casesensitive_arg[b"flags"]
        
        # Verify VECTOR subarguments
        vector_subargs = vector_type[b"arguments"]
        assert isinstance(vector_subargs, list)
        assert len(vector_subargs) == 1
        
        algorithm_arg = vector_subargs[0]
        assert isinstance(algorithm_arg, dict)
        assert algorithm_arg[b"name"] == b"algorithm"
        assert algorithm_arg[b"type"] == b"oneof"
        assert algorithm_arg[b"summary"] == b"Vector algorithm (HNSW or FLAT)"
        assert algorithm_arg[b"since"] == b"1.0.0"
        
        # Verify algorithm options (HNSW and FLAT)
        algorithms = algorithm_arg[b"arguments"]
        assert isinstance(algorithms, list)
        assert len(algorithms) == 2
        
        # Test HNSW algorithm
        hnsw_alg = algorithms[0]
        assert isinstance(hnsw_alg, dict)
        assert hnsw_alg[b"name"] == b"hnsw"
        assert hnsw_alg[b"type"] == b"block"
        assert hnsw_alg[b"token"] == b"HNSW"
        assert hnsw_alg[b"summary"] == b"HNSW vector algorithm"
        assert hnsw_alg[b"since"] == b"1.0.0"
        
        # Verify HNSW parameters
        hnsw_params = hnsw_alg[b"arguments"]
        assert isinstance(hnsw_params, list)
        assert len(hnsw_params) == 8
        
        # Check all HNSW parameters
        # 1. attribute_count parameter
        attr_count_param = hnsw_params[0]
        assert attr_count_param[b"name"] == b"attribute_count"
        assert attr_count_param[b"type"] == b"integer"
        assert attr_count_param[b"summary"] == b"Number of attributes following"
        assert attr_count_param[b"since"] == b"1.0.0"
        
        # 2. DIM parameter
        dim_param = hnsw_params[1]
        assert dim_param[b"name"] == b"dim"
        assert dim_param[b"type"] == b"integer"
        assert dim_param[b"token"] == b"DIM"
        assert dim_param[b"summary"] == b"Vector dimensions (required)"
        assert dim_param[b"since"] == b"1.0.0"
        
        # 3. TYPE parameter
        type_param = hnsw_params[2]
        assert type_param[b"name"] == b"type"
        assert type_param[b"type"] == b"oneof"
        assert type_param[b"token"] == b"TYPE"
        assert type_param[b"summary"] == b"Vector data type (required)"
        assert type_param[b"since"] == b"1.0.0"

        # Check type options (FLOAT32)
        type_options = type_param[b"arguments"]
        assert len(type_options) == 1
        float32_option = type_options[0]
        assert float32_option[b"name"] == b"float32"
        assert float32_option[b"type"] == b"pure-token"
        assert float32_option[b"token"] == b"FLOAT32"
        assert float32_option[b"summary"] == b"32-bit floating point vector"
        assert float32_option[b"since"] == b"1.0.0"

        # 4. DISTANCE_METRIC parameter
        distance_param = hnsw_params[3]
        assert distance_param[b"name"] == b"distance_metric"
        assert distance_param[b"type"] == b"oneof"
        assert distance_param[b"token"] == b"DISTANCE_METRIC"
        assert distance_param[b"summary"] == b"Distance algorithm (required)"
        assert distance_param[b"since"] == b"1.0.0"
        
        # Check distance metric options (L2, IP, COSINE)
        distance_options = distance_param[b"arguments"]
        assert len(distance_options) == 3
        
        l2_option = distance_options[0]
        assert l2_option[b"name"] == b"l2"
        assert l2_option[b"type"] == b"pure-token"
        assert l2_option[b"token"] == b"L2"
        assert l2_option[b"summary"] == b"L2 (Euclidean) distance"
        assert l2_option[b"since"] == b"1.0.0"
        
        ip_option = distance_options[1]
        assert ip_option[b"name"] == b"ip"
        assert ip_option[b"type"] == b"pure-token"
        assert ip_option[b"token"] == b"IP"
        assert ip_option[b"summary"] == b"Inner product distance"
        assert ip_option[b"since"] == b"1.0.0"
        
        cosine_option = distance_options[2]
        assert cosine_option[b"name"] == b"cosine"
        assert cosine_option[b"type"] == b"pure-token"
        assert cosine_option[b"token"] == b"COSINE"
        assert cosine_option[b"summary"] == b"Cosine distance"
        assert cosine_option[b"since"] == b"1.0.0"
        
        # 5. INITIAL_CAP parameter
        initial_cap_param = hnsw_params[4]
        assert initial_cap_param[b"name"] == b"initial_cap"
        assert initial_cap_param[b"type"] == b"integer"
        assert initial_cap_param[b"token"] == b"INITIAL_CAP"
        assert initial_cap_param[b"summary"] == b"Initial index size (optional)"
        assert initial_cap_param[b"since"] == b"1.0.0"
        assert b"optional" in initial_cap_param[b"flags"]
        
        # 6. M parameter
        m_param = hnsw_params[5]
        assert m_param[b"name"] == b"m"
        assert m_param[b"type"] == b"integer"
        assert m_param[b"token"] == b"M"
        assert m_param[b"summary"] == b"Maximum outgoing edges per node (default 16, max 512)"
        assert m_param[b"since"] == b"1.0.0"
        assert b"optional" in m_param[b"flags"]
        
        # 7. EF_CONSTRUCTION parameter
        ef_construction_param = hnsw_params[6]
        assert ef_construction_param[b"name"] == b"ef_construction"
        assert ef_construction_param[b"type"] == b"integer"
        assert ef_construction_param[b"token"] == b"EF_CONSTRUCTION"
        assert ef_construction_param[b"summary"] == b"Vectors examined during index creation (default 200, max 4096)"
        assert ef_construction_param[b"since"] == b"1.0.0"
        assert b"optional" in ef_construction_param[b"flags"]
        
        # 8. EF_RUNTIME parameter
        ef_runtime_param = hnsw_params[7]
        assert ef_runtime_param[b"name"] == b"ef_runtime"
        assert ef_runtime_param[b"type"] == b"integer"
        assert ef_runtime_param[b"token"] == b"EF_RUNTIME"
        assert ef_runtime_param[b"summary"] == b"Vectors examined during query (default 10, max 4096)"
        assert ef_runtime_param[b"since"] == b"1.0.0"
        assert b"optional" in ef_runtime_param[b"flags"]
        
        # Test FLAT algorithm
        flat_alg = algorithms[1]
        assert flat_alg[b"name"] == b"flat"
        assert flat_alg[b"type"] == b"block"
        assert flat_alg[b"token"] == b"FLAT"
        assert flat_alg[b"summary"] == b"FLAT vector algorithm"
        assert flat_alg[b"since"] == b"1.0.0"
        
        # Verify FLAT parameters
        flat_params = flat_alg[b"arguments"]
        assert len(flat_params) == 5
        
        # Check all FLAT parameters
        # 1. attribute_count parameter
        flat_attr_count_param = flat_params[0]
        assert flat_attr_count_param[b"name"] == b"attribute_count"
        assert flat_attr_count_param[b"type"] == b"integer"
        assert flat_attr_count_param[b"summary"] == b"Number of attributes following"
        assert flat_attr_count_param[b"since"] == b"1.0.0"
        
        # 2. DIM parameter
        flat_dim_param = flat_params[1]
        assert flat_dim_param[b"name"] == b"dim"
        assert flat_dim_param[b"type"] == b"integer"
        assert flat_dim_param[b"token"] == b"DIM"
        assert flat_dim_param[b"summary"] == b"Vector dimensions (required)"
        assert flat_dim_param[b"since"] == b"1.0.0"
        
        # 3. TYPE parameter
        flat_type_param = flat_params[2]
        assert flat_type_param[b"name"] == b"type"
        assert flat_type_param[b"type"] == b"oneof"
        assert flat_type_param[b"token"] == b"TYPE"
        assert flat_type_param[b"summary"] == b"Vector data type (required)"
        assert flat_type_param[b"since"] == b"1.0.0"

        # Check type options for FLAT (FLOAT32)
        flat_type_options = flat_type_param[b"arguments"]
        assert len(flat_type_options) == 1
        flat_float32_option = flat_type_options[0]
        assert flat_float32_option[b"name"] == b"float32"
        assert flat_float32_option[b"type"] == b"pure-token"
        assert flat_float32_option[b"token"] == b"FLOAT32"
        assert flat_float32_option[b"summary"] == b"32-bit floating point vector"
        assert flat_float32_option[b"since"] == b"1.0.0"

        # 4. DISTANCE_METRIC parameter
        flat_distance_param = flat_params[3]
        assert flat_distance_param[b"name"] == b"distance_metric"
        assert flat_distance_param[b"type"] == b"oneof"
        assert flat_distance_param[b"token"] == b"DISTANCE_METRIC"
        assert flat_distance_param[b"summary"] == b"Distance algorithm (required)"
        assert flat_distance_param[b"since"] == b"1.0.0"
        
        # Check distance metric options for FLAT (same as HNSW: L2, IP, COSINE)
        assert b"arguments" in flat_distance_param
        flat_distance_options = flat_distance_param[b"arguments"]
        assert len(flat_distance_options) == 3
        
        flat_l2_option = flat_distance_options[0]
        assert flat_l2_option[b"name"] == b"l2"
        assert flat_l2_option[b"type"] == b"pure-token"
        assert flat_l2_option[b"token"] == b"L2"
        assert flat_l2_option[b"summary"] == b"L2 (Euclidean) distance"
        assert flat_l2_option[b"since"] == b"1.0.0"
        
        flat_ip_option = flat_distance_options[1]
        assert flat_ip_option[b"name"] == b"ip"
        assert flat_ip_option[b"type"] == b"pure-token"
        assert flat_ip_option[b"token"] == b"IP"
        assert flat_ip_option[b"summary"] == b"Inner product distance"
        assert flat_ip_option[b"since"] == b"1.0.0"
        
        flat_cosine_option = flat_distance_options[2]
        assert flat_cosine_option[b"name"] == b"cosine"
        assert flat_cosine_option[b"type"] == b"pure-token"
        assert flat_cosine_option[b"token"] == b"COSINE"
        assert flat_cosine_option[b"summary"] == b"Cosine distance"
        assert flat_cosine_option[b"since"] == b"1.0.0"
        
        # 5. INITIAL_CAP parameter
        flat_initial_cap_param = flat_params[4]
        assert flat_initial_cap_param[b"name"] == b"initial_cap"
        assert flat_initial_cap_param[b"type"] == b"integer"
        assert flat_initial_cap_param[b"token"] == b"INITIAL_CAP"
        assert flat_initial_cap_param[b"summary"] == b"Initial index size (optional)"
        assert flat_initial_cap_param[b"since"] == b"1.0.0"
        assert b"optional" in flat_initial_cap_param[b"flags"]
