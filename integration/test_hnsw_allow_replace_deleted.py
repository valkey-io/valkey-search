import struct
import time
import pytest

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestHNSWAllowReplaceDeleted(ValkeySearchTestCaseBase):
    """
    Verify HNSW allow_replace_deleted works correctly with both yes/no settings. 
    """

    @pytest.mark.parametrize("allow_replace_deleted", ["yes", "no"])
    def test_allow_replace_deleted_after_rdb_reload(self, allow_replace_deleted):
        """
        After RDB reload, inserting new vectors must succeed without inc_id_
        collisions regardless of allow_replace_deleted setting.
        """
        client: Valkey = self.server.get_new_client()

        client.config_set("search.hnsw-allow-replace-deleted",
                          allow_replace_deleted)

        index_name = "test_rdb_idx"
        client.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "rdoc:",
            "SCHEMA",
            "vector", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", "4",
            "DISTANCE_METRIC", "L2"
        )

        # Add 10 vectors (labels 0-9)
        num_vecs = 10
        for i in range(num_vecs):
            vec = struct.pack('<4f', *[float(i) + 0.1 * d for d in range(4)])
            client.hset(f"rdoc:{i}", mapping={"vector": vec})

        time.sleep(2)
        info = client.info("SEARCH")
        assert int(info.get("search_num_hnsw_nodes", 0)) >= num_vecs

        # Delete highest-labeled vectors (8 and 9)
        num_deleted = 2
        for i in range(num_vecs - num_deleted, num_vecs):
            client.delete(f"rdoc:{i}")

        time.sleep(2)
        surviving = num_vecs - num_deleted

        # RDB save + reload
        client.execute_command("BGSAVE")
        time.sleep(3)

        exc_before = int(info.get("search_hnsw_add_exceptions_count", 0))

        try:
            client.execute_command("DEBUG", "RELOAD")
        except Exception:
            pass
        time.sleep(3)

        # Add 5 new vectors after reload
        num_new = 5
        for i in range(num_new):
            vec = struct.pack('<4f', *[100.0 + i + 0.1 * d for d in range(4)])
            client.hset(f"rdoc:new{i}", mapping={"vector": vec})

        time.sleep(3)

        info_after = client.info("SEARCH")
        exc_after = int(info_after.get("search_hnsw_add_exceptions_count", 0))

        num_docs_str = None
        try:
            ft_info = client.execute_command("FT.INFO", index_name)
            for j in range(0, len(ft_info) - 1, 2):
                k = ft_info[j]
                if isinstance(k, bytes):
                    k = k.decode()
                if k == "num_docs":
                    v = ft_info[j + 1]
                    num_docs_str = v.decode() if isinstance(v, bytes) else str(v)
        except Exception:
            pass

        num_docs = int(num_docs_str) if num_docs_str else 0
        expected_total = surviving + num_new  # 8 + 5 = 13

        assert exc_after - exc_before == 0, \
            (f"[allow_replace_deleted={allow_replace_deleted}] "
             f"Expected 0 add exceptions after RDB reload, "
             f"got {exc_after - exc_before}")

        assert num_docs == expected_total, \
            (f"[allow_replace_deleted={allow_replace_deleted}] "
             f"Expected {expected_total} docs ({surviving} surviving + "
             f"{num_new} new), got {num_docs}")

        # Verify all vectors are searchable via KNN
        query_vec = struct.pack('<4f', *[50.0, 50.1, 50.2, 50.3])
        search_result = client.execute_command(
            "FT.SEARCH", index_name,
            f"*=>[KNN {expected_total} @vector $q]",
            "PARAMS", "2", "q", query_vec,
        )
        search_count = search_result[0]
        assert search_count == expected_total, \
            (f"[allow_replace_deleted={allow_replace_deleted}] "
             f"KNN search returned {search_count}, expected {expected_total}")

        # Cleanup
        client.execute_command("FT.DROPINDEX", index_name)
