"""
FT.INFO must report the vector data_type that was used at FT.CREATE time.

FLOAT16 and BFLOAT16 share a byte width (2), so an info-reporting bug that
collapses them by size — rather than dispatching on the type enum — would
silently mislabel BF16 indexes as FP16 (or vice versa) without any other
visible symptom. This test pins the contract for all three supported
storage types across both HNSW and FLAT.
"""

import pytest
from valkey import Valkey

from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from ft_info_parser import FTInfoParser
from indexes import Index, Vector


DIM = 4


def _make_index(name: str, algo: str, data_type: str) -> Index:
    fields = [Vector("v", DIM, type=algo, data_type=data_type)]
    return Index(name, fields)


class TestFTInfoVectorDataType(ValkeySearchTestCaseBase):

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    @pytest.mark.parametrize("data_type", ["FLOAT32", "FLOAT16", "BFLOAT16"])
    def test_ft_info_reports_data_type(self, algo: str, data_type: str):
        client: Valkey = self.server.get_new_client()
        index_name = f"idx_{algo}_{data_type}".lower()

        index = _make_index(index_name, algo, data_type)
        index.create(client)

        info = client.execute_command("FT.INFO", index_name)
        parser = FTInfoParser(info)

        attr = parser.get_attribute_by_name("v")
        assert attr is not None, f"vector attribute 'v' missing from FT.INFO for {index_name}"
        assert attr.get("type") == "VECTOR", \
            f"attribute 'v' type {attr.get('type')!r} != 'VECTOR'"

        index_block = attr.get("index")
        assert isinstance(index_block, dict), \
            f"FT.INFO attribute['index'] not parsed as dict: {index_block!r}"

        reported = index_block.get("data_type")
        assert reported == data_type, (
            f"FT.INFO reported data_type={reported!r} for index created with "
            f"TYPE {data_type} ({algo}); expected {data_type!r}"
        )

        algorithm = index_block.get("algorithm", {})
        assert isinstance(algorithm, dict), \
            f"algorithm block not parsed as dict: {algorithm!r}"
        assert algorithm.get("name") == algo, (
            f"FT.INFO reported algorithm={algorithm.get('name')!r} for index "
            f"created with {algo}"
        )
