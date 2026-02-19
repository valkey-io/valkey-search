"""
Create standardized indexes
"""

from typing import Tuple, Union

# from pyparsing import abstractmethod
import valkey
from ft_info_parser import FTInfoParser
import logging, json
import struct
from enum import Enum
from util import waiters

class KeyDataType(Enum):
    HASH = 1
    JSON = 2

def float_to_bytes(flt: list[float]) -> bytes:
    return struct.pack(f"<{len(flt)}f", *flt)


class Field:
    name: str
    alias: Union[str, None]
    def __init__(self, name: str, alias: Union[str, None]):
        self.name = name
        self.alias = alias if alias else name

    def create(self, data_type: KeyDataType) -> list[str]:
        name = self.name
        if data_type == KeyDataType.JSON:
            if not self.name.startswith("$."):
                name = "$." + self.name
        if self.alias:
            return [name, "AS", self.alias]
        else:
            return [name]

    # @abstractmethod
    def make_value(self, row: int, column: int, type: KeyDataType) -> Union[str, bytes, float, list[float]]:
        pass


class Vector(Field):
    def __init__(
        self,
        name: str,
        dim: int,
        alias: Union[str, None] = None,
        type: str = "HNSW",
        distance: str = "L2",
        m: Union[int, None] = None,
        ef: Union[int, None] = None,
        efc: Union[int, None] = None,
        initialcap: Union[int, None] = None,
    ):
        super().__init__(name, alias)
        self.dim = dim
        self.distance = distance
        self.type = type
        self.m = m
        self.ef = ef
        self.efc = efc
        self.initialcap = initialcap

    def create(self, data_type: KeyDataType):
        extra: list[str] = []
        if self.type == "HNSW":
            if self.m:
                extra += ["M", str(self.m)]
            if self.ef:
                extra += ["EF_RUNTIME", str(self.ef)]
            if self.efc:
                extra += ["EF_CONSTRUCTION", str(self.efc)]
            if self.initialcap:
                extra += ["INITIAL_CAP", str(self.initialcap)]
        return (
            super().create(data_type)
            + [
                "VECTOR",
                self.type,
                str(6 + len(extra)),
                "TYPE",
                "FLOAT32",
                "DIM",
                str(self.dim),
                "DISTANCE_METRIC",
                self.distance,
            ]
            + extra
        )

    def make_value(self, row: int, column: int, type: KeyDataType) -> Union[str, bytes, float, list[float]]:
        data = [float(i + row + column) for i in range(self.dim)]
        if type == KeyDataType.HASH:
            return float_to_bytes(data)
        else:
            return data

class Numeric(Field):
    def __init__(self, name: str, alias: Union[str, None] = None):
        super().__init__(name, alias)

    def create(self, data_type: KeyDataType):
        return super().create(data_type) + ["NUMERIC"]

    def make_value(self, row: int, column: int, type: KeyDataType) -> Union[str, bytes, float, list[float]]:
        if type == KeyDataType.HASH:
            return str(row + column)
        else:
            return row + column


class Tag(Field):
    def __init__(
        self,
        name: str,
        alias: Union[str, None] = None,
        separator: Union[str, None] = None,
    ):
        super().__init__(name, alias)
        self.separator = separator

    def create(self, data_type: KeyDataType):
        result = super().create(data_type) + ["TAG"]
        if self.separator:
            result += ["SEPARATOR", self.separator]
        return result

    def make_value(self, row: int, column: int, type: KeyDataType) -> Union[str, bytes, float, list[float]]:
        return f"Tag:{row}:{column}"

class Index:
    def __init__(
        self,
        name: str,
        fields: list[Field],
        prefixes: list[str] = [],
        type: KeyDataType = KeyDataType.HASH,
    ):
        self.name = name
        self.fields = fields
        self.prefixes = prefixes
        self.type = type

    def create(self, client: valkey.client, wait_for_backfill = False):
        cmd = (
            [
                "FT.CREATE",
                self.name,
                "ON",
                self.type.name,
                "PREFIX",
                str(len(self.prefixes)),
            ]
            + self.prefixes
            + ["SCHEMA"]
        )
        for f in self.fields:
            cmd += f.create(self.type)
        print(f"Creating Index: {cmd}")
        client.execute_command(*cmd)
        if wait_for_backfill:
            self.wait_for_backfill_complete(client)

    def drop(self, client: valkey.client):
        cmd = ["FT.DROPINDEX", self.name]
        print("Executing: ", cmd)
        client.execute_command(*cmd)

    def load_data(self, client: valkey.client, rows: int, start_index: int = 0):
        for i in range(start_index, rows):
            data = self.make_data(i)
            self.write_data(client, i, data)

    def write_data(self, client: valkey.client, i:int, data: dict[str, str | bytes]):
        if self.type == KeyDataType.HASH:
            client.hset(self.keyname(i), mapping=data)
        else:
            client.execute_command("JSON.SET", self.keyname(i), "$", json.dumps(data))

    def load_data_with_ttl(self, client: valkey.client, rows: int, ttl_ms: int, start_index: int = 0):
        for i in range(start_index, rows):
            data = self.make_data(i)
            self.write_data(client, i, data)
            client.pexpire(self.keyname(i), ttl_ms)

    def keyname(self, row: int) -> str:
        prefix = self.prefixes[row % len(self.prefixes)] if self.prefixes else ""
        return f"{prefix}:{self.name}:{row:08d}"

    def make_data(self, row: int) -> dict[str, Union[str, bytes]]:
        """Make data for a particular row"""
        d: dict[str, Union[str, bytes, list[float]]] = {}
        for col, f in enumerate(self.fields):
            d[f.name] = f.make_value(row, col, self.type)
        return d

    def info(self, client: valkey.client) -> FTInfoParser:
        res = client.execute_command("FT.INFO", self.name)
        return FTInfoParser(res)
    
    def backfill_complete(self, client: valkey.client) -> bool:
        res = self.info(client)
        return res.backfill_in_progress == 0
    
    def query(self, client:valkey.client, query_string: str, *args) -> dict[bytes, dict[bytes, bytes]]:
        query = ["ft.search", self.name, query_string] + list(args)
        print("Execute Query Command: ", query)
        result = client.execute_command(*query)
        print("Result is ", result)
        count = result[0]
        dict_result = {}
        if self.type == KeyDataType.HASH:
            for row in range(1, len(result)-1, 2):
                key = result[row]
                fields = result[row+1][0::2]
                values = result[row+1][1::2]
                print("Key", key, "Fields:", fields, " Values:", values)
                dict_result[key] = {fields[i]:values[i] for i in range(len(fields))}
        else:
            for row in range(1, len(result)-1, 2):
                key = result[row]
                assert result[row+1][0] == b"$"
                value = json.loads(result[row+1][1])
                dict_result[key] = value
        print("Final result", dict_result)
        return dict_result

    def aggregate(self, client:valkey.client, query_string: str, *args) -> dict[bytes, dict[bytes, bytes]]:
        query = ["ft.aggregate", self.name, query_string] + list(args)
        print("Execute Query Command: ", query)
        result = client.execute_command(*query)
        print("Result is ", result)
        count = result[0]
        array_result = []
        if self.type == KeyDataType.HASH:
            for row in result[1:]:
                print("Doing row:", row)
                d = {row[i].decode():row[i+1] for i in range(0, len(row), 2)}
                array_result += [d]
        else:
            for row in range(1, len(result)-1, 2):
                key = result[row]
                assert result[row+1][0] == b"$"
                value = json.loads(result[row+1][1])
                array_result += value
        print("Final result", array_result)
        return array_result

    def has_field(self, name: str) -> bool:
        return any(f.name == name for f in self.fields)

    def wait_for_backfill_complete(self, client: valkey.client):
        waiters.wait_for_true(
            lambda: not self.info(client).backfill_in_progress
            )

class ClusterTestUtils:
    def execute_primaries(self, command: Union[str, list[str]]) -> list:
        """Execute a command on all primary nodes in the cluster"""
        return [
            self.client_for_primary(i).execute_command(*command)
            for i in range(len(self.replication_groups))
        ]

    def config_set(self, config: str, value: str):
        """Set a config value on all primary nodes"""
        assert self.execute_primaries(["config set", config, value]) == [True] * len(
            self.replication_groups
        )

    def control_set(self, key: str, value: str):
        """Set a controlled variable on all nodes using ft._debug"""
        assert all(
            [node.client.execute_command(*["ft._debug", "CONTROLLED_VARIABLE", "set", key, value]) == b"OK" for node in self.nodes]
        )

    def check_info(self, name: str, value: Union[str, int]):
        """Check that a specific INFO SEARCH field has the expected value on all primaries"""
        results = self.execute_primaries(["INFO", "SEARCH"])
        failed = False
        for ix, r in enumerate(results):
            if r[name] != value:
                print(
                    name,
                    " Expected:",
                    value,
                    " Received:",
                    r[name],
                    " on server:",
                    ix,
                )
                failed = True
        assert not failed

    def _check_info_sum(self, name: str) -> int:
        """Sum the values of a given info field across all servers"""
        results = self.execute_primaries(["INFO", "SEARCH"])
        return sum([int(r[name]) for r in results if name in r])

    def check_info_sum(self, name: str, sum_value: int):
        """Wait for the sum of a given info field across all servers to reach expected value"""
        waiters.wait_for_equal(
            lambda: self._check_info_sum(name), 
            sum_value, 
            timeout=5
        )

    def sum_docs(self, index: Index) -> int:
        """Sum the document count for an index across all primary nodes"""
        return sum([index.info(self.client_for_primary(i)).num_docs for i in range(len(self.replication_groups))])
