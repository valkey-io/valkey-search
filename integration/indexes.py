"""
Create standardized indexes
"""

from typing import Tuple, Union

# from pyparsing import abstractmethod
import valkey
from ft_info_parser import FTInfoParser
from utils import IndexingTestHelper
import logging, json
import struct


def float_to_bytes(flt: list[float]) -> bytes:
    return struct.pack(f"<{len(flt)}f", *flt)


class Field:
    def __init__(self, name: str, alias: Union[str, None]):
        self.name = name
        self.alias = alias if alias else name

    def create(self) -> list[str]:
        if self.alias:
            return [self.name, "AS", self.alias]
        else:
            return [self.name]

    # @abstractmethod
    def make_value(self, row: int, column: int, type:str) -> Union[str, bytes, float, list[float]]:
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

    def create(self):
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
            super().create()
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

    def make_value(self, row: int, column: int, type: str) -> Union[str, bytes, float, list[float]]:
        data = [float(i + row + column) for i in range(self.dim)]
        if type == "HASH":
            return float_to_bytes(data)
        else:
            return data

class Numeric(Field):
    def __init__(self, name: str, alias: Union[str, None] = None):
        super().__init__(name, alias)

    def create(self):
        return super().create() + ["NUMERIC"]

    def make_value(self, row: int, column: int, type: str) -> Union[str, bytes, float, list[float]]:
        if type == "HASH":
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

    def create(self):
        result = super().create() + ["TAG"]
        if self.separator:
            result += ["SEPARATOR", self.separator]
        return result

    def make_value(self, row: int, column: int, type: str) -> Union[str, bytes, float, list[float]]:
        return f"Tag:{row}:{column}"

class Index:
    def __init__(
        self,
        name: str,
        fields: list[Field],
        prefixes: list[str] = [],
        type: str = "HASH",
    ):
        self.name = name
        self.fields = fields
        self.prefixes = prefixes
        self.type = type

    def create(self, client: valkey.client):
        cmd = (
            [
                "FT.CREATE",
                self.name,
                "ON",
                self.type,
                "PREFIX",
                str(len(self.prefixes)),
            ]
            + self.prefixes
            + ["SCHEMA"]
        )
        for f in self.fields:
            cmd += f.create()
        print(f"Creating Index: {cmd}")
        client.execute_command(*cmd)

    def load_data(self, client: valkey.client, rows: int, start_index: int = 0):
        print("Loading data to ", client)
        for i in range(start_index, rows):
            data = self.make_data(i)
            if self.type == "HASH":
                #print("Loading ", self.keyname(i), data)
                client.hset(self.keyname(i), mapping=data)
            else:
                #print("Loading ", self.keyname(i), json.dumps(data))
                client.execute_command("JSON.SET", self.keyname(i), "$", json.dumps(data))

    def load_data_with_ttl(self, client: valkey.client, rows: int, ttl_ms: int, start_index: int = 0):
        for i in range(start_index, rows):
            data = self.make_data(i)
            key = self.keyname(i)
            client.hset(key, mapping=data)
            client.pexpire(key, ttl_ms)

    def keyname(self, row: int) -> str:
        prefix = self.prefixes[row % len(self.prefixes)] if self.prefixes else ""
        return f"{prefix}:{row:08d}"

    def make_data(self, row: int) -> dict[str, Union[str, bytes]]:
        """Make data for a particular row"""
        d: dict[str, Union[str, bytes, list[float]]] = {}
        for col, f in enumerate(self.fields):
            d[f.name] = f.make_value(row, col, self.type)
        return d

    def info(self, client: valkey.client) -> FTInfoParser:
        parser = IndexingTestHelper.get_ft_info(client, self.name)
        if parser is None:
            raise Exception(f"Failed to get FT.INFO for index {self.name}")
        return parser
    
    def backfill_complete(self, client: valkey.client) -> bool:
        return IndexingTestHelper.is_backfill_complete_on_node(client, self.name)
