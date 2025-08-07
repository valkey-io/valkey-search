'''
Create standardized indexes
'''
from typing import Tuple
# from pyparsing import abstractmethod
import valkey
import logging
import struct

def float_to_bytes(flt: list[float]) -> bytes:
    return struct.pack(f"<{len(flt)}f", *flt)

class Field:
    def __init__(self, name:str, alias: str|None):
        self.name = name
        self.alias = alias if alias else name

    def create(self) -> list[str]:
        if self.alias:
            return [self.name, "AS", self.alias]
        else:
            return [self.name]
        
    # @abstractmethod
    def make_value(self, row:int, column:int) -> str|bytes:
        pass

class Vector(Field):
    def __init__(self, name: str, dim:int, alias: str|None=None,
                 type:str = "HNSW", distance:str="L2", 
                 m:int|None=None, ef:int|None=None, efc:int|None=None, initialcap:int|None=None):
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
        return super().create() + ["VECTOR", self.type, str(6+len(extra)), "TYPE", "FLOAT32", "DIM", str(self.dim), "DISTANCE_METRIC", self.distance] + extra

    def make_value(self, row:int, column:int) -> str|bytes:
        data = [float(i+row+column) for i in range(self.dim)]
        return float_to_bytes(data)

class Numeric(Field):
    def __init__(self, name: str, alias: str|None = None):
        super().__init__(name, alias)
    
    def create(self):
        return super().create() + ["NUMERIC"]
    
    def make_value(self, row:int, column:int) -> str|bytes:  
        return str(row + column)
    
class Tag(Field):
    def __init__(self, name: str, alias: str|None = None, separator:str|None = None):
        super().__init__(name, alias)
        self.separator = separator

    def create(self):
        return super().create() + ["SEPARATOR", self.separator] if self.separator else [] 
    
    def make_value(self, row:int, column:int) -> str|bytes:
        return f"Tag:{row}:{column}"

class Index:
    def __init__(self, name: str, fields:list[Field], prefixes:list[str] = [], type:str = "HASH"):
        self.name = name
        self.fields = fields
        self.prefixes = prefixes
        self.type = type

    def create(self, client: valkey.client):
        cmd = ["FT.CREATE", self.name, "ON", self.type, "PREFIX", str(len(self.prefixes))] + self.prefixes + ["SCHEMA"]
        for f in self.fields:
            cmd += f.create()
        print(f"Creating Index: {cmd}")
        client.execute_command(*cmd)

    def load_data(self, client: valkey.client, rows:int):
        for i in range(0, rows):
            data = self.make_data(i)
            client.hset(self.keyname(i), mapping=data)

    def keyname(self, row:int) -> str:
        prefix = self.prefixes[row % len(self.prefixes)] if self.prefixes else ""
        return f"{prefix}:{row:08d}"

    def make_data(self, row:int) -> dict[str, str|bytes]:
        ''' Make data for a particular row'''
        d: dict[str, str|bytes] = {}
        for (col, f) in enumerate(self.fields):
            d[f.name] = f.make_value(row, col)
        return d
    
    def info(self, client:valkey.client) -> dict[str,str]:
        res = client.execute_command("FT.INFO", self.name)
        print("Got info result of ", res)
        return {res[i]: res[i+1] for i in range(len(res), 2)}
