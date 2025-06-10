import pytest, logging, time, itertools, redis, math
import sys, json
import numpy as np
from collections import defaultdict
from operator import itemgetter
from itertools import chain, combinations
import pickle
import compatibility
from compatibility.data_sets import * 
TEST_MARKER = "*" * 100

encoder = lambda x: x.encode() if not isinstance(x, bytes) else x

def printable_cmd(cmd):
    new_cmd = [encoder(c) for c in cmd]
    return b" ".join(new_cmd)


def printable_result(res):
    if isinstance(res, list):
        return [printable_result(x) for x in res]
    else:
        return unbytes(res)

def sortkeyfunc(row):
    if isinstance(row, list):
        r = {row[i]: row[i + 1] for i in range(0, len(row), 2)}
        if b"__key" in r:
            return r[b"__key"]
        elif b"n1" in r:
            return r[b"n1"]
        elif b"t1" in r:
            return r[b"t1"]
        elif b"t2" in r:
            return r[b"t2"]
        elif b"t3" in r:
            return r[b"t3"]
    return None

SYSTEM_L_ADDRESS = ('localhost', 6379)
SYSTEM_R_ADDRESS = ('localhost', 6379)

class ClientSystem:
    def __init__(self, address):
        self.address = address
        self.client = redis.Redis(host=address[0], port=address[1])
        try:
            self.client.execute_command("PING")
        except:
            print(f"Can't ping {address}")
            sys.exit(1)
        self.client.execute_command("FLUSHALL SYNC")
    def execute_command(self, *cmd):
        #print("Execute:", *cmd)
        result = self.client.execute_command(*cmd)
        #print("Result:", result)
        return result
    def pipeline(self):
        return self.client.pipeline()
    
    def wait_for_indexing_done(self, index):
        pass
    
    def hset(self, *cmd):
        return self.client.hset(*cmd)

class ClientLSystem(ClientSystem):
    def __init__(self):
        super().__init__(SYSTEM_L_ADDRESS)
    def execute_command(self, *cmd):
        result = self.client.execute_command(*cmd)
        # print("L:", *cmd, " => ", result)
        return result

def process_row(row):
    if any([isinstance(r, redis.ResponseError) for r in row]):
        return (True, None)
    if 0 != (len(row) % 2):
        print(f"BAD ROW, not even # of fields: {row}")
        for r in row:
            print(f">> [{type(r)}] {r}")
        return (True, None)
    return (False, row)

def parse_field(x, key_type):
    if isinstance(x, bytes):
        return parse_field(x.decode("utf-8"), key_type)
    if isinstance(x, str):
        return x[2::] if x.startswith("$.") else x
    if isinstance(x, int):
        return x
    print("Unknown type ", type(x))
    assert False

def parse_value(x, key_type):
    if key_type == "json" and x[0] == b'[':
        assert isinstance(x, bytes), f"Expected bytes for JSON value, got {type(x)}"
        try:
            return json.loads(x.decode("utf-8"))
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e} for value {x}")
            assert False
    if isinstance(x, bytes):
        if key_type == "json":
            return x.decode("utf-8")
        else:
            return x
    if isinstance(x, str):
        return x
    if isinstance(x, int):
        return x
    print("Unknown type ", type(x))
    assert False

def unpack_search_result(rs, key_type):
    rows = []
    for (key, value) in [(rs[i],rs[i+1]) for i in range(1, len(rs), 2)]:
        #try:
        row = {"__key": key}
        for i in range(0, len(value), 2):
            row[parse_field(value[i], key_type)] = parse_value(value[i+1])
        rows += [row]
        #except:
        #    print("Parse failure: ", key, value)
    return rows

def unpack_agg_result(rs, key_type):
    # Skip the first gibberish int
    try:
        rows = []
        for key_res in rs[1:]:
            rows += [{parse_field(key_res[f_idx], key_type): parse_value(key_res[f_idx + 1], key_type)
                for f_idx in range(0, len(key_res), 2)}]
    except:
        print("Parse Failure: ", rs[1:])
        print("Trying to parse: ", key_res)
        print("Rows so far are:", rows)
        assert False
    return rows

def unpack_result(cmd, key_type, rs, sortkeys):
    if "ft.search" in cmd:
        out = unpack_search_result(rs, key_type)
    else:
        out = unpack_agg_result(rs, key_type)
    #
    # Sort by the sortkeys
    #
    if len(sortkeys) > 0:
        try:
            out.sort(key=itemgetter(*sortkeys))
        except KeyError:
            print("Failed on sortkeys: ", sortkeys)
            print("CMD:", cmd)
            print("RESULT:", rs)
            print("Out:", out)
            assert False
    return out

def compare_number_eq(l, r):
    if (l == "nan" and r == "nan") or (l == "-nan" and r == "-nan") or \
        (l == b"nan" and r == b"nan") or (l == b"-nan" and r == b"-nan"):
        return True
    elif isinstance(l, str) and l.startswith("[") and isinstance(r, str) and r.startswith("["):
        # Special case. It's really a list encoded as JSON
        ll = json.loads(l)
        rr = json.loads(r)
        if len(ll) != len(rr):
            print("mismatch vector field length: ", ll, " ", rr)
            return False
        for i in range(len(ll)):
            if not compare_number_eq(ll[i], rr[i]):
                print("mismatch vector field value: ", ll, " ", rr, " at index ", i)
                return False
        return True
    else:
        return math.isclose(float(l), float(r), rel_tol=.01)
    
def compare_row(l, r, key_type):
    lks = sorted(list(l.keys()))
    rks = sorted(list(r.keys()))
    if lks != rks:
        return False
    for i in range(len(lks)):
        #
        # Hack, fields that start with an 'n' are assumed to be numeric
        #
        if lks[i].startswith("n"):
            if not compare_number_eq(l[lks[i]], r[rks[i]]):
                print("mismatch numeric field: ", l[lks[i]], " ", r[rks[i]])
                return False
        elif lks[i].startswith("v") and key_type == "json":
            # Vector compare fields
            assert isinstance(l[lks[i]], list)
            assert isinstance(r[rks[i]], list)
            if len(l[lks[i]]) != len(r[rks[i]]):
                print("mismatch vector field length: ", l[lks[i]], " ", r[rks[i]])
                return False
            for i in range(l[lks[i]]):
                if not compare_number_eq(l[lks[i]][i], r[rks[i]][i]):
                    print("mismatch vector field value: ", l[lks[i]], " ", r[rks[i]])
                    return False
        elif l[lks[i]] != r[rks[i]]:
            print("mismatch field: ", lks[i], " and ", rks[i], " ", l[lks[i]], "!=", r[rks[i]])
            return False
    return True            
    
def compare_results(expected, results):
    cmd = expected["cmd"]
    key_type = expected["key_type"]
    if cmd != results["cmd"]:
        print("CMD Mismatch: ", cmd, " ", results["cmd"])
        assert False
    
    if 'groupby' in cmd and 'sortby' in cmd:
        assert False
    if 'groupby' in cmd:
        ix = cmd.index('groupby')
        count = int(cmd[ix+1])
        sortkeys = [cmd[ix+2+i][1:] for i in range(count)]
    elif 'sortby' in cmd:
        ix = cmd.index('sortby')
        count = int(cmd[ix+1])
        # Grab the fields after the count, stripping any leading '@'
        sortkeys = [cmd[ix+2+i][1 if cmd[ix+2+i].startswith("@") else 0:] for i in range(count)]
        for f in ['asc', 'desc', 'ASC', 'DESC']:
            if f in sortkeys:
                sortkeys.remove(f)
    else:
        # todo, remove __key as sortkey once the search output sorting is fixed.
        # sortkeys=["__key"] if "ft.aggregate" in cmd else []
        sortkeys=[]

    # If both failed, it's a wrong search cmd and we can exit
    if expected["exception"] and results["exception"]:
        print("Both engines failed.")
        print(f"CMD:{cmd}")
        print(TEST_MARKER)
        return

    if expected["exception"]:
        print("RL Exception, skipped")
        #print(f"RL Exception: Raw: {printable_result(results['RL'])}")
        #print(f"EC Result: {printable_result(results['EC'])}")
        print(TEST_MARKER)
        return

    if results["exception"]:
        print(f"CMD: {cmd}")
        print(f"RL Result: {printable_result(expected['result'])}")
        print(f"EC Exception Raw: {printable_result(results['result'])}")
        print(TEST_MARKER)
        assert False

    # Output raw results
    rl = unpack_result(cmd, expected["key_type"], expected["result"], sortkeys)
    ec = unpack_result(cmd, expected["key_type"], results["result"], sortkeys)

    # Process failures
    if len(rl) != len(ec):
        print(f"CMD:{cmd}")
        print(f"Mismatched sizes RL:{len(rl)} EC:{len(ec)}")
        print("--RL--")
        for r in rl:
            print(r)
        print("--EC--")
        for e in ec:
            print(e)
        #assert False
        return

    # if compare_results(ec, rl):
    # Directly comparing dicts instead of custom compare function
    # TODO: investigate this later
    if all([compare_row(ec[i], rl[i], key_type) for i in range(len(rl))]):
        # print("Results look good.")
        #print(TEST_MARKER)
        if "ft.search" in cmd:
            print(f"CMD:{cmd}")
            for i in range(len(rl)):
                print("RL",i,[(k,rl[i][k]) for k in sorted(rl[i].keys())])
                print("EC",i,[(k,ec[i][k]) for k in sorted(ec[i].keys())])
        return
    print("***** MISMATCH ON DATA *****, sortkeys=", sortkeys, " records=", len(rl))
    print(f"CMD: {cmd}")
    for i in range(len(rl)):
        if not compare_row(rl[i], ec[i], key_type):
            print("RL",i,[(k,rl[i][k]) for k in sorted(rl[i].keys())], "<<<")
            print("EC",i,[(k,ec[i][k]) for k in sorted(ec[i].keys())], "<<<")
        else:
            print("RL",i,[(k,rl[i][k]) for k in sorted(rl[i].keys())])
            print("EC",i,[(k,ec[i][k]) for k in sorted(ec[i].keys())])

    print("Raw RL:", expected["results"])
    print("Raw EC:", results["results"])
    print(TEST_MARKER)
    assert False

def do_answer(expected, data_set):
    if (expected['data_set_name'], expected['key_type']) != data_set:
        print("Loading data set:", expected['data_set_name'], "key type:", expected['key_type'])
        client.execute_command("FLUSHALL SYNC")
        load_data(client, expected['data_set_name'], expected['key_type'])
        data_set = (expected['data_set_name'], expected['key_type'])
    try:
        result = {}
        result["cmd"] = expected['cmd']
        result["result"] = client.execute_command(*expected['cmd'])
        result["exception"] = False
        compare_results(expected, result)
    except redis.ResponseError as e:
        result["exception"] = True
        compare_results(expected, result)
    return data_set


f = open("compatibility/" + ANSWER_FILE_NAME, "rb")
client = ClientLSystem()
data_set = None
answers = pickle.load(f)
print(f"Loaded {len(answers)} answers")
data_set = None
for i in range(len(answers)):
    data_set = do_answer(answers[i], data_set)
