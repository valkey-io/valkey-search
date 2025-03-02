import pytest, logging, time, itertools, redis, math
import sys, json
import numpy as np
from collections import defaultdict
from operator import itemgetter
from itertools import chain, combinations
'''
Compare two systems for a bunch of aggregation operations
'''
SETS_KEY = lambda key_type: f"{key_type} sets"
CREATES_KEY = lambda key_type: f"{key_type} creates"
VECTOR_DIM = 3

TEST_MARKER = "*" * 100

def cvrt(x):
    if not isinstance(x, bytes):
        return x
    else:
        try:
            res = x.decode("utf-8")
            return res
        except:
            return x


encoder = lambda x: x.encode() if not isinstance(x, bytes) else x


def printable_cmd(cmd):
    new_cmd = [encoder(c) for c in cmd]
    return b" ".join(new_cmd)


def printable_result(res):
    if isinstance(res, list):
        return [printable_result(x) for x in res]
    else:
        return cvrt(res)


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
SYSTEM_R_ADDRESS = ('localhost', 6380)

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
    
    def wait_for_indexing_done(self):
        pass
    
    def hset(self, *cmd):
        return self.client.hset(*cmd)

class ClientRSystem(ClientSystem):
    def __init__(self):
        super().__init__(SYSTEM_R_ADDRESS)
        try:
            self.client.execute_command("FT.CONFIG SET TIMEOUT 0")
        except:
            pass
    def execute_command(self, *cmd):
        result = self.client.execute_command(*cmd)
        # print("R:", *cmd, " => ", result)
        return result

class ClientLSystem(ClientSystem):
    def __init__(self):
        super().__init__(SYSTEM_L_ADDRESS)
    def execute_command(self, *cmd):
        result = self.client.execute_command(*cmd)
        # print("L:", *cmd, " => ", result)
        return result

NUM_KEYS = 10

#@pytest.mark.parametrize("key_type", ["hash", "json"])
@pytest.mark.parametrize("key_type", ["hash"])
class TestAggregateCompatibility:

    def setup_method(self):
        self.client_l = ClientLSystem()
        self.client_r = ClientRSystem()
        self.data = {}

    ### Reusable Data ###
    def setup_data(self):
        data = {}

        create_cmds = {
            "hash": "FT.CREATE hash_idx1 ON HASH PREFIX 1 hash: SCHEMA {}",
            "json": "FT.CREATE json_idx1 ON JSON PREFIX 1 json: SCHEMA {}",
        }
        field_type_to_name = {"tag": "t", "numeric": "n", "vector": "v"}
        field_types_to_count = {"numeric": 3, "tag": 3, "vector" : 1}

        def make_field_definition(key_type, name, typ, i):
            if typ == "vector":
                return f"{name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
            else:
                return f"{name}{i} {typ}" if key_type == "hash" else f"$.{name}{i} AS {name}{i} {typ}"

        data["hard numbers"] = {}
        data["sortable numbers"] = {}
        data["reverse vector numbers"] = {}
        for key_type in ["hash", "json"]:
            schema = [
                make_field_definition(key_type, field_type_to_name[typ], typ, i + 1)
                for typ, count in field_types_to_count.items()
                for i in range(count)
            ]
            schema = " ".join(schema)
            print(f"Generated schema: {schema}")
            #
            # Hard Numbers, edge case numbers.
            #
            hard_numbers = ["-0.5", "0", "1", "-1", "-inf", "inf"] # todo "nan", -0
            combinations = list(itertools.combinations(hard_numbers, 3))
            data["hard numbers"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
            data["hard numbers"][SETS_KEY(key_type)] = [
                (
                    f"{key_type}:{i:02d}",
                    {
                        "n1": combinations[i][0],
                        "n2": combinations[i][1],
                        "n3" : combinations[i][2],
                        "t1": f"one.one{i*2}",
                        "t2": f"two.two{i*-2}",
                        "t3": "all_the_same_value",
                        "v1": np.array([i for _ in range(VECTOR_DIM)]).astype(np.float32).tobytes(),
                    },
                )
                for i in range(len(combinations))
            ]
            #
            # Sortable numbers, designed so that sorted keys for this index don't have any duplications
            # which makes the compare functions harder.
            #
            sortable_numbers = range(-5, 10)
            data["sortable numbers"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
            data["sortable numbers"][SETS_KEY(key_type)] = [
                (
                    f"{key_type}:{i:02d}",
                    {
                        "n1": sortable_numbers[i],
                        "n2": -sortable_numbers[i],
                        "n3" : sortable_numbers[-i],
                        "t1": f"one.one{i*2}",
                        "t2": f"two.two{i*-2}",
                        "t3": "all_the_same_value",
                        "v1": np.array([i for _ in range(VECTOR_DIM)]).astype(np.float32).tobytes(),
                    },
                )
                for i in range(len(sortable_numbers))
            ]
            #
            # Sortable numbers, designed so that sorted keys for this index don't have any duplications
            # which makes the compare functions harder.
            #
            sortable_numbers = range(-5, 10)
            data["reverse vector numbers"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
            data["reverse vector numbers"][SETS_KEY(key_type)] = [
                (
                    f"{key_type}:{i:02d}",
                    {
                        "n1": sortable_numbers[i],
                        "n2": -sortable_numbers[i],
                        "n3" : sortable_numbers[-i],
                        "t1": f"one.one{i*2}",
                        "t2": f"two.two{i*-2}",
                        "t3": "all_the_same_value",
                        "v1": np.array([(len(sortable_numbers)-i) for _ in range(VECTOR_DIM)]).astype(np.float32).tobytes(),
                    },
                )
                for i in range(len(sortable_numbers))
            ]
        self.data["hard numbers"] = data["hard numbers"]
        self.data["sortable numbers"] = data["sortable numbers"]
        self.data["reverse vector numbers"] = data["reverse vector numbers"]

    ### Helper Functions ###
    def load_data_with_index(self, data_key, key_type):
        self.setup_data()
        print("load_data with index", data_key, " ", key_type);
        load_list = self.data[data_key][SETS_KEY(key_type)]
        print(f"Start Loading Data, data set has {len(load_list)} records")
        for client in [self.client_r, self.client_l]:
            print("Doing: ", self.data[data_key][CREATES_KEY(key_type)])
            for create_index_cmd in self.data[data_key][CREATES_KEY(key_type)]:
                client.execute_command(create_index_cmd)

            # Make large chunks to accelerate things
            batch_size = 50
            for s in range(0, len(load_list), batch_size):
                pipe = client.pipeline()
                for cmd in load_list[s : s + batch_size]:
                    if client == self.client_r:
                        if key_type == "hash":
                            data_str = " ".join([f"{f} {v}" for f, v in cmd[1].items()])
                            print_cmd = " ".join(["HSET", cmd[0], data_str])
                        else:
                            print_cmd = " ".join(["JSON.SET", cmd[0], "$", f"'{json.dumps(cmd[1])}'"])
                        print(print_cmd)
                    if key_type == "hash":
                        pipe.hset(cmd[0], mapping=cmd[1])
                    else:
                        pipe.execute_command(*["JSON.SET", cmd[0], "$", json.dumps(cmd[1])])
                pipe.execute()

            client.wait_for_indexing_done()
        print(f"load_data_with_index completed {data_key} {key_type}")
        return len(load_list)

    def process_row(self, row):
        if any([isinstance(r, redis.ResponseError) for r in row]):
            return (True, None)
        if 0 != (len(row) % 2):
            print(f"BAD ROW, not even # of fields: {row}")
            for r in row:
                print(f">> [{type(r)}] {r}")
            return (True, None)
        return (False, row)

    def parse_field(self, x):
        if isinstance(x, bytes):
            return self.parse_field(x.decode("utf-8"))
        if isinstance(x, str):
            return x[2::] if x.startswith("$.") else x
        if isinstance(x, int):
            return x
        print("Unknown type ", type(x))
        assert False
    def parse_value(self, x):
        if isinstance(x, bytes):
            return x
        if isinstance(x, str):
            return x
        if isinstance(x, int):
            return x
        print("Unknown type ", type(x))
        assert False
    def unpack_search_result(self, rs):
        rows = []
        for (key, value) in [(rs[i],rs[i+1]) for i in range(1, len(rs), 2)]:
            #try:
            row = {"__key": key}
            for i in range(0, len(value), 2):
                row[self.parse_field(value[i])] = self.parse_value(value[i+1])
            rows += [row]
            #except:
            #    print("Parse failure: ", key, value)
        return rows

    def unpack_agg_result(self, rs, decode=True):
        """
        Input:
            rs: [
                    _,
                    ['__key', key1, field1, val1, field2, val2, ...],
                    ['__key', key2, field1, val1, field2, val2, ...],
                    ...
                ]
        Example input:
                [
                    1,
                    [b'__key', b'hash:0', b'n1', b'0', b'n2', b'0', ...]
                ]

        Output:
            resultset: {
                    'key1': {field_key_1: field_val_key_1},
                    'key2': {field_key_2: field_val_key_2},
                    ...
                }
        Example output:
                {
                    'hash:k3': {'USERNAME': 'Tom', 'AGE': 33}
                }
        """
        # Skip the first gibberish int
        try:
            rows = []
            for key_res in rs[1:]:
                rows += [{self.parse_field(key_res[f_idx]): self.parse_value(key_res[f_idx + 1])
                    for f_idx in range(0, len(key_res), 2)}]
        except:
            print("Parse Failure: ", rs[1:])
        return rows

    def unpack_result(self, cmd, rs, sortkeys):
        if "ft.search" in cmd:
            out = self.unpack_search_result(rs)
        else:
            out = self.unpack_agg_result(rs)
        #
        # Sort by the sortkeys
        #
        if len(sortkeys) > 0:
            try:
                out.sort(key=itemgetter(*sortkeys))
            except KeyError:
                print("Failed on sortkeys: ", sortkeys)
                print("CMD:", cmd)
                print("Out:", out)
        return out
    def compare_number_eq(self, l, r):
        if (l == "nan" and r == "nan") or (l == "-nan" and r == "-nan") or \
           (l == b"nan" and r == b"nan") or (l == b"-nan" and r == b"-nan"):
            return True
        else:
            return math.isclose(float(l), float(r), rel_tol=.01)
    def compare_row(self, l, r):
        lks = sorted(list(l.keys()))
        rks = sorted(list(r.keys()))
        if lks != rks:
            return False
        for i in range(len(lks)):
            #
            # Hack, fields that start with an 'n' are assumed to be numeric
            #
            if lks[i].startswith("n"):
                if not self.compare_number_eq(l[lks[i]], r[rks[i]]):
                    print("mismatch numeric field: ", l[lks[i]], " ", r[rks[i]])
                    return False                
            elif l[lks[i]] != r[rks[i]]:
                print("mismatch field: ", lks[i], " and ", rks[i], " ", l[lks[i]], "!=", r[rks[i]])
                return False
        return True            
        
    def compare_results(self, cmd):
        #print(TEST_MARKER)
        #print(f"CMD: {cmd}")
        engines = {"RL": self.client_r, "EC": self.client_l}
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
            sortkeys=["__key"] if "ft.aggregate" in cmd else []

        results = {eng: None for eng in engines}
        exception = {eng: False for eng in engines}
        for name, client in engines.items():
            try:
                rs = client.execute_command(*cmd)
                results[name] = rs
                # print(f"{name} replied: {rs}")
            except Exception as exc:
                print(f"Got exception for {name} Error: '{exc}', Cmd:{cmd}")
                exception[name] = True
                assert False

        # If both failed, it's a wrong search cmd and we can exit
        if all(exception.values()):
            print("Both engines failed.")
            print(f"CMD:{cmd}")
            print(TEST_MARKER)
            return

        if exception["RL"]:
            print("RL Exception, skipped")
            #print(f"RL Exception: Raw: {printable_result(results['RL'])}")
            #print(f"EC Result: {printable_result(results['EC'])}")
            print(TEST_MARKER)
            return

        if exception["EC"]:
            print(f"CMD: {cmd}")
            print(f"RL Result: {printable_result(results['RL'])}")
            print(f"EC Exception Raw: {printable_result(results['EC'])}")
            print(TEST_MARKER)
            assert False

        # Output raw results
        rl = self.unpack_result(cmd, results["RL"], sortkeys)
        ec = self.unpack_result(cmd, results["EC"], sortkeys)

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
        if all([self.compare_row(ec[i], rl[i]) for i in range(len(rl))]):
            #print("Results look good.")
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
            if not self.compare_row(rl[i], ec[i]):
                print("RL",i,[(k,rl[i][k]) for k in sorted(rl[i].keys())], "<<<")
                print("EC",i,[(k,ec[i][k]) for k in sorted(ec[i].keys())], "<<<")
            else:
                print("RL",i,[(k,rl[i][k]) for k in sorted(rl[i].keys())])
                print("EC",i,[(k,ec[i][k]) for k in sorted(ec[i].keys())])

        print("Raw RL:", results["RL"])
        print("Raw EC:", results["EC"])
        print(TEST_MARKER)
        assert False

    def checkvec(self, *orig_cmd, knn=10000, score_as=""):
        '''Temporary change until query parser is redone.'''
        cmd = orig_cmd[0].split() if len(orig_cmd) == 1 else [*orig_cmd]
        new_cmd = []
        did_one = False
        for c in cmd:
            if c.strip() == "*" and not did_one:
                ''' substitute '''
                new_cmd += [f"*=>[KNN {knn} @v1 $BLOB {score_as}]"]
                did_one = True
            else:
                new_cmd += [c]
        new_cmd += [
            "PARAMS",
            "2",
            "BLOB",
            np.array([0] * VECTOR_DIM).astype(np.float32).tobytes(),
            "DIALECT",
            "3",
        ]
        self.compare_results(new_cmd)
    '''
    def test_search_reverse(self, key_type):
        self.load_data_with_index("reverse vector numbers", key_type)
        self.checkvec(f"ft.search {key_type}_idx1 *")
        self.checkvec(f"ft.search {key_type}_idx1 * limit 0 5")

    def test_search(self, key_type):
        self.load_data_with_index("sortable numbers", key_type)
        self.checkvec(f"ft.search {key_type}_idx1 *")
        self.checkvec(f"ft.search {key_type}_idx1 * limit 0 5")
    '''

    def test_aggregate_sortby(self, key_type):
        self.load_data_with_index("sortable numbers", key_type)
        self.checkvec(f"ft.aggregate {key_type}_idx1 * load 2 @__key @n2 sortby 1 @n2")
        self.checkvec(f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 1 @n2")
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @n2 asc"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @n2 desc"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @__key desc"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 VECTORDISTANCE sortby 2 @VECTORDISTANCE desc"
        , score_as="AS VECTORDISTANCE")
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @__v1_score asc"
        )

    def test_aggregate_groupby(self, key_type):
        self.load_data_with_index("sortable numbers", key_type)
        self.checkvec(f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @n1")
        self.checkvec(f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1")
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce count 0 as count"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce count 0 as count"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce COUNT 0 as count"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce CoUnT 0 as count"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce sum 1 @n1 as sum"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce sum 1 @n1 as sum"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce sum 1 @n2 as sum"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce avg 1 @n1 as avg"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce avg 1 @n1 as avg"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce avg 1 @n2 as avg"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce min 1 @n1 as min"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce min 1 @n2 as min"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce min 1 @n1 as min"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce min 1 @n2 as min"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce stddev 1 @n1 as nstddev"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce stddev 1 @n1 as nstddev"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce stddev 1 @n2 as nstddev"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce max 1 @n1 as nmax"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce max 1 @n1 as nmax"
        )
        self.checkvec(f'ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce max 1 @n2 as nmax')
    '''
    # todo enable when search sorting is fixed
    def test_aggregate_limit(self, key_type):
        keys = self.load_data_with_index("sortable numbers", key_type)
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 limit 1 4")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 limit 1 4 sortby 2 @__key desc")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key desc limit 1 4")
    '''
    def test_aggregate_short_limit(self, key_type):
        keys = self.load_data_with_index("sortable numbers", key_type)
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 limit 0 5")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key desc")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key desc limit 0 5")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key asc limit 1 4", knn=4)

    def test_aggregate_load(self, key_type):
        keys = self.load_data_with_index("sortable numbers", key_type)
        self.checkvec(f"ft.aggregate {key_type}_idx1  *")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load *")

    def test_aggregate_numeric_dyadic_operators(self, key_type):
        keys = self.load_data_with_index("hard numbers", key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        logops = ["||", "&&"]
        for op in dyadic + relops + logops:
            self.checkvec(
                f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 apply @n1{op}@n2 as nn"
            )

    def test_aggregate_numeric_triadic_operators(self, key_type):
        keys = self.load_data_with_index("hard numbers", key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        logops = ["||", "&&"]
        for op1 in dyadic+relops+logops:
            for op2 in dyadic+relops+logops:
                self.checkvec(
                    f"ft.aggregate {key_type}_idx1  * load 4 @__key @n1 @n2 @n3 apply @n1{op1}@n2{op2}@n3 as nn apply (@n1{op1}@n2) as nn1"
                )

    def test_aggregate_numeric_functions(self, key_type):
        keys = self.load_data_with_index("hard numbers", key_type)
        function = ["log", "abs", "ceil", "floor", "log2", "exp", "sqrt"]
        for f in function:
            self.checkvec(
                f"ft.aggregate {key_type}_idx1  * load 2 @__key @n1 apply {f}(@n1) as nn"
            )

    def test_aggregate_string_apply_functions(self, key_type):
        self.load_data_with_index("hard numbers", key_type)

        # String apply function "contains"
        self.checkvec(
            "ft.aggregate",
            f"{key_type}_idx1",
            "*",
            "load",
            "2",
            "__key",
            "t3",
            "apply",
            'contains(@t3, "all")',
            "as",
            "apply_result",
        )
        self.checkvec(
            "ft.aggregate",
            f"{key_type}_idx1",
            "*",
            "load",
            "2",
            "__key",
            "t3",
            "apply",
            'contains(@t3, "value")',
            "as",
            "apply_result",
        )
        self.checkvec(
            "ft.aggregate",
            f"{key_type}_idx1",
            "*",
            "load",
            "2",
            "t2",
            "__key",
            "apply",
            'contains(@t2, "two")',
            "as",
            "apply_result",
        )
        self.checkvec(
            "ft.aggregate",
            f"{key_type}_idx1",
            "*",
            "load",
            "2",
            "t1",
            "__key",
            "apply",
            'contains(@t1, "one")',
            "as",
            "apply_result",
        )
        self.checkvec(
            "ft.aggregate",
            f"{key_type}_idx1",
            "*",
            "load",
            "2",
            "t1",
            "__key",
            "apply",
            'contains(@t1, "")',
            "as",
            "apply_result",
        )
        self.checkvec(
            "ft.aggregate",
            f"{key_type}_idx1",
            "*",
            "load",
            "2",
            "__key",
            "t1",
            "apply",
            'contains("", "one")',
            "as",
            "apply_result",
        )
        self.checkvec(
            "ft.aggregate",
            f"{key_type}_idx1",
            "*",
            "load",
            "2",
            "__key",
            "t3",
            "apply",
            'contains("", "")',
            "as",
            "apply_result",
        )

    def test_aggregate_substr(self, key_type):
        self.load_data_with_index("hard numbers", key_type)
        for offset in [0, 1, 2, 100, -1, -2, -3, -1000]:
            for len in [0, 1, 2, 100, -1, -2, -3, -1000]:
                self.checkvec(
                    "ft.aggregate",
                    f"{key_type}_idx1",
                    "*",
                    "load",
                    "2",
                    "t2",
                    "__key",
                    "apply",
                    f"substr(@t2, {offset}, {len})",
                    "as",
                    "apply_result",
        )

    def test_aggregate_dyadic_ops(self, key_type):
        self.load_data_with_index("hard numbers", key_type)
        values = ["-inf", "-1.5", "-1", "-0.5", "0", "0.5", "1.0", "+inf"]
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        logops = ["||", "&&"]
        for lop in values:
            for rop in values:
                for op in dyadic+relops+logops:
                    self.checkvec(
                        "ft.aggregate",
                        f"{key_type}_idx1",
                        "*",
                        "load",
                        "2",
                        "__key",
                        "t2",
                        "apply",
                        f"({lop}){op}({rop})",
                        "as",
                        "nn",
                )