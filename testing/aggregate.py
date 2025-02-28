import pytest, logging, time, itertools, redis, math
import sys, json
import numpy as np
from collections import defaultdict
from operator import itemgetter

'''
Compare two systems for a bunch of aggregation operations
'''
SETS_KEY = lambda key_type: f"{key_type} sets"
CREATES_KEY = lambda key_type: f"{key_type} creates"
VECTOR_DIM = 3

TEST_MARKER = "*" * 100

def random_f32_vector(dimension):
    return np.random.rand(dimension).astype(np.float32)

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


def compare_results(l, r):
    if len(l) != len(r):
        print(f"Mismatch, length of result {len(l)} != {len(r)}")
        return False
    for i in range(0, len(l)):
        lr = l[i]
        rr = r[i]
        if len(lr) != len(rr) or 0 != len(lr) % 2:
            print(f"Mismatch length of row {i}, {lr} {rr}")
            return False
        for f in range(0, len(lr), 2):
            if lr[f] != rr[f]:
                print(f"Field name mismatch: {lr[f]} != {rr[f]}")
                return False
            if lr[f + 1] != rr[f + 1]:
                # Python value doesn't match, try converting to number
                try:
                    num_l = float(lr[f + 1])
                    num_r = float(rr[f + 1])
                    # RL has some aggregate 0s that show up as 2.22507385851e-308
                    if not math.isclose(num_l, num_r, abs_tol=1e-300):
                        print(f"Value mismatch field {lr[f]}  {lr[f+1]} != {rr[f+1]}")
                        return False
                except:
                    if lr[f + 1] != rr[f + 1]:
                        print(f"Value mismatch field {lr[f]}  {lr[f+1]} != {rr[f+1]}")
                        return False
    return True

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


#@pytest.mark.parametrize("key_type", ["hash", "json"])
@pytest.mark.parametrize("key_type", ["hash"])
class TestAggregateCompatibility:
    """
    Test Aggregation compatibility
    """
    def setup_method(self):
        self.client_l = ClientLSystem()
        self.client_r = ClientRSystem()
        self.data = {}
        self.key_encoding = "utf8"

    ### Reusable Data ###
    def setup_names(self):
        data = {}
        data["names"] = {}

        create_cmds = {
            "hash": "FT.CREATE hash_idx1 ON HASH PREFIX 1 hash: SCHEMA {}",
            "json": "FT.CREATE json_idx1 ON JSON PREFIX 1 json: SCHEMA {}",
        }

        field_type_to_name = {"tag": "t", "numeric": "n", "vector": "v"}
        field_types_to_count = {"numeric": 3, "tag": 3, "vector" : 1}

        def get_field_piece(key_type, name, typ, i):
            if typ == "vector":
                return f"{name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
            else:
                return f"{name}{i} {typ}" if key_type == "hash" else f"$.{name}{i} AS {name}{i} {typ}"

        for key_type in ["hash", "json"]:
            # 2 fields per type except for vector
            schema = [
                get_field_piece(key_type, field_type_to_name[typ], typ, i + 1)
                for typ, count in field_types_to_count.items()
                for i in range(count)
            ]
            schema = " ".join(schema)
            print(f"Generated schema: {schema}")
            data["names"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
            data["names"][SETS_KEY(key_type)] = [
                (
                    f"{key_type}:{i}",
                    {
                        "n1": i,
                        "n2": -i,
                        "n3" : i*i,
                        "t1": f"one.one{i*2}",
                        "t2": f"two.two{i*-2}",
                        "t3": "all_the_same_value",
                        "v1": np.array([i for _ in range(VECTOR_DIM)]).astype(np.float32).tobytes(),
                    },
                )
                for i in range(10)
            ]
        self.data["names"] = data["names"]

    def setup_data(self, name, field_types_to_count):
        print("setup_data ", name)
        data = {}
        field_type_to_name = {"tag": "t", "numeric": "n", "vector": "v"}

        # These are possible values that can be assigned to a field
        possible_values = {}
        possible_values["vector"] = [
            random_f32_vector(VECTOR_DIM),
            np.array([0] * VECTOR_DIM).astype(np.float32),
        ]
        possible_values["text"] = ["", "a", "b", "aa", "a1"]
        possible_values["numeric"] = [-0.5, 0, 1, "-inf", "inf", "-nan", "nan"]
        possible_values["tag"] = ["", "a,b", "aa, bb"]
        # Power sets can have vectors. Let's have the same schema for now
        vector_opts = f"HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"

        def power_set():
            """
            This returns a list of tuples (key, data_dict). E.g [('hash:k0', {'key': 0, 't1': '', 'v1': b'F\xac\t=\xbaXH?\r\x941?'})].
            """
            assert all([typ in field_type_to_name for typ in field_types_to_count])
            field_name_to_type = {
                f"{field_type_to_name[typ]}{i+1}": typ
                for typ, count in field_types_to_count.items()
                for i in range(count)
            }
            field_names_per_type = defaultdict(list)
            for name, typ in field_name_to_type.items():
                field_names_per_type[typ].append(name)

            # Creates
            def full_text_create(key_type, field_name, field_type):
                return (
                    f"{field_name} {field_type}"
                    if key_type == "hash"
                    else f"$.{field_name} AS {field_name} {field_type}"
                )

            def vector_create(key_type, field_name, field_type, vector_opts):
                return (
                    f"{field_name} {field_type} {vector_opts}"
                    if key_type == "hash"
                    else f"$.{field_name} AS {field_name} {field_type} {vector_opts}"
                )

            def field_creates(key_type):
                return " ".join(
                    [
                        full_text_create(key_type, field_name, field_type)
                        for field_name, field_type in field_name_to_type.items()
                        if field_type != "vector"
                    ]
                    + [
                        vector_create(key_type, field, "VECTOR", vector_opts)
                        for field in field_names_per_type["vector"]
                    ]
                )

            cmds = {}
            cmds["hash creates"] = [
                "FT.CREATE hash_idx1 on hash prefix 1 hash: schema key numeric " + field_creates("hash")
            ]
            cmds["json creates"] = [
                "FT.CREATE json_idx1 on json prefix 1 json: schema $.key AS key numeric " + field_creates("json")
            ]

            # Sets
            for key_type in ["hash", "json"]:
                key_ind = 0
                sets = []
                possible_v = [
                    itertools.product(possible_values[typ], repeat=count) for typ, count in field_types_to_count.items()
                ]
                # Generate all possible values and create keys for them
                # Every iteration of this loop is a key to be generated
                # TODO: Return iterable instead of generating upfront
                for val in itertools.product(*possible_v):
                    cmd = {"key": key_ind}
                    for i, typ in enumerate(field_types_to_count):
                        for name, data in zip(field_names_per_type[typ], val[i]):
                            if typ == "vector":
                                if key_type == "hash":
                                    cmd[name] = data.tobytes()
                                else:
                                    cmd[name] = data.tolist()
                            else:
                                cmd[name] = data
                    sets += [(f"{key_type}:k{key_ind}", cmd)]
                    key_ind += 1
                cmds[SETS_KEY(key_type)] = sets
            return cmds

        self.data[name] = power_set()

        self.data["good numeric"] = power_set({"numeric": 3})
        # data["good text"] = power_set({"text": 3})
        # data["good tag"] = power_set({"tag": 3})
        # data["good vector"] = power_set({"vector": 2})
        # data["good text vec"] = power_set({"text": 3, "vector": 1, "tag": 1})
        self.data["vec"] = power_set({"vector": 1})

        keys_should_exist = [SETS_KEY("hash"), SETS_KEY("json")] + [
            CREATES_KEY("hash"),
            CREATES_KEY("json"),
        ]
        for data_key in self.data:
            assert all(
                [k in data[data_key].keys() for k in keys_should_exist]
            ), f"Error in setup_data: {data_key}; {data[data_key].keys()}; {keys_should_exist}"
        print("Setup data is ", self.data)

    ### Helper Functions ###
    def load_data_with_index(self, data_key, key_type):
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
        print(f"CMD: load_data_with_index completed {data_key} {key_type}")
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

    def unpack_search_result(self, rs, decode=False):
        """
        Input:
            rs: [
                    num_keys,
                    'key1', [field1, val1, field2, val2, ...]
                    'key2', [field1, val1, field2, val2, ...]
                    ...
                ]
        Example input:
                [3, b'json:k1', b'json:k4', b'json:k2']
                [3, b'json:k1', [b'$.name', b'John wick'], b'json:k4', [b'$.name', b'Jeff'], b'json:k2', [b'$.name', b'Jane']]

        Output:
            resultset: {
                    'key1': {field_key_1: field_val_key_1},
                    'key2': {field_key_2: field_val_key_2},
                    ...
                }
            num_keys: number_of_keys returned in result
        Example output:
                {
                    'hash:k3': {'USERNAME': 'Tom', 'AGE': 33}
                }

        NOTE: If a field has null value, it will not show in the unpacked result.
        """

        arr = {}
        total_matched_keys = rs[0]
        parse_field = lambda x: x[2::] if x.startswith("$.") else x
        # Let's do some checks here:
        # The first item is number of keys matched
        # Second is key name (even if it's returning keynames only)
        # Third can either be a list or a string but there need not be a third item because it could be returning
        # keynames only of 1 key.
        # If we don't have a list as our second item, we're returning key names only
        if len(rs) < 3 or not isinstance(rs[2], list):
            arr = {k.decode("utf-8"): "x" for k in rs[1::]}
            return arr, total_matched_keys

        for i in range(1, len(rs[1:]), 2):
            key = rs[i].decode("utf-8") if decode else rs[i]
            arr[key] = {}
            for f_idx in range(0, len(rs[i + 1]), 2):
                if rs[i + 1][f_idx + 1]:
                    field_name = parse_field(rs[i + 1][f_idx].decode("utf-8"))
                    try:
                        field_val = rs[i + 1][f_idx + 1].decode("utf-8")
                    except UnicodeDecodeError:
                        field_val = rs[i + 1][f_idx + 1]
                    arr[key][field_name] = field_val
        return arr

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
        arr = {}
        def parse_field(x):
            if isinstance(x, bytes):
                return parse_field(x.decode("utf-8"))
            if isinstance(x, str):
                return x[2::] if x.startswith("$.") else x
            if isinstance(x, int):
                return x
            print("Unknown type ", type(x))
            assert False
        def parse_value(x):
            if isinstance(x, bytes):
                return x.decode("utf-8")
            if isinstance(x, str):
                return x
            if isinstance(x, int):
                return x
            print("Unknown type ", type(x))
            assert False
        # Skip the first gibberish int
        rows = []
        for key_res in rs[1:]:
            rows += [{parse_field(key_res[f_idx]): parse_value(key_res[f_idx + 1])
                for f_idx in range(0, len(key_res), 2)}]
        return rows

    def process_result(self, r):
        result = []
        for row in r[1:]:
            (failure, res) = self.process_row(row)
            if failure:
                return (True, None)
            result += [res]
        return (False, result)

    def unpack_result(self, cmd, rs, sortkeys):
        if "ft.search" in cmd[0].split():
            out = self.unpack_search_result(rs, False)
        else:
            out = self.unpack_agg_result(rs)
        #
        # Sort by the sortkeys
        #
        if len(sortkeys) > 0:
            out.sort(key=itemgetter(*sortkeys))
        return out
    def compare_number_eq(self, l, r):
        if l == "nan" and r == "nan":
            return True
        else:
            return math.isclose(float(l), float(r), rel_tol=.01)
    def compare_row(self, l, r):
        lks = sorted(list(l.keys()))
        rks = sorted(list(r.keys()))
        if lks != rks:
            return False
        for i in range(len(lks)):
            if lks[i].startswith("n"):
                if not self.compare_number_eq(l[lks[i]], r[rks[i]]):
                    return False                
                else:
                    pass
            elif l[lks[i]] != r[rks[i]]:
                print("mismatch field: ", lks[i], " and ", rks[i], " ", lks[i].startswith("n")," ", l[lks[i]], "!=", r[rks[i]])
                return False
        return True            
        
    def compare_results(self, cmd):
        print(TEST_MARKER)
        print(f"CMD: {cmd}")
        engines = {"RL": self.client_r, "EC": self.client_l}
        if 'groupby' in cmd:
            ix = cmd.index('groupby')
            count = int(cmd[ix+1])
            sortkeys = [cmd[ix+2+i][1:] for i in range(count)]
        elif 'sortby' in cmd:
            sortkeys = []
        else:
            sortkeys=['__key']

        results = {eng: None for eng in engines}
        exception = {eng: False for eng in engines}
        for name, client in engines.items():
            try:
                rs = client.execute_command(*cmd)
                results[name] = rs
                # print(f"{name} replied: {rs}")
            except Exception as exc:
                print(f"Got exception for {name} Error: '{exc}'")
                exception[name] = True
                # assert False

        # If both failed, it's a wrong search cmd and we can exit
        if all(exception.values()):
            print("Both engines failed.")
            print(TEST_MARKER)
            return

        if exception["RL"]:
            print("RL Exception, skipped")
            #print(f"RL Exception: Raw: {printable_result(results['RL'])}")
            #print(f"EC Result: {printable_result(results['EC'])}")
            print(TEST_MARKER)
            return

        if exception["EC"]:
            print(f"RL Result: {printable_result(results['RL'])}")
            print(f"EC Exception Raw: {printable_result(results['EC'])}")
            print(TEST_MARKER)
            assert False

        # Process failures
        if len(results["RL"]) != len(results["EC"]):
            print(f"Mismatched sizes RL:{len(results['RL'])} EC:{len(results['EC'])}")
            assert False

        # Output raw results
        rl = self.unpack_result(cmd, results["RL"], sortkeys)
        ec = self.unpack_result(cmd, results["EC"], sortkeys)

        # if compare_results(ec, rl):
        # Directly comparing dicts instead of custom compare function
        # TODO: investigate this later
        if all([self.compare_row(ec[i], rl[i]) for i in range(len(rl))]):
            print("Results look good.")
            print(TEST_MARKER)
            return
        print("***** MISMATCH ON DATA *****, sortkeys=", sortkeys, " records=", len(rl))
        print(f"CMD: {cmd}")
        print(f"RL raw result:{results['RL']}")
        print(f"EC raw result:{results['EC']}")
        print("--- Sorted ---")
        print(rl)
        for i in range(len(rl)):
            print("RL",i,[(k,rl[i][k]) for k in sorted(rl[i].keys())], "" if self.compare_row(rl[i], ec[i]) else "<<<")
            print("EC",i,[(k,ec[i][k]) for k in sorted(ec[i].keys())], "" if self.compare_row(rl[i],ec[i]) else "<<<")
        print(TEST_MARKER)
        assert False

    # Tests generator for FT.SEARCH
    def search_tests_generator(self, data_key, key_type):
        load_list = self.data[data_key][SETS_KEY(key_type)]
        fields = [field for field in load_list[0][1] if field != "key"]
        # Ordering of these values in "possible_values" is important. They are evaluated in the order they appear below.
        # This is because dicts are ordered. We make use of that as we are looping through keys in a dict.
        possible_values = {}
        possible_values["query"] = itertools.chain(["*"], [f"*=>[KNN 3 @{f} $query_vec]" for f in fields])
        # Either we generate possible options for return type or we `RETURN 0`
        possible_values["return"] = itertools.chain(
            (com for num_fields in range(len(fields)) for com in itertools.combinations(fields, num_fields + 1)),
            ["0", ""],
        )
        possible_values["limit"] = itertools.chain(["0 0", "0 10", "0 100"], [""])
        possible_values["sortby"] = itertools.chain(itertools.product(fields, ["asc", "desc"]), [""])

        formatters = {}

        def _return_format(x):
            x = x[0]
            x = [p for p in x if p.strip()]
            if x == ["0"]:
                return "RETURN 0".split()
            else:
                return f"RETURN {len(x)} {' '.join(x)}".split()

        def _sort_format(x):
            f, sort_dir = x[0]
            return f"sortby 2 {f} {sort_dir}".split()

        def _query_format(x):
            query = x[0]
            if query != "*":
                float_data = np.array([0] * VECTOR_DIM).astype(np.float32).tobytes()
                res = [query, "PARAMS", "2", "query_vec", float_data]
                return res
            else:
                return "*".split()

        formatters["return"] = _return_format
        formatters["limit"] = lambda x: f"LIMIT {x[0]}".split() if x[0] else [""]
        formatters["sortby"] = _sort_format
        formatters["query"] = _query_format

        def _internal_gen():
            possible_v = [itertools.combinations(possible_values[typ], r=1) for typ in possible_values]
            for val in itertools.product(*possible_v):
                yield val

        for query, ret_data, limit_data, sort_data in _internal_gen():
            cmd = f"ft.search {key_type}_idx1".split()
            # Query will always exist
            cmd.extend(formatters["query"](query))
            if ret_data[0]:
                cmd.extend(formatters["return"](ret_data))
            if limit_data[0]:
                cmd.extend(formatters["limit"](limit_data))
            if sort_data[0]:
                cmd.extend(formatters["sortby"](sort_data))
            cmd.extend("DIALECT 2".split())
            yield cmd

    # Tests generator for FT.AGGREGATE
    def agg_tests_generator(self, data_key, key_type):
        load_list = self.data[data_key][SETS_KEY(key_type)]
        fields = [field for field in load_list[0][1] if field != "key"]
        reduce_values = [(opt, f) for opt, f in itertools.product(*[["count", "sum", "avg", "min", "max"], fields])]
        possible_values = {}
        possible_values["query"] = itertools.chain([f"*=>[KNN 3 @{f} $query_vec]" for f in fields])
        possible_values["groupby"] = itertools.chain(
            itertools.product(fields, reduce_values),
            [""],
        )
        possible_values["sortby"] = itertools.chain(itertools.product(fields, ["asc", "desc"]), [""])

        def _internal_gen():
            possible_v = [itertools.combinations(possible_values[typ], r=1) for typ in possible_values]
            for val in itertools.product(*possible_v):
                yield val

        def _group_format(x):
            def _reduce_format(x):
                opt, f = x
                if opt != "count":
                    return f"reduce {opt} @{f} {1} @{f} as {opt}"
                else:
                    return "reduce count 0 as count"

            group_f, reduce_data = x[0]
            reduce_str = _reduce_format(reduce_data)
            group_str = f"groupby 1 @{group_f}"
            return f"{group_str} {reduce_str}".split()

        def _sort_format(x):
            f, sort_dir = x[0]
            return f"sortby 2 @{f} {sort_dir}".split()

        def _query_format(x):
            query = x[0]
            if query != "*":
                float_data = np.array([0] * VECTOR_DIM).astype(np.float32).tobytes()
                res = [query, "PARAMS", "2", "query_vec", float_data, "DIALECT", 3]
                return res
            else:
                return "*".split()

        formatters = {}
        formatters["query"] = _query_format
        formatters["groupby"] = _group_format
        formatters["sortby"] = _sort_format

        for query, group_data, sort_data in _internal_gen():
            cmd = f"ft.aggregate {key_type}_idx1".split()
            # Query will always exist
            cmd.extend(formatters["query"](query))
            cmd += ["load", f"{len(fields)+1}", "@__key"]
            # Always load all fields
            cmd += [f"@{f}" for f in fields]
            opt_pieces = []
            if group_data[0]:
                cmd.extend(formatters["groupby"](group_data))
            if sort_data[0]:
                cmd.extend(formatters["sortby"](sort_data))
            cmd += opt_pieces
            yield cmd
    '''
    @pytest.mark.parametrize("test_case", ["vec"])
    def test_agg_vec_only(self, key_type, test_case):
        self.setup_data(test_case, {"vector": 1})
        self.load_data_with_index(test_case, key_type)
        for test in self.agg_tests_generator(test_case, key_type):
            self.compare_results(test)

    @pytest.mark.parametrize("test_case", ["vec"])
    def test_search_vec_only(self, key_type, test_case):
        self.setup_data(test_case, {"vector": 1})
        self.load_data_with_index(test_case, key_type)
        for test in self.search_tests_generator(test_case, key_type):
            self.compare_results(test)
    '''

    def checkvec(self, *orig_cmd):
        '''Temporary change until query parser is redone.'''
        cmd = orig_cmd[0].split() if len(orig_cmd) == 1 else [*orig_cmd]
        new_cmd = []
        print("checkvec:", cmd)
        for c in cmd:
            if c.strip() == "*":
                ''' substitute '''
                new_cmd += ["*=>[KNN 10000 @v1 $BLOB]"]
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

    def test_aggregate_basic_compatibility(self, key_type):
        self.setup_names()
        self.load_data_with_index("names", key_type)
        # self.checkvec(f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3")
        self.checkvec(f"ft.aggregate {key_type}_idx1 * load 2 @__key @n2 sortby 1 @n2")
        self.checkvec(f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 1 @n2")
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @n2 asc"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @n2 desc"
        )
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
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce stddev 1 @n1 as stddev"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce stddev 1 @n1 as stddev"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce stddev 1 @n2 as stddev"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce max 1 @n1 as max"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce max 1 @n1 as max"
        )
        ## RL Max does not support negatives and incorrectly rounds up to zero:
        # self.checkvec(f'ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce max 1 @n2 as max')
    @pytest.mark.parametrize("numbers", ["names"])
    def test_aggregate_numeric_dyadic_operators(self, key_type, numbers):
        self.setup_names()
        keys = self.load_data_with_index(numbers, key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        for k in range(keys):
            for op in dyadic + relops:
                self.checkvec(
                    f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 apply @n1{op}@n2 as nn"
                )

    def test_aggregate_numeric_load_store(self, key_type):
        self.setup_names()
        keys = self.load_data_with_index("names", key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        self.checkvec(f"ft.aggregate {key_type}_idx1 * load 2 @__key @n1")
        self.checkvec(
            f"ft.aggregate {key_type}_idx1  * load 2 @__key @n1 apply @n1 as nn"
        )
        self.checkvec(
            f"ft.aggregate {key_type}_idx1  * load 2 @__key @n1 apply @n1+0 as nn"
        )

    def test_aggregate_numeric_triadic_operators(self, key_type):
        self.setup_names()
        keys = self.load_data_with_index("names", key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        for relop in relops:
            for lop in dyadic:
                for rop in dyadic:
                    self.checkvec(
                        f"ft.aggregate {key_type}_idx1  * load 4 @__key @n1 @n2 @n3 apply (@n1{lop}@n2){relop}@n3 as nn"
                    )
                    self.checkvec(
                        f"ft.aggregate {key_type}_idx1  * load 4 @__key @n1 @n2 @n3 apply @n1{lop}@n2{relop}@n3 as nn"
                    )

    def test_aggregate_numeric_functions(self, key_type):
        self.setup_names()
        keys = self.load_data_with_index("names", key_type)
        function = ["log", "abs", "ceil", "floor", "log2", "exp", "sqrt"]
        for f in function:
            self.checkvec(
                f"ft.aggregate {key_type}_idx1  * load 2 @__key @n1 apply {f}(@n1) as nn"
            )

    def test_aggregate_string_apply_functions(self, key_type):
        self.setup_names()
        self.load_data_with_index("names", key_type)

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

        # # String apply function "substr"
        self.checkvec(
            "ft.aggregate",
            f"{key_type}_idx1",
            "*",
            "load",
            "2",
            "t2",
            "__key",
            "apply",
            "substr(@t2, 0, 5)",
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
            "substr(@t2, 1, 2)",
            "as",
            "apply_result",
        )
        # self.checkvec('ft.aggregate', f'{key_type}_idx1', '*', 'load', '1', 't2', 'apply', 'substr(@t2, 1.1, 2)', 'as', 'apply_result')
        # self.checkvec('ft.aggregate', f'{key_type}_idx1', '*', 'load', '1', 't2', 'apply', 'substr(@t2, 1, "2.9")', 'as', 'apply_result')
        # self.checkvec('ft.aggregate', f'{key_type}_idx1', '*', 'load', '1', 't2', 'apply', 'substr(@t2, 1, "-2.1")', 'as', 'apply_result')
        self.checkvec(
            "ft.aggregate",
            f"{key_type}_idx1",
            "*",
            "load",
            "2",
            "t2",
            "__key",
            "apply",
            "substr(@t2, 3, 10)",
            "as",
            "apply_result",
        )

    def test_aggregate_substr(self, key_type):
        self.setup_names()
        self.load_data_with_index("names", key_type)
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
