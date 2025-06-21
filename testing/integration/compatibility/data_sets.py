import numpy as np
import itertools, redis, json

### Reusable Data ###
#
# This is the generate data set for all tests.
# Also, common defines.
#
NUM_KEYS = 10
VECTOR_DIM = 3

SETS_KEY = lambda key_type: f"{key_type} sets"
CREATES_KEY = lambda key_type: f"{key_type} creates"
ANSWER_FILE_NAME = "aggregate-answers.pickle"

def unbytes(b):
    if isinstance(b, bytes):
        return b.decode("utf-8")
    else:
        return b
class ClientSystem:
    def __init__(self, address):
        self.address = address
        self.client = redis.Redis(host=address[0], port=address[1])

    def execute_command(self, *cmd):
        print("Execute:", *cmd)
        result = self.client.execute_command(*cmd)
        #print("Result:", result)
        return result
    
    def ft_info(self, index):
        values = self.client.execute_command(f"FT.INFO {index}")
        result = {unbytes(values[i]):unbytes(values[i+1]) for i in range(0, len(values), 2)}
        return result
        
    def pipeline(self):
        return self.client.pipeline()
    
    def wait_for_indexing_done(self, index_name):
        assert False
    
    def hset(self, *cmd):
        return self.client.hset(*cmd)
    
def array_encode(key_type, array):
    if key_type == "hash":
        return np.array(array).astype(np.float32).tobytes()
    else:
        return array

def json_quote(s):
    if s == '"':
        return '\\"'
    if s == '\\':
        return '\\\\'
    return f'\\u{s:04x}'

def binary_string_encode(key_type, s):
    if key_type == "hash":
        return s
    else:
        return '"' + "".join([json_quote(s[i]) for i in range(len(s))]) + '"'       
    
def compute_data_sets():
    '''Generate all of the possible data sets'''
    data = {}

    create_cmds = {
        "hash": "FT.CREATE hash_idx1 ON HASH PREFIX 1 hash: SCHEMA {}",
        "json": "FT.CREATE json_idx1 ON JSON PREFIX 1 json: SCHEMA {}",
    }
    field_type_to_name = {"tag": "t", "numeric": "n", "vector": "v"}
    field_types_to_count = {"numeric": 3, "tag": 3, "vector" : 1}

    def make_field_definition(key_type, name, typ, i):
        if typ == "vector":
            if key_type == "hash":
                return f"{name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
            else:
                return f"$.{name}{i} as {name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
            return f"{name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
        else:
            return f"{name}{i} {typ}" if key_type == "hash" else f"$.{name}{i} AS {name}{i} {typ}"

    data["hard numbers"] = {}
    data["sortable numbers"] = {}
    data["reverse vector numbers"] = {}
    data["bad numbers"] = {}
    data["hard strings"] = {}
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
        hard_numbers = [-0.5, 0, -0, 1, -1] # todo "nan", -0
        if key_type == "hash":
            hard_numbers += [float("inf"), float("-inf")]
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
                    "v1": array_encode(key_type, [i for _ in range(VECTOR_DIM)]),
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
                    "v1": array_encode(key_type, [i for _ in range(VECTOR_DIM)]),
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
                    "v1": array_encode(key_type, [(len(sortable_numbers)-i) for _ in range(VECTOR_DIM)]),
                },
            )
            for i in range(len(sortable_numbers))
        ]
        #
        #  Bad numbers, things that don't convert to their designated types.
        #
        data["bad numbers"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
        data["bad numbers"][SETS_KEY(key_type)] = [
            (f"{key_type}:0",
                {
                    "n1": 0,
                    "n2": 0,
                    "n3": 0,
                    "t1": "",
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [0 for _ in range(VECTOR_DIM)]),
                },
            ),
            (f"{key_type}:1",
                {
                    "n1": "bad",
                    "n2": 0,
                    "n3": 0,
                    "t1": "",
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [1 for _ in range(VECTOR_DIM)]),
                },
            ),
            (f"{key_type}:2",
                {
                    "n1": True if key_type == "json" else 1,
                    "n2": 0,
                    "n3": 0,
                    "t1": "",
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [2 for _ in range(VECTOR_DIM)]),
                },
            ),
            (f"{key_type}:3",
                {
                    # "n1": 0,
                    "n2": 0,
                    "n3": 0,
                    "t1": "",
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [3 for _ in range(VECTOR_DIM)]),
                },
            ),
            (f"{key_type}:4",
                {
                    "n1": 0,
                    "n2": 0,
                    "n3": 0,
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [4 for _ in range(VECTOR_DIM)]),
                },
            ),
            (f"{key_type}:5",
                {
                    "n1": 0,
                    "n2": 0,
                    "n3": 0,
                    "t2": "",
                    "t3": "",
                    "v1": array_encode(key_type, [5 for _ in range(VECTOR_DIM+1)]),
                },
            ),
        ]
        #
        # hard strings
        #
        data["hard strings"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
        data["hard strings"][SETS_KEY(key_type)] = [
            (
                f"{key_type}:{i:02d}",
                {
                    "n1": 0,
                    "n2": -i,
                    "n3" : i*2,
                    "t1": f"one.one{i*2}",
                    "t2": f"two.two{i*-2}",
                    "t3": "all_the_same_value",
                    "v1": array_encode(key_type, [i for _ in range(VECTOR_DIM)]),
                },
            )
            for i in range(300))
        ]
    return data

### Helper Functions ###
def load_data(client, data_set, key_type):
    data = compute_data_sets()
    load_list = data[data_set][SETS_KEY(key_type)]
    print(f"Start Loading Data, data set has {len(load_list)} records")
    for create_index_cmd in data[data_set][CREATES_KEY(key_type)]:
        client.execute_command(create_index_cmd)

    # Make large chunks to accelerate things
    batch_size = 50
    for s in range(0, len(load_list), batch_size):
        pipe = client.pipeline()
        for cmd in load_list[s : s + batch_size]:
            if key_type == "hash":
                pipe.hset(cmd[0], mapping=cmd[1])
            else:
                pipe.execute_command(*["JSON.SET", cmd[0], "$", json.dumps(cmd[1])])
        pipe.execute()

    client.wait_for_indexing_done(f"{key_type}_idx1")
    print(f"setup_data completed {data_set} {key_type}")
    if key_type != "hash":
        for s in range(0, len(load_list)):
            k = client.execute_command(*["JSON.GET", load_list[s][0], "$"])
            print(f"{s}:{load_list[s][0]}:  ", k)
    return len(load_list)
