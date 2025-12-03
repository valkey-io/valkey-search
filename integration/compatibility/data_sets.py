import itertools, valkey, json, struct

### Reusable Data ###
#
# This is the generate data set for all tests.
# Also, common defines.
#
NUM_KEYS = 10
VECTOR_DIM = 3

SETS_KEY = lambda key_type: f"{key_type} sets"
CREATES_KEY = lambda key_type: f"{key_type} creates"

text_data_path = "integration/compatibility/text_data.txt"

def unbytes(b):
    if isinstance(b, bytes):
        return b.decode("utf-8")
    else:
        return b
class ClientSystem:
    def __init__(self, address):
        self.address = address
        self.client = valkey.Valkey(host=address[0], port=address[1])

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
        return struct.pack(f"<{len(array)}f", *array)
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

def load_text_data(filepath=text_data_path):
    """
    Load text test data from file.
    
    Returns:
        dict: Dictionary mapping data type to list of text samples
              e.g., {'BASIC_TEXT': [...], 'SUFFIX_CODES': [...], ...}
    """
    import os
    
    # Handle relative path
    if not os.path.isabs(filepath):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        filepath = os.path.join(current_dir, "text_data.txt")
    
    text_data = {}
    current_section = None
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Check for section header
            if line.startswith('[') and line.endswith(']'):
                current_section = line[1:-1]
                text_data[current_section] = []
            elif current_section:
                # Add data line to current section
                text_data[current_section].append(line)
    return text_data

def add_text_datasets(data, key_type):
    """
    Add TEXT field test data sets for compatibility testing.
    Loads data from text_data.txt file.
    
    Args:
        data: The main data dictionary to populate
        key_type: Either "hash" or "json"
    """
    # Load text samples from file
    text_samples = load_text_data()
    
    # 1. Basic text - default settings
    schema = "txt1 TEXT txt2 TEXT" if key_type == "hash" else "$.txt1 AS txt1 TEXT $.txt2 AS txt2 TEXT"
    data["basic text"][CREATES_KEY(key_type)] = [
        f"FT.CREATE {key_type}_idx1 ON {key_type.upper()} PREFIX 1 {key_type}: SCHEMA {schema}"
    ]
    data["basic text"][SETS_KEY(key_type)] = [
        (
            f"{key_type}:{i:02d}",
            {
                "txt1": text_samples["BASIC_TEXT"][i],
                "txt2": text_samples["BASIC_TEXT"][-(i+1)],
                "n1": i,
            },
        )
        for i in range(len(text_samples["BASIC_TEXT"]))
    ]

    # 2. Text with suffix trie
    schema = "code TEXT WITHSUFFIXTRIE description TEXT" if key_type == "hash" else \
             "$.code AS code TEXT WITHSUFFIXTRIE $.description AS description TEXT"
    data["text with suffix"][CREATES_KEY(key_type)] = [
        f"FT.CREATE {key_type}_idx1 ON {key_type.upper()} PREFIX 1 {key_type}: SCHEMA {schema}"
    ]
    data["text with suffix"][SETS_KEY(key_type)] = [
        (
            f"{key_type}:{i:02d}",
            {
                "code": text_samples["SUFFIX_CODES"][i],
                "description": text_samples["SUFFIX_DESCRIPTIONS"][i],
                "n1": i,
            },
        )
        for i in range(len(text_samples["SUFFIX_CODES"]))
    ]

    # 3. Text phrase matching (WITHOFFSETS is default)
    schema = "phrase TEXT" if key_type == "hash" else "$.phrase AS phrase TEXT"
    data["text phrase matching"][CREATES_KEY(key_type)] = [
        f"FT.CREATE {key_type}_idx1 ON {key_type.upper()} PREFIX 1 {key_type}: WITHOFFSETS SCHEMA {schema}"
    ]
    data["text phrase matching"][SETS_KEY(key_type)] = [
        (
            f"{key_type}:{i:02d}",
            {
                "phrase": text_samples["PHRASE_MATCHING"][i],
                "n1": i,
            },
        )
        for i in range(len(text_samples["PHRASE_MATCHING"]))
    ]

    # 4. Text no stem
    schema = "technical TEXT NOSTEM" if key_type == "hash" else "$.technical AS technical TEXT NOSTEM"
    data["text no stem"][CREATES_KEY(key_type)] = [
        f"FT.CREATE {key_type}_idx1 ON {key_type.upper()} PREFIX 1 {key_type}: NOSTEM SCHEMA {schema}"
    ]
    data["text no stem"][SETS_KEY(key_type)] = [
        (
            f"{key_type}:{i:02d}",
            {
                "technical": text_samples["NO_STEM"][i],
                "n1": i,
            },
        )
        for i in range(len(text_samples["NO_STEM"]))
    ]

    # 5. Text custom punctuation
    schema = "content TEXT" if key_type == "hash" else "$.content AS content TEXT"
    data["text custom punctuation"][CREATES_KEY(key_type)] = [
        f'FT.CREATE {key_type}_idx1 ON {key_type.upper()} PREFIX 1 {key_type}: PUNCTUATION " @." SCHEMA {schema}'
    ]
    data["text custom punctuation"][SETS_KEY(key_type)] = [
        (
            f"{key_type}:{i:02d}",
            {
                "content": text_samples["CUSTOM_PUNCTUATION"][i],
                "n1": i,
            },
        )
        for i in range(len(text_samples["CUSTOM_PUNCTUATION"]))
    ]

    # 6. Text no stopwords
    schema = "short_text TEXT" if key_type == "hash" else "$.short_text AS short_text TEXT"
    data["text no stopwords"][CREATES_KEY(key_type)] = [
        f"FT.CREATE {key_type}_idx1 ON {key_type.upper()} PREFIX 1 {key_type}: NOSTOPWORDS SCHEMA {schema}"
    ]
    data["text no stopwords"][SETS_KEY(key_type)] = [
        (
            f"{key_type}:{i:02d}",
            {
                "short_text": text_samples["NO_STOPWORDS"][i],
                "n1": i,
            },
        )
        for i in range(len(text_samples["NO_STOPWORDS"]))
    ]

    # 7. Text mixed config - per-field options
    schema = "exact TEXT NOSTEM WITHSUFFIXTRIE searchable TEXT MINSTEMSIZE 2" if key_type == "hash" else \
             "$.exact AS exact TEXT NOSTEM WITHSUFFIXTRIE $.searchable AS searchable TEXT MINSTEMSIZE 2"
    data["text mixed config"][CREATES_KEY(key_type)] = [
        f'FT.CREATE {key_type}_idx1 ON {key_type.upper()} PREFIX 1 {key_type}: PUNCTUATION ".,;" STOPWORDS 3 the and or SCHEMA {schema}'
    ]
    data["text mixed config"][SETS_KEY(key_type)] = [
        (
            f"{key_type}:{i:02d}",
            {
                "exact": text_samples["MIXED_EXACT"][i],
                "searchable": text_samples["MIXED_SEARCHABLE"][i],
                "n1": i,
            },
        )
        for i in range(len(text_samples["MIXED_EXACT"]))
    ]

def compute_data_sets():
    '''Generate all of the possible data sets'''
    data = {}

    create_cmds = {
        "hash": "FT.CREATE hash_idx1 ON HASH PREFIX 1 hash: SCHEMA {}",
        "json": "FT.CREATE json_idx1 ON JSON PREFIX 1 json: SCHEMA {}",
    }
    field_type_to_name = {"tag": "t", "numeric": "n", "vector": "v", "text": "txt"}
    field_types_to_count = {"numeric": 3, "tag": 3, "vector": 1, "text": 3}

    def make_field_definition(key_type, name, typ, i, text_options=""):
        if typ == "vector":
            if key_type == "hash":
                return f"{name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
            else:
                return f"$.{name}{i} as {name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
            return f"{name}{i} vector HNSW 6 DIM {VECTOR_DIM} TYPE FLOAT32 DISTANCE_METRIC L2"
        elif typ == "text":
            base = f"{name}{i} TEXT" if key_type == "hash" else f"$.{name}{i} AS {name}{i} TEXT"
            return f"{base} {text_options}".strip() 
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
                    "e1" : 1,
                    "e2" : "two",
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
                    "e1" : 1,
                    "e2" : "two",
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
                    "e1" : 1,
                    "e2" : "two",
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
                    "e1" : 1,
                    "e2" : "two",
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
                    "e1" : 1,
                    "e2" : "two",
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
                    "e1" : 1,
                    "e2" : "two",
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
                    "e1" : 1,
                    "e2" : "two",
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
                    "e1" : 1,
                    "e2" : "two",
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
        unicode_chars = "".join(
            [chr(c) for c in range(0, 128)] + 
            [chr(c) for c in range(0x7f, 0x82)] +
            [chr(c) for c in range(0x7ff, 0x802)] +
            [chr(c) for c in range(0xFFFB, 0x10002)] +
            [chr(c) for c in range(0x10FFFB, 0x110000)])
        data["hard strings"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema)]
        data["hard strings"][SETS_KEY(key_type)] = [
            (
                f"{key_type}:{i:02d}",
                {
                    "n1": 0,
                    "n2": -i,
                    "n3" : i*2,
                    "t1": unicode_chars,
                    "t2": unicode_chars[i:],
                    "t3": "all_the_same_value",
                    "v1": array_encode(key_type, [i for _ in range(VECTOR_DIM)]),
                    "e1" : 1,
                    "e2" : "two",
                },
            )
            for i in range(20)
        ]

    # TEXT data
    # Initialize TEXT data sets
    data["basic text"] = {}
    data["text with suffix"] = {}
    data["text phrase matching"] = {}
    data["text no stem"] = {}
    data["text custom punctuation"] = {}
    data["text no stopwords"] = {}
    data["text mixed config"] = {}
    
    # Generate TEXT data for both hash and json
    for key_type in ["hash", "json"]:
        add_text_datasets(data, key_type)

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

    # client.wait_for_indexing_done(f"{key_type}_idx1")
    print(f"setup_data completed {data_set} {key_type}")
    if key_type != "hash":
        for s in range(0, len(load_list)):
            k = client.execute_command(*["JSON.GET", load_list[s][0], "$"])
            print(f"{s}:{load_list[s][0]}:  ", k)
    return len(load_list)
