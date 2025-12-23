import itertools, valkey, json, struct, random

### Reusable Data ###
#
# This is the generate data set for all tests.
# Also, common defines.
#
NUM_KEYS = 10
VECTOR_DIM = 3

SETS_KEY = lambda key_type: f"{key_type} sets"
CREATES_KEY = lambda key_type: f"{key_type} creates"

# Text data configuration
TEXT_SCHEMA = {
    'text': ['title', 'body'],
    'tag': ['color'],
    'numeric': ['price']
}

TEXT_DATASETS = {
    'pure text': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                # fruits
                'apple', 'banana', 'orange', 'grape', 'cherry', 'mango',
                # other foods
                'pear', 'peach', 'plum', 'melon', 'kiwi', 'lemon',
                # objects
                'table', 'chair', 'desk', 'lamp', 'window', 'door',
                # abstract / misc
                'music', 'movie', 'book', 'story', 'game', 'puzzle',
                # adjectives
                'quick', 'bright', 'silent', 'heavy', 'smooth', 'sharp'
            ],
            'body': [
                # veggies
                'potato', 'tomato', 'lettuce', 'onion', 'carrot', 'brocoli',
                # animals
                'dog', 'cat', 'horse', 'tiger', 'eagle', 'shark',
                # places
                'city', 'village', 'forest', 'desert', 'ocean', 'river',
                # actions
                'run', 'jump', 'swim', 'drive', 'fly', 'build',
                # descriptors
                'fast', 'slow', 'loud', 'quiet', 'warm', 'cold'
            ],

            'color': [
                'red',
                'yellow',
                'green',
                'purple',
                'blue',
                'black',
                'white',
                'orange',
                'pink',
                'brown'
            ],
            'price': (0, 50)
        }
    },
    'pure text small': {
        'schema': TEXT_SCHEMA,
        'field_values': {
            'title': [
                'apple', 'banana', 'orange', 'grape', 'cherry',
                'dog', 'cat', 'horse', 'city', 'forest'
            ],
            'body': [
                'apple', 'banana', 'orange', 'grape', 'cherry',
                'dog', 'cat', 'horse', 'city', 'forest'
            ],
            'color': ['red', 'yellow', 'green', 'purple', 'blue'],
            'price': (0, 10)
        }
    }
}

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
    vec_algos = ["flat", "hnsw"]
    metrics = ["cosine", "ip", "l2"]
    for algo in vec_algos:
        for metric in metrics:
            data[f"vector data {metric} {algo}"] = {}
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
        for algo in vec_algos:
            for metric in metrics:
                vector_points = [-1.5, 1.5]
                data[f"vector data {metric} {algo}"][CREATES_KEY(key_type)] = [create_cmds[key_type].format(schema).replace("L2", metric).replace("HNSW", algo)]
                data[f"vector data {metric} {algo}"][SETS_KEY(key_type)] = [
                    (
                        f"{key_type}:{x:}:{y}:{z}",
                        {
                            "n1": x,
                            "n2": y,
                            "n3" : z,
                            "t1": "",
                            "t2": "",
                            "t3": "all_the_same_value",
                            "v1": array_encode(key_type, [x, y, z]),
                            "e1" : 1,
                            "e2" : "two",
                        },
                    )
                    for x in vector_points
                    for y in vector_points
                    for z in vector_points
                ]
    return data

# TODO: stopwords, punctuation, numbers in text field (with periods, float, +/-)
# TODO: special case with lexers (random lexical chars, prove lexical analyzer work correct) (isolation)
def compute_text_data_sets(dataset_name, seed=123):
    """Generate random documents for a specific dataset.
    
    Args:
        dataset_name: Name of dataset (e.g., "pure text", "pure text small")
        seed: Random seed for reproducibility
    
    Returns:
        dict with structure: {dataset_name: {"hash creates": [...], "hash sets": [...], ...}}
    """
    if dataset_name not in TEXT_DATASETS:
        raise ValueError(f"Unknown dataset: {dataset_name}. Available: {list(TEXT_DATASETS.keys())}")
    
    config = TEXT_DATASETS[dataset_name]
    field_values = config['field_values']
    schema = config['schema']
    
    random.seed(seed)
    
    data = {}
    data[dataset_name] = {}
    
    text_fields = schema.get('text', [])
    tag_fields = schema.get('tag', [])
    numeric_fields = schema.get('numeric', [])
    
    # Build create commands for both hash and json
    create_cmds = {
        "hash": "FT.CREATE hash_idx1 ON HASH PREFIX 1 hash: SCHEMA {}",
        "json": "FT.CREATE json_idx1 ON JSON PREFIX 1 json: SCHEMA {}",
    }
    
    # Build schema strings
    hash_schema_parts = []
    json_schema_parts = []
    
    for field in text_fields:
        hash_schema_parts.append(f"{field} TEXT WITHSUFFIXTRIE NOSTEM")
        json_schema_parts.append(f"$.{field} AS {field} TEXT WITHSUFFIXTRIE NOSTEM")
    
    for field in tag_fields:
        hash_schema_parts.append(f"{field} TAG")
        json_schema_parts.append(f"$.{field} AS {field} TAG")
    
    for field in numeric_fields:
        hash_schema_parts.append(f"{field} NUMERIC")
        json_schema_parts.append(f"$.{field} AS {field} NUMERIC")
    
    hash_schema = " ".join(hash_schema_parts)
    json_schema = " ".join(json_schema_parts)
    
    # Get vocab for text fields
    vocab = {}
    for field in text_fields:
        if field in field_values:
            vocab[field] = field_values[field]

    # Helper to generate a document
    def generate_doc(doc_id):
        fields = {}
        # Text fields - generate random length 1-5 for title and body
        for field in text_fields:
            if field in vocab:
                num_words = random.randint(1, 10)
                words = random.choices(vocab[field], k=num_words)
                fields[field] = " ".join(words)
        # Tag fields
        for field in tag_fields:
            if field in field_values:
                fields[field] = random.choice(field_values[field])
        # Numeric fields
        for field in numeric_fields:
            if field in field_values:
                min_val, max_val = field_values[field]
                fields[field] = random.randint(min_val, max_val)
        return fields
    
    for key_type in ["hash", "json"]:
        # Set create commands
        if key_type == "hash":
            data[dataset_name][CREATES_KEY(key_type)] = [create_cmds[key_type].format(hash_schema)]
        else:
            data[dataset_name][CREATES_KEY(key_type)] = [create_cmds[key_type].format(json_schema)]
        
        # Generate documents
        docs = []
        for i in range(NUM_KEYS):
            fields = generate_doc(i)
            docs.append((f"{key_type}:{i:02d}", fields))
        
        data[dataset_name][SETS_KEY(key_type)] = docs
    
    return data

### Helper Functions ###
def load_data(client, data_set, key_type, data_source=None):
    # Auto-detect data source based on data_set name
    if data_source is None:
        data_source = "text" if data_set in TEXT_DATASETS else "vector"

    match data_source:
        case "vector":
            data = compute_data_sets()
        case "text":
            data = compute_text_data_sets(data_set)
        case _:
            raise ValueError(f"Unknown data source: {data_source}")
    load_list = data[data_set][SETS_KEY(key_type)]
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

    # Print loaded data for debugging
    print(f"Loaded {len(load_list)} items")
    for s in range(min(10, len(load_list))):  # Print first 10 items
        print(f"{s}:{load_list[s][0]}: {load_list[s][1]}")

    if key_type != "hash":
        for s in range(0, len(load_list)):
            k = client.execute_command(*["JSON.GET", load_list[s][0], "$"])
            print(f"{s}:{load_list[s][0]}:  ", k)
    return len(load_list)

def load_data_cluster(cluster_client, test_case, data_set, key_type):
    data = compute_data_sets()

    primary0 = test_case.new_client_for_primary(0)
    for create_cmd in data[data_set][CREATES_KEY(key_type)]:
        primary0.execute_command(create_cmd)

    for key, fields in data[data_set][SETS_KEY(key_type)]:
        if key_type == "hash":
            cluster_client.hset(key, mapping=fields)
        else:
            cluster_client.execute_command(
                "JSON.SET", key, "$", json.dumps(fields)
            )

    print(f"cluster load completed {data_set} {key_type}")

def extract_vocab_from_text_data(dataset_name, key_type):
    """Extract unique words from TEXT fields in a text data set."""
    data = compute_text_data_sets(dataset_name)
    
    vocab = set()
    for _, fields in data[dataset_name][SETS_KEY(key_type)]:
        for field in TEXT_SCHEMA['text']:
            if field in fields:
                words = fields[field].split()
                vocab.update(words)
    return sorted(list(vocab))

def extract_tag_values_from_text_data(dataset_name, key_type):
    """Extract unique tag values from TAG fields in a text data set."""
    data = compute_text_data_sets(dataset_name)
    
    tag_values = {}
    for _, fields in data[dataset_name][SETS_KEY(key_type)]:
        for field in TEXT_SCHEMA['tag']:
            if field in fields:
                if field not in tag_values:
                    tag_values[field] = set()
                tag_values[field].add(fields[field])
    return {k: sorted(list(v)) for k, v in tag_values.items()}

def extract_numeric_ranges_from_text_data(dataset_name, key_type):
    """Extract min/max ranges from NUMERIC fields in a text data set."""
    data = compute_text_data_sets(dataset_name)
    
    numeric_ranges = {}
    for _, fields in data[dataset_name][SETS_KEY(key_type)]:
        for field in TEXT_SCHEMA['numeric']:
            if field in fields:
                value = int(fields[field])
                if field not in numeric_ranges:
                    numeric_ranges[field] = [value, value]
                else:
                    numeric_ranges[field][0] = min(numeric_ranges[field][0], value)
                    numeric_ranges[field][1] = max(numeric_ranges[field][1], value)
    return {k: tuple(v) for k, v in numeric_ranges.items()}

def extract_vocab_by_field_from_text_data(dataset_name, key_type):
    """Extract vocabularies per field from TEXT fields in a text data set.
    
    Returns:
        dict of {field_name: [unique_words]}
    """
    if dataset_name not in TEXT_DATASETS:
        raise ValueError(f"Unknown dataset: {dataset_name}")
    
    config = TEXT_DATASETS[dataset_name]
    field_values = config['field_values']
    schema = config['schema']
    
    vocab_by_field = {}
    for field in schema.get('text', []):
        if field in field_values:
            vocab_by_field[field] = field_values[field]
    
    return vocab_by_field
