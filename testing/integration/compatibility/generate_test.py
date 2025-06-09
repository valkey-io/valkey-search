import pytest, traceback
import sys, json, os
import numpy as np
import pickle
from data_sets import compute_data_sets, ClientSystem, SETS_KEY, CREATES_KEY, VECTOR_DIM, NUM_KEYS, ANSWER_FILE_NAME
'''
Compare two systems for a bunch of aggregation operations
'''
TEST_MARKER = "*" * 100

encoder = lambda x: x.encode() if not isinstance(x, bytes) else x

SYSTEM_R_ADDRESS = ('localhost', 6379)
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

#@pytest.mark.parametrize("key_type", ["hash", "json"])
@pytest.mark.parametrize("key_type", ["hash"])
class TestAggregateCompatibility:

    @classmethod
    def setup_class(cls):
        if os.system(f"docker run --name Generate-search -p {SYSTEM_R_ADDRESS[1]}:6379 redis/redis-stack-server"):
            print("Failed to start Redis Stack server, please check your Docker setup.")
            sys.exit(1)
        cls.data = compute_data_sets()
        cls.answer_file = open(ANSWER_FILE_NAME, "wb")
        cls.generating_answers = True
        cls.client = ClientRSystem()

    @classmethod
    def teardown_class(cls):
        os.system(f"docker stop Generate-search")
    def setup_method(self):
        self.client.execute_command("FLUSHALL SYNC")

    ### Helper Functions ###
    def setup_data(self, data_set, key_type):
        self.loaded_data_set = data_set
        print("load_data with index", data_set, " ", key_type);
        load_list = self.data[data_set][SETS_KEY(key_type)]
        print(f"Start Loading Data, data set has {len(load_list)} records")
        print("Doing: ", self.data[data_set][CREATES_KEY(key_type)])
        for create_index_cmd in self.data[data_set][CREATES_KEY(key_type)]:
            self.client.execute_command(create_index_cmd)

        # Make large chunks to accelerate things
        batch_size = 50
        for s in range(0, len(load_list), batch_size):
            pipe = self.client.pipeline()
            for cmd in load_list[s : s + batch_size]:
                if key_type == "hash":
                    pipe.hset(cmd[0], mapping=cmd[1])
                else:
                    pipe.execute_command(*["JSON.SET", cmd[0], "$", json.dumps(cmd[1])])
            pipe.execute()

        self.client.wait_for_indexing_done()
        print(f"setup_data completed {data_set} {key_type}")
        return len(load_list)

    def execute_command(self, cmd):
        answer = {"cmd": cmd, "data_set": self.data_set, "traceback": "".join(traceback.format_stack())}
        try:
            answer["result"] = self.client.execute_command(*cmd)
            answer["exception"] = False
            exception = None
            # print(f"{name} replied: {rs}")
        except Exception as exc:
            print(f"Got exception for Error: '{exc}', Cmd:{cmd}")
            answer["result"] = {}
            answer["exception"] = True

        pickle.dump(answer, self.answer_file)
    
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
        self.execute_command(new_cmd)
    '''
    def test_search_reverse(self, key_type):
        self.setup_data("reverse vector numbers", key_type)
        self.checkvec(f"ft.search {key_type}_idx1 *")
        self.checkvec(f"ft.search {key_type}_idx1 * limit 0 5")

    def test_search(self, key_type):
        self.setup_data("sortable numbers", key_type)
        self.checkvec(f"ft.search {key_type}_idx1 *")
        self.checkvec(f"ft.search {key_type}_idx1 * limit 0 5")
    '''

    def test_aggregate_sortby(self, key_type):
        self.setup_data("sortable numbers", key_type)
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
        self.setup_data("sortable numbers", key_type)
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
        keys = self.setup_data("sortable numbers", key_type)
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 limit 1 4")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 limit 1 4 sortby 2 @__key desc")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key desc limit 1 4")
    '''
    def test_aggregate_short_limit(self, key_type):
        keys = self.setup_data("sortable numbers", key_type)
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 limit 0 5")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key desc")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key desc limit 0 5")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key asc limit 1 4", knn=4)

    def test_aggregate_load(self, key_type):
        keys = self.setup_data("sortable numbers", key_type)
        self.checkvec(f"ft.aggregate {key_type}_idx1  *")
        self.checkvec(f"ft.aggregate {key_type}_idx1  * load *")

    def test_aggregate_numeric_dyadic_operators(self, key_type):
        keys = self.setup_data("hard numbers", key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        logops = ["||", "&&"]
        for op in dyadic + relops + logops:
            self.checkvec(
                f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 apply @n1{op}@n2 as nn"
            )

    def test_aggregate_numeric_triadic_operators(self, key_type):
        keys = self.setup_data("hard numbers", key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        logops = ["||", "&&"]
        for op1 in dyadic+relops+logops:
            for op2 in dyadic+relops+logops:
                self.checkvec(
                    f"ft.aggregate {key_type}_idx1  * load 4 @__key @n1 @n2 @n3 apply @n1{op1}@n2{op2}@n3 as nn apply (@n1{op1}@n2) as nn1"
                )

    def test_aggregate_numeric_functions(self, key_type):
        keys = self.setup_data("hard numbers", key_type)
        function = ["log", "abs", "ceil", "floor", "log2", "exp", "sqrt"]
        for f in function:
            self.checkvec(
                f"ft.aggregate {key_type}_idx1  * load 2 @__key @n1 apply {f}(@n1) as nn"
            )

    def test_aggregate_string_apply_functions(self, key_type):
        self.setup_data("hard numbers", key_type)

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
        self.setup_data("hard numbers", key_type)
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
        self.setup_data("hard numbers", key_type)
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