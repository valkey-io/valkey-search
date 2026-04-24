import pytest, traceback, valkey, time, struct
import sys, os
import pickle
import gzip
from . import data_sets
from .data_sets import *
from valkey.exceptions import ConnectionError
'''
Capture answer from Redisearch
'''
TEST_MARKER = "*" * 100

encoder = lambda x: x.encode() if not isinstance(x, bytes) else x

SYSTEM_R_ADDRESS = ('localhost', 6380)
class ClientRSystem(ClientSystem):
    def __init__(self):
        super().__init__(SYSTEM_R_ADDRESS)
        try:
            self.client.execute_command("FT.CONFIG SET TIMEOUT 0")
        except:
            pass

    def wait_for_indexing_done(self, index_name):
        '''Wait for indexing to be done.
        indexing = True
        while indexing:
            try:
                indexing = self.ft_info(index_name)["indexing"]
            except redis.ConnectionError:
                print("failed")
                assert False
                '''
        print("Indexing is done.")

class BaseCompatibilityTest:
    """Base class for compatibility tests with shared infrastructure."""
    
    # Subclasses must define this
    ANSWER_FILE_NAME = None
    
    @classmethod
    def setup_class(cls):
        if cls.ANSWER_FILE_NAME is None:
            raise NotImplementedError("Subclass must define ANSWER_FILE_NAME")
            
        if os.system("docker run --rm -d --name Generate-search -p 6380:6379 redis/redis-stack-server") != 0:
            print("Failed to start Redis Stack server, please check your Docker setup.")
            sys.exit(1)
        print("Started Generate-search server")
        cls.answers = []
        # add reply count to check redis non-empty answer
        cls.replied_count = 0
        cls.client = ClientRSystem()
        while True:
            try:
                cls.client.execute_command("PING")
                break
            except ConnectionError:
                print("Waiting for R system to be ready...")
                time.sleep(.25)
        print("Done initializing")

    @classmethod
    def teardown_class(cls):
        print("Stopping Generate-search server")
        os.system("docker stop Generate-search")
        print("Dumping ", len(cls.answers), " answers")
        with gzip.open(cls.ANSWER_FILE_NAME, "wb") as answer_file:
            pickle.dump(cls.answers, answer_file)

    def setup_method(self):
        self.client.execute_command("FLUSHALL SYNC")
        time.sleep(1)

    def setup_data(self, data_set_name, key_type):
        self.data_set_name = data_set_name
        self.key_type = key_type
        load_data(self.client, data_set_name, key_type)

    def execute_command(self, cmd):
        answer = {"cmd": cmd,
                  "key_type": self.key_type,
                  "data_set_name": self.data_set_name,
                  "testname": os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0],
                  "traceback": "".join(traceback.format_stack())}
        try:
            print("Cmd:", *cmd)
            answer["result"] = self.client.execute_command(*cmd)
            answer["exception"] = False
            if answer["result"] != [0]:
                self.__class__.replied_count += 1  # ADD THIS LINE
            print(f"replied: {answer['result']} (count: {self.__class__.replied_count})")
            # print(f"replied: {answer['result']}")
        except Exception as exc:
            print(f"Got exception for Error: '{exc}', Cmd:{cmd}")
            answer["result"] = {}
            answer["exception"] = True
        self.answers.append(answer)

    def check(self, *orig_cmd):
        """Non-vector command."""
        cmd = orig_cmd[0].split() if len(orig_cmd) == 1 else [*orig_cmd]
        self.execute_command(cmd)

@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["json", "hash"])
class TestAggregateCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "aggregate-answers.pickle.gz"

    def checkrange(self, dialect, *orig_cmd, radius=1.0,
                   query_vector=[0] * VECTOR_DIM, field="v1",
                   extra_params="", query_attrs="", negate=False):
        """Build and execute a VECTOR_RANGE query.

        The first ``*`` in *orig_cmd* is replaced with the range clause.
        ``extra_params`` can be e.g. ``"EF_RUNTIME 100"`` or ``"AS dist"``.
        ``query_attrs`` can be e.g. ``"{$yield_distance_as: dist}"`` and will
        be prepended with ``=>`` before the ``@field:`` clause.
        ``negate`` prepends ``-`` to the range clause.
        """
        cmd = orig_cmd[0].split() if len(orig_cmd) == 1 else [*orig_cmd]
        range_clause = f"@{field}:[VECTOR_RANGE $RADIUS $BLOB {extra_params}]".strip()
        if query_attrs:
            range_clause = f"{query_attrs}=>{range_clause}"
        if negate:
            range_clause = f"-{range_clause}"
        new_cmd = []
        did_one = False
        for c in cmd:
            if c.strip() == "*" and not did_one:
                new_cmd.append(range_clause)
                did_one = True
            else:
                new_cmd.append(c)
        new_cmd += [
            "PARAMS", "4",
            "BLOB", struct.pack(f"<{VECTOR_DIM}f", *query_vector),
            "RADIUS", str(radius),
            "DIALECT", str(dialect),
        ]
        self.execute_command(new_cmd)

    def checkvec(self, dialect, *orig_cmd, knn=10000, score_as="", query_vector=[0] * VECTOR_DIM):
        '''Check vector queries only.'''
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
            struct.pack(f"<{VECTOR_DIM}f", *query_vector),
            "DIALECT",
            str(dialect),
        ]
        self.execute_command(new_cmd)
    def check(self, dialect, *orig_cmd):
        '''Check Non-vector queries. Doesn't have support for '*' yet. '''
        cmd = orig_cmd[0].split() if len(orig_cmd) == 1 else [*orig_cmd]
        for query in ["@n1:[-inf inf]", "@t1:{aaaaaaa*}", "-@n1:[-inf inf]", "-@t1:{aaaaaa*}"]:
            new_cmd = []
            did_one = False
            for c in cmd:
                if c.strip() == "*" and not did_one:
                    ''' substitute '''
                    new_cmd += [query]
                    did_one = True
                else:
                    new_cmd += [c]
            new_cmd += [
                "DIALECT",
                str(dialect),
            ]
            self.execute_command(new_cmd)

    def checkall(self, dialect, *orig_cmd, **kwargs):
        '''Non-vector commands. Doesn't have support for '*' yet. '''
        self.checkvec(self, dialect, orig_cmd, kwargs)
        self.check(self, dialect, orig_cmd)

    def test_bad_numeric_data(self, key_type, dialect):
        self.setup_data("bad numbers", key_type)
        self.check(dialect, f"ft.search {key_type}_idx1",  "@n1:[-inf inf]")
        self.check(dialect, f"ft.search {key_type}_idx1", "-@n1:[-inf inf]")
        self.check(dialect, f"ft.search {key_type}_idx1",  "@n2:[-inf inf]")
        self.check(dialect, f"ft.search {key_type}_idx1", "-@n2:[-inf inf]")

    def test_search_reverse(self, key_type, dialect):
        self.setup_data("reverse vector numbers", key_type)
        self.checkall(dialect, f"ft.search {key_type}_idx1 *")
        self.checkall(dialect, f"ft.search {key_type}_idx1 * limit 0 5")

    def test_search(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.checkall(dialect, f"ft.search {key_type}_idx1 *")
        self.checkall(dialect, f"ft.search {key_type}_idx1 * limit 0 5")
    
@pytest.mark.parametrize("algo", ["flat", "hnsw"])
    @pytest.mark.parametrize("metric", ["l2", "ip", "cosine"])
    def test_vector_distance(self, key_type, dialect, algo, metric):
        self.setup_data(f"vector data {metric} {algo}", key_type)
        vector_points = [-.75, .75]
        for x in vector_points:
            for y in vector_points:
                for z in vector_points:
                    self.checkvec(dialect, f"ft.aggregate {key_type}_idx1 * load 1 __key", query_vector=[x, y, z])
                    self.checkvec(dialect, f"ft.aggregate {key_type}_idx1 * load 2 __v1_score __key", query_vector=[x, y, z])
                    self.checkvec(dialect, f"ft.search {key_type}_idx1 *", query_vector=[x, y, z])
    def test_aggregate_sortby(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 2 @__key @n2 sortby 1 @n2")
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 1 @n2")
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @n2 asc"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @n2 desc"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @__key desc"
        )
        self.checkvec(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 VECTORDISTANCE sortby 2 @VECTORDISTANCE desc"
        , score_as="AS VECTORDISTANCE")
        self.checkvec(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 sortby 2 @__v1_score asc"
        )

    def test_aggregate_groupby(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @n1")
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1")
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce count 0 as count"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce count 0 as count"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce COUNT 0 as count"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce CoUnT 0 as count"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce sum 1 @n1 as sum"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce sum 1 @n1 as sum"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce sum 1 @n2 as sum"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce avg 1 @n1 as avg"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce avg 1 @n1 as avg"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce avg 1 @n2 as avg"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce min 1 @n1 as min"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce min 1 @n2 as min"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce min 1 @n1 as min"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce min 1 @n2 as min"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce stddev 1 @n1 as nstddev"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce stddev 1 @n1 as nstddev"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce stddev 1 @n2 as nstddev"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce max 1 @n1 as nmax"
        )
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce max 1 @n1 as nmax"
        )
        self.check(dialect, f'ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce max 1 @n2 as nmax')
    def test_aggregate_limit(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.check(dialect, f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2")
        self.check(dialect, f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key asc limit 1 4 ")
        self.check(dialect, f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key desc limit 1 4")

    def test_aggregate_short_limit(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.checkvec(dialect, f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 limit 0 5")
        self.check(dialect, f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key desc")
        self.check(dialect, f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key desc limit 0 5")
        self.checkvec(dialect, f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 sortby 2 @__key asc limit 1 4", knn=4)

    def test_aggregate_load(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.checkvec(dialect, f"ft.aggregate {key_type}_idx1  *")
        self.checkvec(dialect, f"ft.aggregate {key_type}_idx1  * load *")

    def test_aggregate_numeric_dyadic_operators(self, key_type, dialect):
        self.setup_data("hard numbers", key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        logops = ["||", "&&"] if dialect == 2 else []
        for op in dyadic + relops + logops:
            self.check(dialect, 
                f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 apply @n1{op}@n2 as nn"
            )
    def test_aggregate_numeric_dyadic_operators_sortable_numbers(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        logops = ["||", "&&"] if dialect == 2 else []
        for op in dyadic + relops + logops:
            self.check(dialect, 
                f"ft.aggregate {key_type}_idx1  * load 3 @__key @n1 @n2 apply @n1{op}@n2 as nn"
            )

    def test_aggregate_numeric_triadic_operators(self, key_type, dialect):
        self.setup_data("hard numbers", key_type)
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        logops = ["||", "&&"] if dialect == 2 else []
        for op1 in dyadic+relops+logops:
            for op2 in dyadic+relops+logops:
                self.check(dialect, 
                    f"ft.aggregate {key_type}_idx1  * load 4 @__key @n1 @n2 @n3 apply @n1{op1}@n2{op2}@n3 as nn apply (@n1{op1}@n2) as nn1"
                )

    def test_aggregate_numeric_functions(self, key_type, dialect):
        self.setup_data("hard numbers", key_type)
        function = ["log", "abs", "ceil", "floor", "log2", "exp", "sqrt"]
        for f in function:
            self.check(dialect, 
                f"ft.aggregate {key_type}_idx1  * load 2 @__key @n1 apply {f}(@n1) as nn"
            )

    @pytest.mark.parametrize("dataset", ["hard numbers", "hard strings"])
    def test_aggregate_string_apply_functions(self, key_type, dialect, dataset):
        self.setup_data(dataset, key_type)

        # String apply function "contains"
        self.check(dialect, 
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
        self.check(dialect, 
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
        self.check(dialect, 
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
        self.check(dialect, 
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
        self.check(dialect, 
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
        self.check(dialect, 
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
        self.check(dialect, 
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

    @pytest.mark.parametrize("dataset", ["hard numbers", "hard strings"])
    def test_aggregate_substr(self, key_type, dialect, dataset):
        self.setup_data(dataset, key_type)
        for offset in [0, 1, 2, 100, -1, -2, -3, -1000]:
            for len in [0, 1, 2, 100, -1, -2, -3, -1000]:
                self.check(dialect, 
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

    def test_aggregate_dyadic_ops(self, key_type, dialect):
        self.setup_data("hard numbers", key_type)
        values = ["-inf", "-1.5", "-1", "-0.5", "0", "0.5", "1.0", "+inf"]
        dyadic = ["+", "-", "*", "/", "^"]
        relops = ["<", "<=", "==", "!=", ">=", ">"]
        logops = ["||", "&&"]
        for lop in values:
            for rop in values:
                for op in dyadic+relops+logops:
                    self.check(dialect, 
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

    def test_search_sortby(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)

        for sort_key in ["n1", "n2"]:
            for direction in ["ASC", "DESC", ""]:
                for return_keys in ["", "RETURN 3 @n1 @t1"]:
                    for wsk in ["", "WITHSORTKEYS"]:
                        for limit in ["LIMIT 0 5", "LIMIT 2 3", ""]:
                            self.check(dialect, f"ft.search {key_type}_idx1 * SORTBY {sort_key} {direction} {return_keys} {limit} {wsk}")

    @pytest.mark.parametrize("algo", ["flat", "hnsw"])
    @pytest.mark.parametrize("metric", ["l2", "ip", "cosine"])
    def test_vector_range_basic(self, key_type, dialect, algo, metric):
        """Standalone VECTOR_RANGE across algos/metrics with varying radii."""
        self.setup_data(f"vector data {metric} {algo}", key_type)
        vector_points = [-.75, .75]
        radii = [0, 0.5, 2.0, 100.0]
        for x in vector_points:
            for y in vector_points:
                for z in vector_points:
                    for r in radii:
                        self.checkrange(
                            dialect,
                            f"ft.search {key_type}_idx1 *",
                            radius=r, query_vector=[x, y, z],
                        )

    @pytest.mark.parametrize("algo", ["flat", "hnsw"])
    @pytest.mark.parametrize("metric", ["l2", "ip", "cosine"])
    def test_vector_range_nocontent(self, key_type, dialect, algo, metric):
        """VECTOR_RANGE with NOCONTENT returns only keys."""
        self.setup_data(f"vector data {metric} {algo}", key_type)
        for r in [0.5, 5.0]:
            self.checkrange(
                dialect,
                f"ft.search {key_type}_idx1 * NOCONTENT",
                radius=r, query_vector=[0.75, 0.75, 0.75],
            )

    def test_vector_range_and_numeric(self, key_type, dialect):
        """VECTOR_RANGE combined with numeric filter via AND."""
        self.setup_data("sortable numbers", key_type)
        for r in [5.0, 50.0]:
            self.checkrange(
                dialect,
                f"ft.search {key_type}_idx1 * @n1:[0 +inf] NOCONTENT",
                radius=r,
            )

    def test_vector_range_and_tag(self, key_type, dialect):
        """VECTOR_RANGE combined with tag filter via AND."""
        self.setup_data("sortable numbers", key_type)
        self.checkrange(
            dialect,
            f"ft.search {key_type}_idx1 * @t3:{{all_the_same_value}} NOCONTENT",
            radius=50,
        )

    def test_vector_range_or_numeric(self, key_type, dialect):
        """VECTOR_RANGE combined with numeric filter via OR."""
        self.setup_data("sortable numbers", key_type)
        self.checkrange(
            dialect,
            f"ft.search {key_type}_idx1 * | @n1:[0 +inf] NOCONTENT",
            radius=1,
        )

    def test_vector_range_negate(self, key_type, dialect):
        """Negated VECTOR_RANGE returns complement."""
        self.setup_data("sortable numbers", key_type)
        for r in [5.0, 50.0]:
            self.checkrange(
                dialect,
                f"ft.search {key_type}_idx1 * NOCONTENT",
                radius=r, negate=True,
            )

    def test_vector_range_sortby(self, key_type, dialect):
        """VECTOR_RANGE with SORTBY overrides default distance ordering."""
        self.setup_data("sortable numbers", key_type)
        for sort_key in ["n1", "n2"]:
            for direction in ["ASC", "DESC"]:
                self.checkrange(
                    dialect,
                    f"ft.search {key_type}_idx1 * SORTBY {sort_key} {direction}",
                    radius=50,
                )

    def test_vector_range_limit(self, key_type, dialect):
        """VECTOR_RANGE with LIMIT returns the correct subset."""
        self.setup_data("sortable numbers", key_type)
        for offset, count in [(0, 3), (2, 5), (0, 0)]:
            self.checkrange(
                dialect,
                f"ft.search {key_type}_idx1 * LIMIT {offset} {count} NOCONTENT",
                radius=50,
            )

    def test_vector_range_return(self, key_type, dialect):
        """VECTOR_RANGE with RETURN limits returned fields."""
        self.setup_data("sortable numbers", key_type)
        self.checkrange(
            dialect,
            f"ft.search {key_type}_idx1 * RETURN 2 n1 __v1_score",
            radius=50,
        )

    def test_vector_range_yield_distance_as(self, key_type, dialect):
        """VECTOR_RANGE with $yield_distance_as query attribute."""
        self.setup_data("sortable numbers", key_type)
        self.checkrange(
            dialect,
            f"ft.search {key_type}_idx1 * RETURN 2 n1 my_dist",
            radius=50,
            query_attrs="{$yield_distance_as: my_dist}",
        )

    def test_vector_range_epsilon(self, key_type, dialect):
        """VECTOR_RANGE with $epsilon query attribute."""
        self.setup_data("sortable numbers", key_type)
        self.checkrange(
            dialect,
            f"ft.search {key_type}_idx1 * NOCONTENT",
            radius=50, query_vector=[0] * VECTOR_DIM,
            query_attrs="{$epsilon: 0.5}",
        )

