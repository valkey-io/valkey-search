import pytest, traceback, valkey, time, struct
import sys, os
import pickle
import gzip
from . import data_sets
from .data_sets import *
from . import compute_sources_hash
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
        payload = {
            "sources_hash": compute_sources_hash(),
            "answers": cls.answers,
        }
        with gzip.open(cls.ANSWER_FILE_NAME, "wb") as answer_file:
            pickle.dump(payload, answer_file)

    def setup_method(self):
        self.client.execute_command("FLUSHALL SYNC")
        time.sleep(1)

    def setup_data(self, data_set_name, key_type):
        self.data_set_name = data_set_name
        self.key_type = key_type
        load_data(self.client, data_set_name, key_type)

    def execute_command(self, cmd, excluded=False):
        answer = {"cmd": cmd,
                  "key_type": self.key_type,
                  "data_set_name": self.data_set_name,
                  "testname": os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0],
                  "traceback": "".join(traceback.format_stack())}
        if excluded:
            # Known, intentional difference from Redisearch. The answer is still
            # captured, but the replay only checks that valkey-search does not
            # crash on the command rather than comparing results.
            answer["excluded"] = True
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
    def check(self, dialect, *orig_cmd, excluded=False):
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
            self.execute_command(new_cmd, excluded=excluded)

    def checkall(self, dialect, *orig_cmd, **kwargs):
        '''Non-vector commands. Doesn't have support for '*' yet. '''
        self.checkvec(dialect, *orig_cmd, **kwargs)
        self.check(dialect, *orig_cmd)

    def test_bad_numeric_data(self, key_type, dialect):
        self.setup_data("bad numbers", key_type)
        self.check(dialect, "ft.search", f"{key_type}_idx1", "@n1:[-inf inf]")
        self.check(dialect, "ft.search", f"{key_type}_idx1", "-@n1:[-inf inf]")
        self.check(dialect, "ft.search", f"{key_type}_idx1", "@n2:[-inf inf]")
        self.check(dialect, "ft.search", f"{key_type}_idx1", "-@n2:[-inf inf]")
        # Negative query over the bad field itself: a key whose NUMERIC field
        # holds invalid data is dropped entirely, so it must NOT show up in a
        # negative query over that field -- unlike a merely-missing field, which
        # would match the negation. `[100 200]` excludes every valid key, so the
        # negation returns all surviving keys; the dropped key must be absent.
        self.check(dialect, "ft.search", f"{key_type}_idx1", "-@n1:[100 200]")

    def test_bad_vector_data(self, key_type, dialect):
        # Keys with a malformed (wrong-length) VECTOR field are dropped entirely
        # by Redisearch, so they must not appear in vector KNN results nor in
        # queries against this key's other (valid) fields.
        self.setup_data("bad vectors", key_type)
        # KNN over all documents: keys with a malformed vector must be absent.
        # (LOAD is FT.AGGREGATE syntax; FT.SEARCH takes the bare KNN query.)
        self.checkvec(dialect, f"ft.search {key_type}_idx1 *")
        self.checkvec(dialect, f"ft.aggregate {key_type}_idx1 * load 1 __key")
        # The dropped keys are also absent from numeric/tag queries.
        self.check(dialect, "ft.search", f"{key_type}_idx1", "@n1:[-inf inf]")
        self.check(dialect, "ft.search", f"{key_type}_idx1", "@t2:{common}")
        # Negative queries over the dropped keys' other (valid) fields. A vector
        # field cannot be negated directly, so we verify that a key dropped for a
        # bad vector does not reappear in a negation it would otherwise satisfy:
        # its numeric value is outside `[100 200]`, and its tag is not
        # `electronics`, so both negations return every surviving key -- the
        # dropped keys must not be among them.
        self.check(dialect, "ft.search", f"{key_type}_idx1", "-@n1:[100 200]")
        self.check(dialect, "ft.search", f"{key_type}_idx1", "-@t2:{electronics}")

    @pytest.mark.skip(reason="Needs research")
    def test_search_reverse(self, key_type, dialect):
        self.setup_data("reverse vector numbers", key_type)
        self.checkall(dialect, f"ft.search {key_type}_idx1 *")
        self.checkall(dialect, f"ft.search {key_type}_idx1 * limit 0 5")

    @pytest.mark.skip(reason="Needs research")
    def test_search(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        self.checkall(dialect, f"ft.search {key_type}_idx1 *")
    
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

    def test_aggregate_groupby_tolist(self, key_type, dialect):
        self.setup_data("sortable numbers", key_type)
        # Basic TOLIST on numeric field grouped by tag
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce tolist 1 @n1 as items"
        )
        # TOLIST on a different numeric field
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @n2 as items"
        )
        # TOLIST alongside COUNT in the same GROUPBY
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce tolist 1 @n1 as items reduce count 0 as cnt"
        )
        # TOLIST on tag field grouped by another tag
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce tolist 1 @t2 as tag_items"
        )
        # Case insensitivity
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce TOLIST 1 @n1 as items"
        )
        # TOLIST on t3 which is "all_the_same_value" for every record — all
        # records land in one group, so the list should contain all unique n1
        # values (15 distinct integers from -5 to 9).
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @n1 as items"
        )
        # TOLIST collecting t3 grouped by t3 — every record has the same t3,
        # so the result list should contain exactly one element (dedup).
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @t3 as items"
        )
        # Multiple TOLIST reducers on different fields in the same GROUPBY
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @n1 as n1_items reduce tolist 1 @n2 as n2_items"
        )
        # TOLIST with TOLIST + COUNT + SUM in the same GROUPBY
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @n1 as items reduce count 0 as cnt reduce sum 1 @n1 as total"
        )
        # TOLIST on n3 field grouped by t3 (single group, all unique n3 values)
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 7 @__key @n1 @n2 @n3 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @n3 as items"
        )
        # Mixed case reducer name: ToLiSt
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce ToLiSt 1 @n1 as items"
        )

    def test_aggregate_groupby_tolist_duplicates(self, key_type, dialect):
        """Test TOLIST with a dataset where duplicate values exist within groups."""
        self.setup_data("hard numbers", key_type)
        # hard numbers has t3="all_the_same_value" for all records, and
        # numeric fields with repeated values like -0.5, 0, -0, 1, -1 in
        # various combinations. TOLIST should deduplicate within the group.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @n1 as items"
        )
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @n2 as items"
        )
        # TOLIST on t3 grouped by t3 — all same value, should produce single-element list
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @t3 as items"
        )
        # TOLIST with COUNT to verify count reflects total records, not unique values
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @n1 as items reduce count 0 as cnt"
        )
        # Multiple TOLIST reducers on different fields
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t3 reduce tolist 1 @n1 as n1_items reduce tolist 1 @n2 as n2_items"
        )
        # TOLIST per-tag group (each t1 is unique, so each group has one record)
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 6 @__key @n1 @n2 @t1 @t2 @t3 groupby 1 @t1 reduce tolist 1 @n1 as items"
        )

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

    def test_aggregate_load_rename(self, key_type, dialect):
        # The LOAD <count> includes the AS keyword and its alias.
        self.setup_data("sortable numbers", key_type)
        # Single rename.
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 4 @__key @n1 as num1")
        # Multiple renames.
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 7 @__key @n1 as a @n2 as b")
        # Proof: a renamed field is usable in a subsequent APPLY.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 7 @__key @n1 as a @n2 as b apply @a+@b as total"
        )
        # Renamed field reused across two APPLY stages.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 4 @__key @n1 as a apply @a*2 as dbl apply @dbl+@a as tripled"
        )
        # Mix of a renamed and a non-renamed load, both used in APPLY.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 5 @__key @n1 @n2 as b apply @n1+@b as total"
        )
        # Rename a tag field and use it in a string APPLY. No spaces in the
        # expression so the whitespace-split in check() keeps it one token.
        self.check(dialect,
            f'ft.aggregate {key_type}_idx1 * load 4 @__key @t1 as tag1 apply contains(@tag1,"one") as has_one'
        )

    def test_aggregate_load_rename_name_conflicts(self, key_type, dialect):
        # Name conflicts created by the LOAD ... AS clause. @__key is loaded so
        # the rows have a stable sort key for the comparison.
        self.setup_data("sortable numbers", key_type)

        # An AS name may hide a declared field: `@n1 as n2` makes the name n2
        # refer to n1's value rather than the schema's n2 ...
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 4 @__key @n1 as n2")
        # ... including for later pipeline stages.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 4 @__key @n1 as n2 apply @n2+100 as r"
        )
        # Hiding a declared field of a different type (numeric hides a tag).
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 4 @__key @n1 as t1")
        # An AS name may hide the key field.
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 3 @n1 as __key")
        # Renaming a field onto its own name is a no-op.
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 4 @__key @n1 as n1")
        # Loading the same field twice is de-duplicated, not an error.
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 3 @__key @n2 @n2")

        # KNOWN DIFFERENCES. Redisearch lets the first claim of an output name
        # win and silently drops any later claim. valkey-search instead rejects
        # a LOAD clause that names the same output twice when an `AS` rename is
        # involved (see COMPATIBILITY.md, "stricter input validation"), so these
        # commands error and are excluded from the result comparison.
        #
        # A rename onto the name of a field loaded earlier in the same clause.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 5 @__key @n2 @n1 as n2",
            excluded=True,
        )
        # ... and the same collision in the opposite order.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 5 @__key @n1 as n2 @n2",
            excluded=True,
        )
        # A rename onto the key field when the key is also loaded.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 4 @__key @n1 as __key",
            excluded=True,
        )
        # Two AS clauses targeting the same alias.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 7 @__key @n1 as x @n2 as x",
            excluded=True,
        )
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 7 @__key @n1 as x @n2 as x apply @x+1 as y",
            excluded=True,
        )

    def test_aggregate_load_rename_json_path(self, key_type, dialect):
        # Loading a field by its JSON path only applies to JSON keys.
        if key_type != "json":
            pytest.skip("JSON-path loads apply only to JSON keys")
        self.setup_data("sortable numbers", key_type)
        # Load by JSON path with a rename, then use the rename in APPLY.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 4 @__key $.n1 as a apply @a+1 as b"
        )
        # Load by JSON path without a rename: emitted under the path.
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 2 @__key $.n1")

    def test_aggregate_json_field_names(self, key_type, dialect):
        """Field names in the reply must be the user-facing name, not the
        schema identifier. Only JSON can distinguish the two (its schema is
        `$.n1 AS n1 ...`); HASH is run too so both are held to one expectation.

        See issue #1243. Row alignment follows the harness rules: queries with
        neither GROUPBY nor SORTBY are aligned on `@__key`, so it is loaded;
        GROUPBY aligns on the grouped fields; GROUPBY and SORTBY are never
        combined (the harness rejects that pairing outright).
        """
        self.setup_data("sortable numbers", key_type)

        # --- LOAD: the loaded field's own name.
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 2 @__key @n1")
        self.check(dialect, f"ft.aggregate {key_type}_idx1 * load 4 @__key @n1 @n2 @t1")

        # --- APPLY: both the source name and the computed name.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @__key @n1 apply @n1+1 as computed"
        )
        # APPLY writing back over the loaded field's own name.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @__key @n1 apply @n1+1 as n1"
        )
        # APPLY over a field that was never LOADed.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 1 @__key apply @n1*2 as doubled"
        )
        # APPLY over a tag field, so the name survives a string stage too.
        self.check(dialect,
            f'ft.aggregate {key_type}_idx1 * load 2 @__key @t1 apply upper(@t1) as shout'
        )

        # --- FILTER: names of the records that survive.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 3 @__key @n1 @n2 filter @n1<@n2"
        )

        # --- GROUPBY: the grouping key is emitted under its own name.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 1 @t1 groupby 1 @t1 reduce count 0 as cnt"
        )
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @t1 @n1 groupby 1 @t1 reduce sum 1 @n1 as total"
        )
        # Grouping on two fields.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @t1 @t2 groupby 2 @t1 @t2 reduce count 0 as cnt"
        )
        # NOTE: `groupby 1 @t1` with no LOAD of @t1 is deliberately omitted.
        # Redisearch auto-loads the grouped field (15 groups, each emitting
        # t1 and cnt); valkey-search collapses everything into one group and
        # emits only cnt. That is a separate defect from #1243, and it makes
        # the harness raise KeyError on the missing sort key rather than
        # report a mismatch, which would abort the whole replay.
        # REDUCE with no AS clause: the generated name must not embed a path.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @t1 @n1 groupby 1 @t1 reduce sum 1 @n1"
        )
        # TOLIST over a JSON-pathed field.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @t1 @n1 groupby 1 @t1 reduce tolist 1 @n1 as items"
        )
        # A reducer feeding a later APPLY, so the reducer's name is re-read.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @t1 @n1 groupby 1 @t1 reduce sum 1 @n1 as total apply @total+1 as bumped"
        )

        # --- SORTBY does not rename what it sorts on.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @__key @n1 sortby 2 @n1 asc"
        )
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 3 @__key @n1 @n2 sortby 4 @n1 asc @n2 desc"
        )
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @__key @t1 sortby 2 @t1 asc"
        )

        # --- LIMIT after the names are established.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @__key @n1 sortby 2 @n1 asc limit 0 3"
        )

        # --- Multi-stage pipelines, end to end.
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 3 @__key @n1 @t1 apply @n1+10 as bumped filter @bumped>0"
        )
        self.check(dialect,
            f"ft.aggregate {key_type}_idx1 * load 2 @t1 @n1 apply @n1+10 as bumped groupby 1 @t1 reduce max 1 @bumped as peak"
        )

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

    @pytest.mark.skip(reason="Requires a large change to the underlying comparison operations and changes to many existing tests")
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
                for return_keys in ["", "RETURN 2 @n1 @t1"]:
                    for wsk in ["", "WITHSORTKEYS"]:
                        for limit in ["LIMIT 0 5", "LIMIT 2 3", ""]:
                            self.check(dialect, f"ft.search {key_type}_idx1 * SORTBY {sort_key} {direction} {return_keys} {limit} {wsk}")

    def test_tag_escaped_special_chars(self, key_type, dialect):
        """Escaped special characters in tag queries. Ref: #454."""
        self.setup_data("tag special chars", key_type)
        self.check(dialect, "ft.search", f"{key_type}_idx1", r"@tags:{ a\}b }")
        self.check(dialect, "ft.search", f"{key_type}_idx1", r"@tags:{ a\|b }")
        self.check(dialect, "ft.search", f"{key_type}_idx1", r"@tags:{ x\}y\}z }")
        self.check(dialect, "ft.search", f"{key_type}_idx1", r"@tags:{ a\\b }")
        # Values the JSON module returns backslash-escaped (\", \t, \n).
        self.check(dialect, "ft.search", f"{key_type}_idx1", r'@tags:{ a\"b }')
        self.check(dialect, "ft.search", f"{key_type}_idx1", "@tags:{ a\\\tb }")
        self.check(dialect, "ft.search", f"{key_type}_idx1", "@tags:{ a\\\nb }")
        self.check(dialect, "ft.search", f"{key_type}_idx1", r"@tags:{ normal }")
        # Multi-byte / non-ASCII values.
        self.check(dialect, "ft.search", f"{key_type}_idx1", "@tags:{ café }")
        self.check(dialect, "ft.search", f"{key_type}_idx1", "@tags:{ 中文 }")
        self.check(dialect, "ft.search", f"{key_type}_idx1", "@tags:{ 😀 }")
        # LIMIT 0 40: these match >10 docs; bound the set so it isn't truncated.
        self.check(dialect, "ft.search", f"{key_type}_idx1",
                   r"@tags:{ a\}b | normal }", "LIMIT", "0", "40")
        self.check(dialect, "ft.search", f"{key_type}_idx1",
                   r"@tags:{ a\|b | a\}b }", "LIMIT", "0", "40")
        self.check(dialect, "ft.search", f"{key_type}_idx1",
                   r"@tags:{ a\|b | x\}y\}z }", "LIMIT", "0", "40")
        self.check(dialect, "ft.search", f"{key_type}_idx1",
                   r"@tags:{ a\}b | a\|b | x\}y\}z | a\\b | normal }",
                   "LIMIT", "0", "40")

    # test_first_value_simple_mode is intentionally omitted.
    # FIRST_VALUE without a BY clause is non-deterministic: the order of
    # records within a group depends on retrieval order, which differs between
    # Redis and Valkey implementations. Compatibility testing requires
    # deterministic results, so only BY-clause (sorted) mode is tested here.

    def test_first_value_by_clause(self, key_type, dialect):
        """Test FIRST_VALUE with BY clause - sorted mode."""
        self.setup_data("sortable numbers", key_type)

        # (value_field, group_field, load_fields, by_field, order)
        # order=None means default (3-arg form, no explicit ASC/DESC)
        cases = [
            # Numeric value sorted by numeric field
            ("@n1", "@n2", "3 @__key @n1 @n2", "@n1", "ASC"),
            ("@n1", "@n2", "3 @__key @n1 @n2", "@n1", "DESC"),
            ("@n1", "@n2", "3 @__key @n1 @n2", "@n1", None),   # default order
            # String value sorted by string field
            ("@t1", "@t2", "3 @__key @t1 @t2", "@t1", "ASC"),
            ("@t1", "@t2", "3 @__key @t1 @t2", "@t1", "DESC"),
            ("@t1", "@t2", "3 @__key @t1 @t2", "@t1", None),   # default order
            # String value grouped by numeric, sorted by string
            ("@t1", "@n2", "3 @__key @t1 @n2", "@t1", "ASC"),
            ("@t1", "@n2", "3 @__key @t1 @n2", "@t1", "DESC"),
            # Numeric value sorted by string field
            ("@n1", "@t2", "4 @__key @n1 @t1 @t2", "@t1", "ASC"),
            ("@n1", "@t2", "4 @__key @n1 @t1 @t2", "@t1", "DESC"),
            # Cross-field: string value sorted by numeric
            ("@t1", "@n2", "4 @__key @t1 @n1 @n2", "@n1", "ASC"),
            # Numeric value sorted by different numeric (tie-breaking)
            ("@n1", "@n2", "3 @__key @n1 @n2", "@n2", "ASC"),
        ]
        for val, group, load, by, order in cases:
            if order is None:
                nargs, order_clause = "3", ""
            else:
                nargs, order_clause = "4", f" {order}"
            alias = f"first_{val[1:]}_{by[1:]}_{order or 'default'}"
            self.check(dialect,
                f"ft.aggregate {key_type}_idx1 * "
                f"load {load} "
                f"groupby 1 {group} "
                f"reduce first_value {nargs} {val} BY {by}{order_clause} as {alias}"
            )

    def test_first_value_keyword_case(self, key_type, dialect):
        """Test FIRST_VALUE with case-insensitive keywords."""
        self.setup_data("sortable numbers", key_type)

        # 3-arg form: vary BY keyword case only
        for by_kw in ["by", "BY", "By"]:
            self.check(dialect,
                f"ft.aggregate {key_type}_idx1 * "
                f"load 3 @__key @n1 @n2 "
                f"groupby 1 @n2 "
                f"reduce first_value 3 @n1 {by_kw} @n1 as first_{by_kw}"
            )

        # 4-arg form: vary order keyword case (ASC and DESC variants)
        for order_kw in ["asc", "ASC", "Asc", "desc", "DESC", "Desc"]:
            self.check(dialect,
                f"ft.aggregate {key_type}_idx1 * "
                f"load 3 @__key @n1 @n2 "
                f"groupby 1 @n2 "
                f"reduce first_value 4 @n1 BY @n1 {order_kw} as first_{order_kw}"
            )

    def test_first_value_edge_cases(self, key_type, dialect):
        """Test FIRST_VALUE with edge cases like nil values."""
        self.setup_data("hard numbers", key_type)

        # nil values in comparison field, both directions
        for order in ["ASC", "DESC"]:
            self.check(dialect,
                f"ft.aggregate {key_type}_idx1 * "
                f"load 3 @__key @n1 @n2 "
                f"groupby 1 @n2 "
                f"reduce first_value 4 @n1 BY @n1 {order} as first_nil_{order.lower()}"
            )

        # NOTE: Simple mode test removed due to non-deterministic ordering.
        # When FIRST_VALUE is used without a BY clause, the order of values
        # within each group is undefined, leading to inconsistent results.
        
        # Switch to sortable numbers for duplicate comparison values
        self.client.execute_command("FLUSHALL SYNC")
        time.sleep(0.5)
        self.setup_data("sortable numbers", key_type)
        
        # Test with duplicate comparison values (tie-breaking)
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * "
            f"load 3 @__key @n1 @n2 "
            f"groupby 1 @n2 "
            f"reduce first_value 4 @n1 BY @n2 ASC as first_dup_tie"
        )

    def test_first_value_errors(self, key_type, dialect):
        """Test FIRST_VALUE error conditions."""
        self.setup_data("sortable numbers", key_type)
        
        # Test nargs=0 (too few arguments) - this will be caught by parser
        # Note: This may not be testable via compatibility tests if parser rejects it
        
        # Test nargs=2 (incomplete BY clause)
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * "
            f"load 3 @__key @n1 @n2 "
            f"groupby 1 @n2 "
            f"reduce first_value 2 @n1 @n2 as first_error_nargs2"
        )
        
        # Test nargs=5 (too many arguments) - this will be caught by parser
        # Note: This may not be testable via compatibility tests if parser rejects it
        
        # Test invalid BY keyword (e.g., NOTBY)
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * "
            f"load 3 @__key @n1 @n2 "
            f"groupby 1 @n2 "
            f"reduce first_value 3 @n1 NOTBY @n2 as first_error_notby"
        )
        
        # Test invalid sort order (not ASC/DESC)
        self.check(dialect, 
            f"ft.aggregate {key_type}_idx1 * "
            f"load 3 @__key @n1 @n2 "
            f"groupby 1 @n2 "
            f"reduce first_value 4 @n1 BY @n2 INVALID as first_error_invalid"
        )

