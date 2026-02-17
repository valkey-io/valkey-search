import pytest, logging, time, itertools, math, valkey, gzip, struct, os
import sys, json
from collections import defaultdict
from operator import itemgetter
from itertools import chain, combinations
import pickle
import compatibility
from compatibility.data_sets import * 
TEST_MARKER = "*" * 100
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker

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

def process_row(row):
    if any([isinstance(r, valkey.ResponseError) for r in row]):
        return (True, None)
    if 0 != (len(row) % 2):
        print(f"BAD ROW, not even # of fields: {row}")
        for r in row:
            print(f">> [{type(r)}] {r}")
        return (True, None)
    return (False, row)

def json_load(s):
    if isinstance(s, bytes):
        try:
            s = s.decode("utf-8")
        except UnicodeDecodeError:
            print(f">>>> Unicode decode error for value {s}")
            return None
    try:
        return json.loads(s.replace("inf","Infinity"))
    except json.decoder.JSONDecodeError as e:
        print(f">>>> JSON decode error: {e} for value {s}")
        return None

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
    try:
        if key_type == "json" and isinstance(x, int):
            result = x
        elif key_type == "json" and x.startswith(b'['):
            assert isinstance(x, bytes), f"Expected bytes for JSON value, got {type(x)}"
            return json_load(x)
        elif isinstance(x, bytes):
            result = x
        elif isinstance(x, str):
            result = x
        elif isinstance(x, int):
            result = x
        else:
            print("Unknown type ", type(x))
            assert False
    except Exception as e:
        print(">>> Got Exception parsing field type: ", type(x), " Value: ",x, " for key_type ", key_type, " Exception was ", e)
        raise
    return result

def result_has_sortkeys(rs):
    """Detect if a search result actually contains sort keys by checking the format.
    
    With sort keys: [count, key1, #sortkey1, [fields1], key2, #sortkey2, [fields2], ...]
    Without sort keys: [count, key1, [fields1], key2, [fields2], ...]
    
    The sort key is a bytes/string that starts with '#' (or '$' in some Redis versions),
    and fields are always a list.
    """
    if len(rs) < 3:
        return False
    # Check if element at index 2 (after count and first key) is a sort key (starts with # or $)
    # or a fields list
    second_elem = rs[2]
    if isinstance(second_elem, list):
        # It's a fields list, so no sort keys
        return False
    if isinstance(second_elem, (bytes, str)):
        # Check if it starts with '#' or '$' (sort key indicator)
        if isinstance(second_elem, bytes):
            return second_elem.startswith(b'#') or second_elem.startswith(b'$')
        return second_elem.startswith('#') or second_elem.startswith('$')
    return False

def unpack_search_result(rs, key_type, has_sortkeys=False):
    rows = []
    if has_sortkeys:
        # Format: [count, key1, sortkey1, [fields1], key2, sortkey2, [fields2], ...]
        # Step by 3 elements at a time
        for (key, sortkey, value) in [(rs[i], rs[i+1], rs[i+2]) for i in range(1, len(rs), 3)]:
            row = {"__key": key}
            for j in range(0, len(value), 2):
                row[parse_field(value[j], key_type)] = parse_value(value[j+1], key_type)
            rows += [row]
    else:
        # Format: [count, key1, [fields1], key2, [fields2], ...]
        for (key, value) in [(rs[i],rs[i+1]) for i in range(1, len(rs), 2)]:
            row = {"__key": key}
            for i in range(0, len(value), 2):
                row[parse_field(value[i], key_type)] = parse_value(value[i+1], key_type)
            rows += [row]
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
        raise
    return rows

def unpack_result(cmd, key_type, rs, sortkeys):
    if "ft.search" in cmd[0].lower():
        # Detect if the result actually has sort keys by checking the format,
        # not just whether WITHSORTKEYS is in the command. This handles cases
        # where the expected result (from pickle) may not have sort keys even
        # if the command requested them.
        has_sortkeys = result_has_sortkeys(rs)
        out = unpack_search_result(rs, key_type, has_sortkeys)
    else:
        out = unpack_agg_result(rs, key_type)
    #
    # Sort by the sortkeys
    #
    if len(sortkeys) > 0:
        try:
            out.sort(key=itemgetter(*sortkeys))
        except KeyError:
            if sortkeys == ['__key']:
                # we're not smart about when there is or isn't a key in the return
                return out
            print("Failed on sortkeys: ", sortkeys)
            print("CMD:", cmd)
            print("RESULT:", rs)
            print("Out:", out)
            assert False
    return out

def compare_number_eq(l, r):
    lnan = l in ["nan", b"nan", "-nan", b"-nan"]
    rnan = r in ["nan", b"nan", "-nan", b"-nan"]

    if lnan and rnan:
        return True
    elif isinstance(l, list) and isinstance(r, list):
        if len(l) != len(r):
            print("mismatch vector field length: ", l, " ", r)
            return False
        for i in range(len(l)):
            if not compare_number_eq(l[i], r[i]):
                print("mismatch vector field value: ", l, " ", r, " at index ", i)
                return False
        return True
    elif isinstance(l, str) and l.startswith("[") and isinstance(r, str) and r.startswith("["):
        # Special case. It's really a list encoded as JSON
        ll = json_load(l)
        rr = json_load(r)
        if len(ll) != len(rr):
            print("mismatch vector field length: ", ll, " ", rr)
            return False
        for i in range(len(ll)):
            if not compare_number_eq(ll[i], rr[i]):
                print("mismatch vector field value: ", ll, " ", rr, " at index ", i)
                return False
        return True
    else:
        try:
            return math.isclose(float(l), float(r), abs_tol=.01)
        except ValueError:
            print("ValueError comparing: ", l, " and ", r)
            return False
        except TypeError:
            print("TypeError comparing: ", l, " type:", type(l), " and ", r, " type:", type(r))
            return False
        
        
    
def compare_row(l, r, key_type):
    lks = sorted(list(l.keys()))
    rks = sorted(list(r.keys()))
    #print("Comparing row: ", l, " and ", r)
    #print("Sorted keys: ", lks, " and ", rks)
    if lks != rks:
        return False
    for i in range(len(lks)):
        #
        # Hack, fields that start with an 'n' are assumed to be numeric
        #
        if lks[i].startswith("n") or lks[i].endswith("score"):
            if not compare_number_eq(l[lks[i]], r[rks[i]]):
                print(f"mismatch numeric field: {l[lks[i]]}:{type(l[lks[i]])} and {r[rks[i]]}:{type(r[rks[i]])}")
                print("RL: ", r)
                print("VK: ", l)
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
        elif lks[i] == b'$' and rks[i] == b'$':
            try:
                l_json = json_load(l[lks[i]])
                r_json = json_load(r[rks[i]])
                if l_json != r_json:
                    print("mismatch JSON field: ", l[lks[i]], " and ", r[rks[i]])
                    print("Loaded JSON: L:", l_json, " and R:", r_json)
                    return False
            except json.decoder.JSONDecodeError:
                print("JSON decode error comparing: ", l[lks[i]], " and ", r[rks[i]])
                return False
        elif l[lks[i]] != r[rks[i]]:
            print("mismatch field: ", lks[i], " and ", rks[i], " ", l[lks[i]], "!=", r[rks[i]])
            return False
    return True            
    
def compare_results(expected, results):
    print("CMD:", printable_cmd(expected["cmd"]))
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
        count = int(cmd[ix+1]) if cmd[0] != 'ft.search' else 1
        # Grab the fields after the count, stripping any leading '@'
        sortkeys = [cmd[ix+2+i][1 if cmd[ix+2+i].startswith("@") else 0:] for i in range(count)]
        for f in ['asc', 'desc', 'ASC', 'DESC']:
            if f in sortkeys:
                sortkeys.remove(f)
    else:
        sortkeys=["__key"]
        # sortkeys=[]

    # If both failed, it's a wrong search cmd and we can exit
    if expected["exception"] and results["exception"]:
        print("Both engines failed.")
        print(f"CMD:{cmd}")
        print(TEST_MARKER)
        return True

    if expected["exception"]:
        print("RL Exception, skipped")
        #print(f"RL Exception: Raw: {printable_result(results['RL'])}")
        #print(f"VK: Result: {printable_result(results['VK:'])}")
        print(TEST_MARKER)
        return True

    if results["exception"]:
        print(f"CMD: {cmd}")
        print(f"RL: Result: {printable_result(expected['result'])}")
        # print(f"VK: Exception Raw: {printable_result(results['result'])}")
        print(TEST_MARKER)
        return False

    # Output raw results
    # print("Raw expected result:", expected["result"])
    rl = unpack_result(cmd, expected["key_type"], expected["result"], sortkeys)
    # print("Unpack of expected result:", rl)
    # print("Raw actual result:", results["result"])
    vk = unpack_result(cmd, expected["key_type"], results["result"], sortkeys)
    # print("Unpack of actual result:", vk)

    # Process failures
    if len(rl) != len(vk):
        print(f"CMD:{cmd}")
        print(f"Mismatched sizes RL:{len(rl)} VK:{len(vk)}")
        print("--RL--")
        for r in rl:
            print(r)
        print("--VK:--")
        for e in vk:
            print(e)
        #assert False
        return False

    # if compare_results(vk, rl):
    # Directly comparing dicts instead of custom compare function
    # TODO: investigate this later
    if all([compare_row(vk[i], rl[i], key_type) for i in range(len(rl))]):
        # print("Results look good.")
        #print(TEST_MARKER)
        if "ft.search" in cmd:
            print(f"CMD:{cmd}")
            for i in range(len(rl)):
                print("RL:",i,[(k,rl[i][k]) for k in sorted(rl[i].keys())])
                print("VK:",i,[(k,vk[i][k]) for k in sorted(vk[i].keys())])
        return True
    print("***** MISMATCH ON DATA *****, sortkeys=", sortkeys, " records=", len(rl), " TestName: ", expected["testname"], " <<< Identifies mismatching results")
    print(f"CMD: {cmd}")
    for i in range(len(rl)):
        if not compare_row(rl[i], vk[i], key_type):
            print("RL:",i,[(k,rl[i][k]) for k in sorted(rl[i].keys())], "<<<")
            print("VK:",i,[(k,vk[i][k]) for k in sorted(vk[i].keys())], "<<<")
        else:
            print("RL:",i,[(k,rl[i][k]) for k in sorted(rl[i].keys())], u'\u2713')
            print("VK:",i,[(k,vk[i][k]) for k in sorted(vk[i].keys())], u'\u2713')

    print("Raw RL:", expected["result"])
    print("Raw VK:", results["result"])
    print(TEST_MARKER)
    return False

correct_answers = 0
wrong_answers = 0
StopOnFailure = False
failed_tests = {}
passed_tests = {}

def mark_as_passed(testname):
    global correct_answers, passed_tests
    correct_answers += 1
    if testname not in passed_tests:
        passed_tests[testname] = 0
    passed_tests[testname] += 1

def mark_as_failed(testname):
    global failed_tests, wrong_answers
    print(">>>>>>>>>>> ERROR FAILURE <<<<<<<<<<<<<<")
    if testname not in failed_tests:
        failed_tests[testname] = 0
    failed_tests[testname] += 1
    wrong_answers += 1
    assert not StopOnFailure, "Test failed, stopping execution"

def do_answer(client, expected, data_set):
    global correct_answers, failed_tests, passed_tests
    if (expected['data_set_name'], expected['key_type']) != data_set:
        print("Loading data set:", expected['data_set_name'], "key type:", expected['key_type'])
        client.execute_command("FLUSHALL SYNC")
        load_data(client, expected['data_set_name'], expected['key_type'])
        data_set = (expected['data_set_name'], expected['key_type'])

    # Set the cluster mode fanout-data-uniformity config to handle non uniform data distrubution 
    # across shards.
    try:
        client.execute_command("CONFIG", "SET", "search.fanout-data-uniformity", "0")
    except Exception as e:
        print(f"⚠ Failed to set search.fanout-data-uniformity config for test: {expected['testname']}, error: {e}")

    # Set Valkey-specific config for inorder tests
    if 'inorder' in expected['testname'] or 'slop' in expected['testname']:
        try:
            client.execute_command("CONFIG", "SET", "search.proximity-inorder-compat-mode", "yes")
            print(f"✓ Set Valkey compat mode for test: {expected['testname']}")
        except Exception as e:
            print(f"⚠ Failed to set Valkey compat mode for test: {expected['testname']}, error: {e}")
    
    result = {}
    try:
        print(f">>>>>> Starting Test {expected['testname']} So Far: Correct:{correct_answers} Wrong:{wrong_answers} <<<<<<<<<")
        result["cmd"] = expected['cmd']
        result["result"] = client.execute_command(*expected['cmd'])
        result["exception"] = False
        if compare_results(expected, result):
            mark_as_passed(expected['testname'])
        else:
            mark_as_failed(expected['testname'])
    except valkey.ResponseError as e:
        print(f"Got ResponseError: {e} for command {expected['cmd']}")
        result["exception"] = True
        if compare_results(expected, result):
            mark_as_passed(expected['testname'])
        else:
            mark_as_failed(expected['testname'])
    return data_set

def drop_index_cluster(test_case, key_type):
    index_name = "json_idx1" if key_type == "json" else "hash_idx1"
    primary0 = test_case.new_client_for_primary(0)
    try:
        primary0.execute_command("FT.DROPINDEX", index_name)
        print(f"Dropped index {index_name}")
    except valkey.ResponseError:
        pass  # index may not exist yet

def do_answer_cluster(cluster_client, expected, data_set, test_case):
    global correct_answers, failed_tests, passed_tests

    next_data_set = (expected["data_set_name"], expected["key_type"])

    if data_set != next_data_set:
        print(
            "Loading CLUSTER data set:",
            expected["data_set_name"],
            "key type:",
            expected["key_type"],
        )

        drop_index_cluster(test_case, expected["key_type"])

        cluster_client.execute_command("FLUSHALL")

        load_data_cluster(
            cluster_client,
            test_case,
            expected["data_set_name"],
            expected["key_type"],
        )

        data_set = next_data_set

    result = {}
    try:
        print(
            f">>>>>> Starting CLUSTER Test {expected['testname']} "
            f"So Far: Correct:{correct_answers} Wrong:{wrong_answers} <<<<<<<<<"
        )

        result["cmd"] = expected["cmd"]
        result["result"] = cluster_client.execute_command(*expected["cmd"])
        result["exception"] = False

        if compare_results(expected, result):
            mark_as_passed(expected["testname"])
        else:
            mark_as_failed(expected["testname"])

    except valkey.ResponseError as e:
        print(f"Got ResponseError: {e} for command {expected['cmd']}")
        result["exception"] = True

        if compare_results(expected, result):
            mark_as_passed(expected["testname"])
        else:
            mark_as_failed(expected["testname"])

    return data_set

class TestAnswersCMD(ValkeySearchTestCaseBase):
    @pytest.mark.parametrize("answers", ["aggregate-answers.pickle.gz", "text-search-answers.pickle.gz"])
    def test_answers(self, answers):
        global client, data_set
        global correct_answers, failed_tests, passed_tests

        # RESET GLOBAL COUNTERS AT START OF EACH TEST
        correct_answers = 0
        wrong_answers = 0
        failed_tests = {}
        passed_tests = {}

        print("Running test_answers with answers file:", answers)
        with gzip.open(os.getenv("ROOT_DIR") + "/integration/compatibility/" + answers, "rb") as answer_file:
            answers = pickle.load(answer_file)

        data_set = None
        for i in range(len(answers)):
            data_set = do_answer(self.server.get_new_client(), answers[i], data_set)

        if correct_answers != len(answers):
            print(f"Correct answers: {correct_answers} out of {len(answers)}")
            if len(failed_tests) != 0:
                print(">>>>>>>>> Failed Tests <<<<<<<<<")
                for k, v in failed_tests.items():
                    print(f"Failed test {k:60}: {v} times")
            assert False

        '''
        print(f"Correct answers: {correct_answers} out of {len(answers)}")
        if len(failed_tests) != 0:
            print(">>>>>>>>> Failed Tests <<<<<<<<<")
            for k, v in failed_tests.items():
                print(f"Failed test {k:60}: {v} times")
        print(">>>>>>>>> Passed Tests <<<<<<<<<")
        for k, v in passed_tests.items():
            print(f"Passed test {k:60}: {v} times")

        f = open("compatibility/" + ANSWER_FILE_NAME, "rb")
        client = ClientLSystem()
        data_set = None
        answers = pickle.load(f)
        print(f"Loaded {len(answers)} answers")
        data_set = None
        for i in range(len(answers)):
            data_set = do_answer(answers[i], data_set)

        print(f"Correct answers: {correct_answers} out of {len(answers)}")
        if len(failed_tests) != 0:
            print(">>>>>>>>> Failed Tests <<<<<<<<<")
            for k, v in failed_tests.items():
                print(f"Failed test {k:60}: {v} times")
        print(">>>>>>>>> Passed Tests <<<<<<<<<")
        for k, v in passed_tests.items():
            print(f"Passed test {k:60}: {v} times")
    '''

# TODO: fix cluster mode test failures
class TestAnswersCME(ValkeySearchClusterTestCase):
    @pytest.mark.parametrize("answers", ["aggregate-answers.pickle.gz"])
    def test_answers(self, answers):
        global correct_answers, wrong_answers, failed_tests, passed_tests

        correct_answers = 0
        wrong_answers = 0
        failed_tests = {}
        passed_tests = {}

        print("Running CLUSTER test_answers with answers file:", answers)

        with gzip.open(
            os.getenv("ROOT_DIR") + "/integration/compatibility/" + answers,
            "rb",
        ) as answer_file:
            answers = pickle.load(answer_file)

        data_set = None
        cluster_client = self.new_cluster_client()

        for expected in answers:
            data_set = do_answer_cluster(
                cluster_client=cluster_client,
                expected=expected,
                data_set=data_set,
                test_case=self,
            )

        if correct_answers != len(answers):
            print(f"Correct answers: {correct_answers} out of {len(answers)}")
            if failed_tests:
                print(">>>>>>>>> Failed Tests <<<<<<<<<")
                for k, v in failed_tests.items():
                    print(f"Failed test {k:60}: {v} times")
            assert False