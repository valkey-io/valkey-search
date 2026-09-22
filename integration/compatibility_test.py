import pytest, logging, time, itertools, math, valkey, gzip, struct, os
import sys, json
from collections import defaultdict
from operator import itemgetter
from itertools import chain, combinations
import pickle
import compatibility
from valkey.cluster import ValkeyCluster
from compatibility import GENERATORS, compute_sources_hash
from compatibility.data_sets import *

ALL_ANSWER_FILES = [g["answers"] for g in GENERATORS]
CLUSTER_ANSWER_FILES = [g["answers"] for g in GENERATORS if g["cluster"]]
TEST_MARKER = "*" * 100
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchClusterTestCaseDebugMode,
    ValkeySearchTestCaseBase,
    ValkeySearchTestCaseDebugMode,
)

# The compatibility pickles capture Redisearch behavior, which is the compatible
# target for every emulate-release-gated compatibility fix. Pin the replay to the
# maximum release so all such fixes are enabled regardless of the version each
# was introduced in. This requires debug-mode (the value is above
# kModuleVersion), which the *DebugMode base classes enable. Datasets that do not
# exercise a gated fix are unaffected by this setting.
COMPAT_EMULATE_RELEASE = "65535.255.255"
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper
from valkeytestframework.util import waiters

# How closely two engines' numbers have to agree, by the storage type of the
# vectors in play. See compare_number_eq.
#
# FLOAT32 holds about seven decimal digits and the two engines agree to nearly
# all of them; what differs is how they format the result. The 2-byte types
# hold about three, and the engines round them differently -- the repo's own
# space_distance_test.cc compares them at 1e-2 for the same reason.
TOLERANCE_BY_VECTOR_TYPE = {
    "FLOAT32": (1e-5, 1e-6),
    "FLOAT16": (1e-2, 1e-2),
    "BFLOAT16": (1e-2, 1e-2),
}
DEFAULT_TOLERANCE = TOLERANCE_BY_VECTOR_TYPE["FLOAT32"]

encoder = lambda x: x.encode() if not isinstance(x, bytes) else x

def printable_cmd(cmd):
    new_cmd = [encoder(c) for c in cmd]
    return b" ".join(new_cmd)


def printable_result(res):
    if isinstance(res, list):
        return [printable_result(x) for x in res]
    if isinstance(res, bytes):
        # Vector fields are raw little-endian floats, so a result carrying one
        # (any FT.HYBRID answer loading the vector column, for instance) is not
        # UTF-8. Fall back to repr rather than letting the diagnostic print
        # raise and hide the mismatch it was called to explain.
        try:
            return res.decode("utf-8")
        except UnicodeDecodeError:
            return repr(res)
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
    """Normalize a field name from a reply.

    This deliberately does NOT strip a leading "$.". Doing so used to hide
    issue #1243: on a JSON index valkey-search emitted the schema identifier
    ($.n1) where Redisearch emits the attribute name (n1), and stripping the
    prefix made the two compare equal.
    """
    if isinstance(x, bytes):
        return parse_field(x.decode("utf-8"), key_type)
    if isinstance(x, str):
        return x
    if isinstance(x, int):
        return x
    print("Unknown type ", type(x))
    assert False

def parse_value(x, key_type):
    try:
        if x is None:
            # RESP nil: an APPLY whose expression evaluated to nothing.
            # Both engines can return this (e.g. a string function applied to
            # a numeric field on JSON), so represent it as None on both sides.
            result = None
        elif isinstance(x, list):
            # TOLIST reducer returns a Python list for both hash and json
            result = x
        elif key_type == "json" and isinstance(x, int):
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

def has_token(cmd, token):
    """Whether cmd carries a keyword, case-insensitively."""
    # Case-insensitive: the generators emit uppercase. isinstance skips binary BLOBs.
    return any(isinstance(c, str) and c.lower() == token for c in cmd)

def unpack_scored_search_result(rs, key_type, has_content):
    """Unpack a WITHSCORES reply: [count, key, score, (fields), ...]."""
    # result_has_sortkeys cannot detect this shape, so the stride comes from the command.
    rows = []
    stride = 3 if has_content else 2
    for i in range(1, len(rs), stride):
        row = {"__key": rs[i], "__score": rs[i + 1]}
        if has_content:
            fields = rs[i + 2]
            for j in range(0, len(fields), 2):
                row[parse_field(fields[j], key_type)] = parse_value(fields[j + 1], key_type)
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

def unpack_hybrid_result(rs, key_type):
    """Unpack an FT.HYBRID reply into a list of row dicts.

    The two engines wrap the same rows differently:

      Redis   [b"total_results", N, b"results", [row, ...],
               b"warnings", [...], b"execution_time", b"..."]
      Valkey  [N, row, row, ...]                       (the FT.AGGREGATE shape)

    Only the rows are comparable. Redis's `total_results` is the size of the
    fused set *before* LIMIT while Valkey's leading count is the number of rows
    actually returned, and `execution_time` is wall-clock noise, so both are
    dropped here rather than compared.
    """
    if len(rs) >= 2 and unbytes(rs[0]) == "total_results":
        fields = {unbytes(rs[i]): rs[i + 1] for i in range(0, len(rs), 2)}
        rows = fields.get("results", [])
    else:
        rows = rs[1:]
    out = []
    for row in rows:
        out.append({
            parse_field(row[i], key_type): parse_value(row[i + 1], key_type)
            for i in range(0, len(row), 2)
        })
    return out

def order_insensitive(v):
    """Row-ordering form of a field value.

    A TOLIST field comes back in a different element order from each engine, so
    a row keyed on one would otherwise sort differently on each side and the
    two replies would be compared row-against-the-wrong-row.
    """
    if isinstance(v, list):
        return sorted(repr(order_insensitive(i)) for i in v)
    return repr(v)


def row_sort_key(sortkeys):
    # Rows that tie on the sort keys are ordered by their whole content, so
    # equal-keyed rows still line up between the two replies.
    def key(row):
        # A sort key can be absent from a row: a field the key never had is
        # left out of the reply, so `sortby 2 @t2 asc` over a dataset where
        # some documents lack t2 yields rows without it. Both engines omit it
        # the same way, so a shared placeholder keeps those rows comparable
        # and lets the whole-content tiebreak below order them.
        return ([order_insensitive(row.get(k)) for k in sortkeys],
                sorted((repr(k), order_insensitive(v)) for k, v in row.items()))
    return key


def canonicalize_ties(rows, sortkeys):
    """Order the rows that tie on `sortkeys`, and leave everything else where
    it is.

    Used where the reply's own sequence is the answer. Two engines asked to
    sort by a field must agree on the order of rows whose values differ, but
    nothing decides the order of rows that hold the same value -- a SORTBY on
    a field some documents do not carry leaves every one of those tied, and so
    does a SORTBY over an array column where two groups hold the same array.
    Canonicalizing each run of tied rows keeps those interchangeable without
    giving up on the order of the rest.
    """
    def key_of(row):
        return [order_insensitive(row.get(k)) for k in sortkeys]

    def content_of(row):
        return sorted((repr(k), order_insensitive(v)) for k, v in row.items())

    out = []
    i = 0
    while i < len(rows):
        j = i + 1
        while j < len(rows) and key_of(rows[j]) == key_of(rows[i]):
            j += 1
        run = rows[i:j]
        run.sort(key=content_of)
        out.extend(run)
        i = j
    return out


def unpack_result(cmd, key_type, rs, sortkeys, ordered=False):
    if "ft.hybrid" in cmd[0].lower():
        out = unpack_hybrid_result(rs, key_type)
    elif "ft.search" in cmd[0].lower():
        if has_token(cmd, "withscores"):
            # NOCONTENT is the only suppressor emitted; RETURN 0 would need the same.
            out = unpack_scored_search_result(
                rs, key_type, has_content=not has_token(cmd, "nocontent"))
        else:
            # Detect if the result actually has sort keys by checking the format,
            # not just whether WITHSORTKEYS is in the command. This handles cases
            # where the expected result (from pickle) may not have sort keys even
            # if the command requested them.
            has_sortkeys = result_has_sortkeys(rs)
            out = unpack_search_result(rs, key_type, has_sortkeys)
    else:
        out = unpack_agg_result(rs, key_type)
    #
    # Align the rows for comparison. `ordered` means the command fixed the
    # reply's sequence, so the sequence itself is the thing under test and
    # only tied rows may be moved.
    #
    if ordered and not any(isinstance(row.get(k), list)
                           for row in out for k in sortkeys):
        return canonicalize_ties(out, sortkeys)
    # A list-valued sort key falls through to the alignment below. The engines
    # return the elements of a TOLIST in different orders -- which is what
    # order_insensitive() exists to absorb -- so they are not sorting the same
    # values, and the sequence each produces is not something the other can be
    # held to.
    if len(sortkeys) > 0:
        try:
            out.sort(key=row_sort_key(sortkeys))
        except KeyError:
            if sortkeys == ['__key']:
                # we're not smart about when there is or isn't a key in the return
                return out
            # A sort field the engine did not return as its own column. That is
            # itself a difference worth reporting, so leave the rows unsorted
            # and let compare_results surface the mismatch -- aborting the whole
            # run here would hide every answer after this one.
            print("Failed on sortkeys: ", sortkeys)
            print("CMD:", cmd)
            print("RESULT:", rs)
            print("Out:", out)
            return out
    return out

def _is_numeric(x):
    # nan/-nan don't survive float() on every platform, so name them explicitly.
    if x in ("nan", "-nan", b"nan", b"-nan"):
        return True
    try:
        float(x)
        return True
    except (ValueError, TypeError):
        return False

# `tol` is a (relative, absolute) pair: see the note at the comparison itself.
def compare_number_eq(l, r, tol=DEFAULT_TOLERANCE):
    lnan = l in ["nan", b"nan", "-nan", b"-nan"]
    rnan = r in ["nan", b"nan", "-nan", b"-nan"]

    # A numeric field can come back as a RESP nil -- GROUPBY on a field some
    # documents lack names the group's key with one. float(None) raises, so
    # without this two identical nil replies read as a mismatch.
    if l is None or r is None:
        return l is None and r is None

    if lnan and rnan:
        return True
    elif isinstance(l, list) and isinstance(r, list):
        if len(l) != len(r):
            print("mismatch vector field length: ", l, " ", r)
            return False
        for i in range(len(l)):
            if not compare_number_eq(l[i], r[i], tol):
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
            if not compare_number_eq(ll[i], rr[i], tol):
                print("mismatch vector field value: ", ll, " ", rr, " at index ", i)
                return False
        return True
    else:
        try:
            # Relative first, absolute only as a floor near zero.
            #
            # The absolute tolerance on its own was 0.01, which is wider than
            # the whole range of a reciprocal-rank-fusion score: with the
            # default constant those span about 0.012 to 0.033 across a page,
            # so any permutation of them compared equal and the column was
            # decorative. The two engines format the same value to different
            # precision -- 0.0327868852459 against 0.0327868834138, a relative
            # difference near 6e-8 -- which is what the relative tolerance is
            # sized for. The absolute floor keeps values that straddle zero
            # from being held to a relative standard they cannot meet.
            rel_tol, abs_tol = tol
            return math.isclose(float(l), float(r), rel_tol=rel_tol,
                                abs_tol=abs_tol)
        except ValueError:
            print("ValueError comparing: ", l, " and ", r)
            return False
        except TypeError:
            print("TypeError comparing: ", l, " type:", type(l), " and ", r, " type:", type(r))
            return False
        
        
    
def compare_row(l, r, key_type, tol=DEFAULT_TOLERANCE):
    lks = sorted(list(l.keys()))
    rks = sorted(list(r.keys()))
    #print("Comparing row: ", l, " and ", r)
    #print("Sorted keys: ", lks, " and ", rks)
    if lks != rks:
        return False
    for i in range(len(lks)):
        #
        # TOLIST reducer returns lists where order is non-deterministic.
        # Check for list values first, before any field-name-based heuristics.
        # Sort both lists before comparing since insertion order differs between
        # Redis (random hash table traversal) and valkey-search (index order).
        #
        if isinstance(l[lks[i]], list) and isinstance(r[rks[i]], list):
            if sorted(l[lks[i]], key=lambda x: x if isinstance(x, (int, float)) else str(x)) != \
               sorted(r[rks[i]], key=lambda x: x if isinstance(x, (int, float)) else str(x)):
                print("mismatch list field: ", lks[i], " ", sorted(l[lks[i]], key=lambda x: x if isinstance(x, (int, float)) else str(x)), "!=", sorted(r[rks[i]], key=lambda x: x if isinstance(x, (int, float)) else str(x)))
                return False
        #
        # Hack, fields that start with an 'n' are assumed to be numeric
        #
        elif lks[i].startswith("n") or lks[i].endswith("score"):
            if not compare_number_eq(l[lks[i]], r[rks[i]], tol):
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
                if not compare_number_eq(l[lks[i]][i], r[rks[i]][i], tol):
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
        else:
            lv, rv = l[lks[i]], r[rks[i]]
            # Exact match is the fast path, which is what every loaded/stored
            # field hits.
            if lv == rv:
                continue
            # Values differ byte-for-byte. If both are numeric, fall back to the
            # tolerant numeric compare -- it treats nan/-nan as equal and uses
            # math.isclose, absorbing the two engines' differing float precision
            # and negative-zero formatting on any server-computed numeric field
            # (APPLY results, GROUPBY reducers). Non-numeric values (concat/
            # lower/substr/timefmt string results, tags, keys) stay an exact
            # match.
            if _is_numeric(lv) and _is_numeric(rv) and compare_number_eq(lv, rv):
                continue
            print("mismatch field: ", lks[i], " and ", rks[i], " ", lv, "!=", rv)
            return False
    return True            
    
def compare_results(expected, results):
    print("CMD:", printable_cmd(expected["cmd"]))
    cmd = expected["cmd"]
    key_type = expected["key_type"]
    tol = TOLERANCE_BY_VECTOR_TYPE.get(
        expected.get("vector_data_type", "FLOAT32"), DEFAULT_TOLERANCE)
    if cmd != results["cmd"]:
        print("CMD Mismatch: ", cmd, " ", results["cmd"])
        assert False
    
    # Key on the *last* GROUPBY/SORTBY in the pipeline: it decides which fields
    # the reply carries, and an earlier GROUPBY's key is gone from the output
    # once a later stage regroups.
    def last_index(keyword):
        # Case-insensitive: a generator writing SORTBY the way the command
        # reference does must not silently get a different comparison from one
        # writing it in lower case. str() because a command carries a raw
        # vector blob among its arguments.
        hits = [i for i, c in enumerate(cmd) if str(c).lower() == keyword]
        return hits[-1] if hits else -1

    def field_name(token):
        return str(token).lstrip('@')

    gix = last_index('groupby')
    six = last_index('sortby')
    # `ordered` says the command fixed the reply's sequence, so the sequence is
    # itself under test and the rows are compared as they arrived. Only a
    # SORTBY that nothing regroups afterwards does that: a GROUPBY puts the
    # reply back in an order no one specified, so a reply ending in one is
    # aligned on its group key instead.
    ordered = False
    if gix > six:
        count = int(cmd[gix+1])
        sortkeys = [field_name(cmd[gix+2+i]) for i in range(count)]
    elif six >= 0:
        ordered = True
        # FT.SEARCH takes a bare field where the aggregate pipeline takes a
        # count followed by that many tokens: `SORTBY @n1 ASC` against
        # `SORTBY 2 @n1 ASC`. The names are still needed here, to tell which
        # rows tie and may therefore be ordered either way.
        if str(cmd[0]).lower() == 'ft.search':
            sortkeys = [field_name(cmd[six+1])]
        else:
            count = int(cmd[six+1])
            sortkeys = [field_name(cmd[six+2+i]) for i in range(count)]
        sortkeys = [f for f in sortkeys if f.lower() not in ('asc', 'desc')]
    elif "ft.hybrid" in str(cmd[0]).lower():
        # A rank-fusion command answers in fused-score order whether or not the
        # query says so -- that ordering IS the answer -- so it is compared as
        # it arrived. Without this the reply was aligned on `__key`, a column
        # an FT.HYBRID reply does not even carry under `LOAD *`, so every row
        # tied, the alignment fell back to whole-row content, and the ranking
        # was never compared at all.
        #
        # The tie key is the name COMBINE gave the fused score, because that
        # is the column that holds it. With no COMBINE alias there is no tie
        # key at all and the order is compared strictly, position by position.
        # `__score`, the default name, would work as one -- both engines emit
        # it when the caller gave no LOAD clause -- but naming it would only
        # let rows that share a score swap places, which is a comparison these
        # cases pass without.
        ordered = True
        sortkeys = []
        cix = last_index('combine')
        if cix >= 0:
            tail = [str(c) for c in cmd[cix:]]
            for i, token in enumerate(tail):
                if token.lower() == 'yield_score_as' and i + 1 < len(tail):
                    sortkeys = [field_name(tail[i + 1])]
                    break
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

    # The sortkey-prefix cases assert the sort-key bytes, which the generic
    # unpack path below discards (unpack_search_result drops the sort-key
    # element). The return-clause cases produce no-content replies whose
    # 1-element stride the unpacker cannot parse. Both data sets are fully
    # deterministic, so compare them raw.
    if expected.get("data_set_name") in (SORTKEY_PREFIX_DATA_SET,
                                         RETURN_CLAUSE_DATA_SET):
        if expected["result"] == results["result"]:
            return True
        print(f"CMD: {cmd}")
        print(f"RL: {printable_result(expected['result'])}")
        print(f"VK: {printable_result(results['result'])}")
        return False

    # Output raw results
    # print("Raw expected result:", expected["result"])
    rl = unpack_result(cmd, expected["key_type"], expected["result"], sortkeys,
                       ordered)
    # print("Unpack of expected result:", rl)
    # print("Raw actual result:", results["result"])
    vk = unpack_result(cmd, expected["key_type"], results["result"], sortkeys,
                       ordered)
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
    if all([compare_row(vk[i], rl[i], key_type, tol)
            for i in range(len(rl))]):
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
        if not compare_row(rl[i], vk[i], key_type, tol):
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
xfailed_tests = {}
xpassed_tests = {}

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

def mark_as_xfailed(testname):
    """An answer marked `xfail` in the generator that did not match, as
    expected. Counts as accounted-for so the suite stays green while the gap it
    documents is open."""
    global correct_answers, xfailed_tests
    correct_answers += 1
    xfailed_tests[testname] = xfailed_tests.get(testname, 0) + 1

def mark_as_xpassed(testname):
    """An answer marked `xfail` that now matches -- the gap it documents has
    been closed, and the marker should come off. Reported loudly at the end of
    the run; deliberately not a failure, so closing the gap does not break the
    build before someone gets to the marker."""
    global correct_answers, xpassed_tests
    correct_answers += 1
    xpassed_tests[testname] = xpassed_tests.get(testname, 0) + 1

def do_answer(client, expected, data_set):
    global correct_answers, failed_tests, passed_tests
    next_data_set = (expected['data_set_name'], expected['key_type'],
                     expected.get('schema_type'),
                     expected.get('vector_data_type', 'FLOAT32'))
    if next_data_set != data_set:
        print("Loading data set:", expected['data_set_name'], "key type:", expected['key_type'],
              "vector_data_type:", expected.get('vector_data_type', 'FLOAT32'))
        client.execute_command("FLUSHALL SYNC")
        load_data(client, expected['data_set_name'], expected['key_type'],
                  schema_type=expected.get('schema_type', 'default'),
                  vector_data_type=expected.get('vector_data_type', 'FLOAT32'))
        waiters.wait_for_true(lambda: IndexingTestHelper.is_indexing_complete_on_node(client, f"{expected['key_type']}_idx1"))
        data_set = next_data_set

    # for the excluded queries with known difference
    # just run in valkey to make sure they do not crash
    if expected.get('excluded'):
        try:
            print(f"Running excluded query (no-crash check): {expected['cmd']}")
            client.execute_command(*expected['cmd'])
            print(f"Excluded query completed without crash")
        except Exception as e:
            print(f"Excluded query raised: {e} for cmd {expected['cmd']}")
        return data_set

    # Set Valkey-specific config for inorder tests
    if 'inorder' in expected['testname']:
        try:
            client.execute_command("CONFIG", "SET", "search.proximity-inorder-compat-mode", "yes")
            print(f"✓ Set Valkey compat mode for test: {expected['testname']}")
        except Exception as e:
            print(f"⚠ Failed to set Valkey compat mode for test: {expected['testname']}, error: {e}")
    
    # An `xfail` answer is compared like any other, but a mismatch is the
    # documented state of an open gap rather than a regression. See
    # integration/compatibility/unsupported_tests.md for what each one covers.
    xfail = expected.get('xfail', False)
    if xfail:
        print(f"xfail answer (known gap): {expected['cmd']}")

    def record(matched):
        if xfail:
            (mark_as_xpassed if matched else mark_as_xfailed)(expected['testname'])
        else:
            (mark_as_passed if matched else mark_as_failed)(expected['testname'])

    result = {}
    try:
        print(f">>>>>> Starting Test {expected['testname']} So Far: Correct:{correct_answers} Wrong:{wrong_answers} <<<<<<<<<")
        result["cmd"] = expected['cmd']
        result["result"] = client.execute_command(*expected['cmd'])
        result["exception"] = False
        record(compare_results(expected, result))
    except valkey.ResponseError as e:
        print(f"Got ResponseError: {e} for command {expected['cmd']}")
        result["exception"] = True
        record(compare_results(expected, result))
    return data_set

def cluster_routing(cmd):
    """Extra execute_command kwargs needed to route `cmd` in cluster mode.

    The cluster client routes a keyless command only if it recognizes the name:
    its SEARCH_COMMANDS list carries FT.SEARCH and FT.AGGREGATE but predates
    FT.HYBRID, so that one raises "No way to dispatch this command" instead of
    reaching a node. Sending it to the default node is what the client does for
    the two it knows, and any primary is a valid entry point -- the coordinator
    fans out from wherever the command lands.
    """
    name = str(cmd[0]).upper()
    if name in ValkeyCluster.SEARCH_COMMANDS[0]:
        return {}
    if name.startswith("FT."):
        return {"target_nodes": ValkeyCluster.DEFAULT_NODE}
    return {}


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

    # for the excluded queries with known difference
    # just run in valkey to make sure they do not crash
    if expected.get("excluded"):
        try:
            print(f"Running excluded CLUSTER query (no-crash check): {expected['cmd']}")
            cluster_client.execute_command(*expected["cmd"])
            print("Excluded CLUSTER query completed without crash")
        except Exception as e:
            print(f"Excluded CLUSTER query raised: {e} for cmd {expected['cmd']}")
        return data_set

    result = {}
    try:
        print(
            f">>>>>> Starting CLUSTER Test {expected['testname']} "
            f"So Far: Correct:{correct_answers} Wrong:{wrong_answers} <<<<<<<<<"
        )

        result["cmd"] = expected["cmd"]
        result["result"] = cluster_client.execute_command(
            *expected["cmd"], **cluster_routing(expected["cmd"]))
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

def _load_answers_with_hash_check(answer_file_name):
    """Load a compatibility pickle answer file and verify its sources hash.

    Set SKIP_COMPATIBILITY_HASH_CHECK=1 to bypass the hash check (useful when
    manually generating a small pickle for local testing).
    """
    root_dir = os.getenv("ROOT_DIR") or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
    pickle_path = os.path.join(
        root_dir, "integration/compatibility", answer_file_name
    )
    with gzip.open(pickle_path, "rb") as f:
        payload = pickle.load(f)

    if isinstance(payload, dict) and "answers" in payload:
        stored_hash = payload.get("sources_hash")
        answers = payload["answers"]
    else:
        stored_hash = None
        answers = payload

    if os.getenv("SKIP_COMPATIBILITY_HASH_CHECK") == "1":
        print(f"SKIP_COMPATIBILITY_HASH_CHECK=1; skipping hash check for {answer_file_name}")
        return answers

    current_hash = compute_sources_hash()
    if stored_hash != current_hash:
        pytest.fail(
            f"\nCompatibility pickle file '{answer_file_name}' is stale.\n"
            f"  Stored hash:  {stored_hash}\n"
            f"  Current hash: {current_hash}\n"
            f"\n"
            f"Python sources in integration/compatibility/ have changed since\n"
            f"the pickle was generated. Regenerate with:\n"
            f"\n"
            f"  ./integration/compatibility/regenerate.sh\n"
            f"\n"
            f"Then commit the updated pickle file. To bypass this check (e.g.\n"
            f"when manually generating a small pickle for local testing), set\n"
            f"the env variable SKIP_COMPATIBILITY_HASH_CHECK=1.\n",
            pytrace=False,
        )
    return answers


class TestAnswersCMD(ValkeySearchTestCaseDebugMode):
    @pytest.mark.parametrize("answers", ALL_ANSWER_FILES)
    def test_answers(self, answers):
        global client, data_set
        global correct_answers, failed_tests, passed_tests
        global xfailed_tests, xpassed_tests

        # RESET GLOBAL COUNTERS AT START OF EACH TEST
        correct_answers = 0
        wrong_answers = 0
        failed_tests = {}
        passed_tests = {}
        xfailed_tests = {}
        xpassed_tests = {}

        print("Running test_answers with answers file:", answers)
        answers = _load_answers_with_hash_check(answers)

        data_set = None
        client = self.server.get_new_client()
        client.execute_command(
            "CONFIG", "SET", "search.emulate-release", COMPAT_EMULATE_RELEASE
        )
        for i in range(len(answers)):
            data_set = do_answer(client, answers[i], data_set)

        if xfailed_tests:
            print(">>>>>>>>> Expected Failures (known gaps) <<<<<<<<<")
            for k, v in sorted(xfailed_tests.items()):
                print(f"xfail {k:60}: {v} times")
        if xpassed_tests:
            # Not a failure: closing the gap should not break the build before
            # someone removes the marker. It does need to be impossible to miss.
            print("!" * 78)
            print("XPASS: answers marked `xfail` in the generator now MATCH.")
            print("The gap they document has been closed -- drop the xfail")
            print("marker in integration/compatibility/ and update")
            print("unsupported_tests.md.")
            for k, v in sorted(xpassed_tests.items()):
                print(f"  xpass {k:60}: {v} times")
            print("!" * 78)

        expected_count = sum(1 for a in answers if not a.get('excluded'))
        if correct_answers != expected_count:
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
class TestAnswersCME(ValkeySearchClusterTestCaseDebugMode):
    @pytest.mark.parametrize("answers", CLUSTER_ANSWER_FILES)
    def test_answers(self, answers):
        global correct_answers, wrong_answers, failed_tests, passed_tests

        correct_answers = 0
        wrong_answers = 0
        failed_tests = {}
        passed_tests = {}

        print("Running CLUSTER test_answers with answers file:", answers)

        answers = _load_answers_with_hash_check(answers)

        data_set = None
        cluster_client = self.new_cluster_client()
        for primary in self.get_all_primary_clients():
            primary.execute_command(
                "CONFIG", "SET", "search.emulate-release", COMPAT_EMULATE_RELEASE
            )

        # Cluster mode does not yet thread vector_data_type through the
        # data-loading path, so restrict the cluster compatibility run to FP32
        # entries. FP16 single-node coverage is exercised by TestAnswersCMD.
        answers = [a for a in answers if a.get("vector_data_type", "FLOAT32") == "FLOAT32"]
        # Pin every primary to the compatible (whole-key-drop) behavior so the
        # invalid-data datasets match the Redisearch reference answers.
        for node_idx in range(self.CLUSTER_SIZE):
            self.client_for_primary(node_idx).execute_command(
                "CONFIG", "SET", "search.emulate-release", COMPAT_EMULATE_RELEASE
            )

        for expected in answers:
            data_set = do_answer_cluster(
                cluster_client=cluster_client,
                expected=expected,
                data_set=data_set,
                test_case=self,
            )

        expected_count = sum(1 for a in answers if not a.get('excluded'))
        if correct_answers != expected_count:
            print(f"Correct answers: {correct_answers} out of {len(answers)}")
            if failed_tests:
                print(">>>>>>>>> Failed Tests <<<<<<<<<")
                for k, v in failed_tests.items():
                    print(f"Failed test {k:60}: {v} times")
            assert False
