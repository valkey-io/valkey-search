"""Pre-1.3 behavior is preserved when search.emulate-release is 1.2.

Each row of the "Known Compatibility Defect Corrections" table in
COMPATIBILITY.md whose fix is newer than 1.2.0 has one check here, asserting
the old (left-hand) behavior.

The whole sequence runs once per entry in MODULES. The real 1.2 release is the
reference: it shows the expected replies are what 1.2 actually answers. Each
later module runs it with search.emulate-release set to RELEASE, and must also
count every row's usage label in search_compatibility_usage.

Each run costs one server start, one HASH index, one JSON index and one data
load. Failures are collected rather than raised one at a time, so a run
reports every row that broke on every module. Set RELEASE to 1.3.0 to see
every check fail on the emulating modules.

When a later release adds rows to the table, add a test_emulate_release_<x_y>.py
for the release before it.
"""

import json
import os
import warnings

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters
from utils import compatibility_usage, extract_old_module

RELEASE = "1.2.0"

# (name, release whose integration/module/<release>-libsearch.so.zip to load,
# or None for the module under test, whether it emulates RELEASE, labels whose
# check that module cannot run). A module that emulates is configured with
# search.emulate-release and must count each legacy path; the real 1.2 release
# has neither.
MODULES = [
    # On 1.2.0, timefmt() with an empty format never returns, and parsetime()
    # with a mismatched format returns uninitialized memory (a different
    # number on every run); the emulation answers "" and -2209075200.
    (
        "1.2",
        "1.2.0",
        False,
        {"timefmt_empty_format_to_nil", "parsetime_format_mismatch_to_nil"},
    ),
    ("1.3", "1.3.0", True, set()),
    ("main", None, True, set()),
]

ABSENT = "<absent>"


def _s(v):
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    if isinstance(v, list):
        return [_s(x) for x in v]
    return v


class TestEmulateRelease1_2(ValkeySearchTestCaseBase):

    module_path = None

    def get_config_file_lines(self, testdir, port):
        lines = super().get_config_file_lines(testdir, port)
        if self.module_path is None:
            return lines
        default = f"loadmodule {os.getenv('MODULE_PATH')}"
        return [
            f"loadmodule {self.module_path}" if line == default else line
            for line in lines
        ]

    def _client(self, release):
        """A client to a server running `release`, or None to skip it."""
        if release is None:
            return self.server.get_new_client()
        if os.environ.get("SAN_BUILD", "no") != "no":
            return None  # Old binaries are not ASAN/TSAN-compatible.
        try:
            self.module_path = extract_old_module(self.server.cwd, release)
        except pytest.skip.Exception as e:
            warnings.warn(f"skipping module {release}: {e}")
            return None
        try:
            return self.start_new_server().client
        finally:
            self.module_path = None

    def _load(self, c: Valkey) -> None:
        assert (
            c.execute_command(
                "FT.CREATE",
                "h",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "h:",
                "SCORE",
                "1.0",
                "SCHEMA",
                "t",
                "TEXT",
                "tg",
                "TAG",
                "n",
                "NUMERIC",
                "SORTABLE",
                "z",
                "TAG",
                "SORTABLE",
            )
            == b"OK"
        )
        assert (
            c.execute_command(
                "FT.CREATE",
                "j",
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "j:",
                "SCHEMA",
                "$.price",
                "AS",
                "price",
                "NUMERIC",
            )
            == b"OK"
        )
        docs = {
            # Expression checks run against this key alone.
            "h:one": {"tg": "one", "n": "9", "z": "apple"},
            # Phrase vs two terms.
            "h:p1": {"tg": "p", "t": "great wall"},
            "h:p2": {"tg": "p", "t": "wall of the great"},
            # Fuzzy distance in bytes vs code points.
            "h:f1": {"tg": "f", "t": "¡hola"},
            # MINSTEMSIZE (4) counted in bytes vs code points: "aés" is
            # 4 bytes, 3 code points.
            "h:st": {"tg": "st", "t": "aés"},
            # MIN/MAX over strings.
            "h:m1": {"tg": "mm", "z": "apple"},
            "h:m2": {"tg": "mm", "z": "Banana"},
            # Sort keys: one key with the sort field, one without.
            "h:s1": {"tg": "sk", "n": "1", "z": "apple"},
            "h:s2": {"tg": "sk", "n": "2"},
            # LOAD * fetching NUMERIC fields as numbers vs raw strings.
            "h:r1": {"tg": "raw", "n": "9"},
            "h:r2": {"tg": "raw", "n": "10"},
            # Malformed NUMERIC: only that field is missing.
            "h:bad": {"tg": "bad", "n": "notanumber"},
        }
        # SORTBY retention: more than the default MAX of 10.
        for i in range(12):
            docs[f"h:many{i}"] = {"tg": "many", "n": str(i)}
        for key, fields in docs.items():
            c.hset(key, mapping=fields)
        # Malformed UTF-8 TAG value: indexed with its raw bytes.
        c.hset("h:nu", mapping={"tg": b"\xff\xfe", "n": "4242"})
        c.execute_command("JSON.SET", "j:1", "$", json.dumps({"price": 10}))
        total = len(docs) + 1
        waiters.wait_for_true(
            lambda: c.execute_command("FT.INFO", "h")[
                c.execute_command("FT.INFO", "h").index(b"num_docs") + 1
            ]
            in (total, str(total).encode()),
            timeout=10,
        )
        waiters.wait_for_true(
            lambda: c.execute_command("FT.SEARCH", "j", "@price:[0 100]", "NOCONTENT")[
                0
            ]
            == 1,
            timeout=10,
        )

    def test_emulate_release_1_2(self):
        failures = []
        for name, release, emulates, skip in MODULES:
            c = self._client(release)
            if c is not None:
                failures += [f"[{name}] {f}" for f in self._run(c, emulates, skip)]
        assert not failures, f"emulate-release {RELEASE}:\n" + "\n".join(failures)

    def _run(self, c: Valkey, emulates: bool, skip: set) -> list:
        """Every check against `c`; returns the failures."""
        if emulates:
            assert (
                c.execute_command("CONFIG", "SET", "search.emulate-release", RELEASE)
                == b"OK"
            )
        self._load(c)
        usage = compatibility_usage if emulates else lambda c: {}
        loaded = usage(c)
        failures = []

        def check(label, fn, expected, at_load=False):
            if label in skip:
                return
            before = loaded if at_load else usage(c)
            base = 0 if at_load else before.get(label, 0)
            try:
                actual = fn()
            except ResponseError as e:
                actual = f"ERR {e}"
            counted = (loaded if at_load else usage(c)).get(label, 0) - base
            if actual != expected:
                failures.append(f"{label}: expected {expected!r}, got {actual!r}")
            if emulates and counted <= 0:
                failures.append(f"{label}: legacy path was not counted")

        def search(*args):
            return _s(c.execute_command("FT.SEARCH", "h", *args))

        def agg(index, *args):
            reply = _s(c.execute_command("FT.AGGREGATE", index, *args))
            return [dict(zip(r[::2], r[1::2])) for r in reply[1:]]

        # 1.2.0 has no FT.AGGREGATE autoload: a stage sees only the fields a
        # LOAD clause names, so every check that reads a field LOADs it.
        def apply(expr, *head):
            rows = agg(
                "h", "@tg:{one}", *head, "LOAD", "1", "@n", "APPLY", expr, "AS", "x"
            )
            if len(rows) != 1:
                return rows
            return rows[0].get("x", ABSENT)

        def count(query):
            return search(query, "NOCONTENT")[0]

        # ---- Ingestion-time rows: decided when the keys were indexed. ----
        check("invalid_data_drops_key", lambda: count("@tg:{bad}"), 1, at_load=True)
        check("tag_rejects_non_utf8", lambda: count("@n:[4242 4242]"), 1, at_load=True)
        check("min_stem_size_code_points", lambda: count("aé"), 1, at_load=True)

        # ---- FT.SEARCH ----
        check("fuzzy_code_point_distance", lambda: count("%hola%"), 0)
        check("filter_parser_invalid_utf8_expression", lambda: count(b"@t:abc\xff"), 0)
        check("ft_search_apostrophe_phrase", lambda: count("'great wall'"), 2)
        check(
            "ft_search_return_last_wins",
            lambda: search("@tg:{one}", "RETURN", "1", "n", "RETURN", "1", "z"),
            [1, "h:one", ["n", "9", "z", "apple"]],
        )
        # Only the key without z: 1.2.0 sorts a missing value first and
        # reports every sort key of the reply as the first one's.
        check(
            "ft_search_sortkey_nil",
            lambda: search(
                "@tg:{sk} @n:[2 2]",
                "SORTBY",
                "z",
                "ASC",
                "WITHSORTKEYS",
                "RETURN",
                "1",
                "n",
            ),
            [1, "h:s2", "#", ["n", "2"]],
        )
        check(
            "ft_search_sortkey_type_prefix",
            lambda: search(
                "@tg:{one}", "SORTBY", "z", "ASC", "WITHSORTKEYS", "RETURN", "1", "z"
            ),
            [1, "h:one", "#apple", ["z", "apple"]],
        )

        # ---- FT.INFO ----
        def index_definition():
            info = _s(c.execute_command("FT.INFO", "h"))
            return info[info.index("index_definition") + 1]

        check(
            "ft_info_score_field",
            index_definition,
            ["key_type", "HASH", "prefixes", ["h:"], "default_score", "1"],
        )

        # ---- FT.AGGREGATE: LOAD, names, reducers, SORTBY ----
        check(
            "aggregate_json_field_names",
            lambda: agg("j", "@price:[0 100]", "LOAD", "1", "@price"),
            [{"$.price": "10"}],
        )
        check(
            "ft_aggregate_load_as",
            lambda: agg("h", "@tg:{one}", "LOAD", "3", "@n", "AS", "nn"),
            "ERR Index field `AS` does not exist",
        )
        check(
            "ft_aggregate_loadall_fetch",
            lambda: agg("h", "@tg:{one}", "LOAD", "*", "LOAD", "1", "@__key"),
            [{"tg": "one", "n": "9", "z": "apple"}],
        )
        check(
            "aggregate_load_all_raw_strings",
            lambda: [
                r["n"]
                for r in agg("h", "@tg:{raw}", "LOAD", "*", "SORTBY", "2", "@n", "ASC")
            ],
            ["9", "10"],
        )
        check(
            "aggregate_reducer_default_alias",
            lambda: agg(
                "h",
                "@tg:{mm}",
                "LOAD",
                "1",
                "@tg",
                "GROUPBY",
                "1",
                "@tg",
                "REDUCE",
                "COUNT",
                "0",
            ),
            [{"tg": "mm", "COUNT()": "2"}],
        )
        check(
            "reduce_minmax_numeric",
            lambda: agg(
                "h",
                "@tg:{mm}",
                "LOAD",
                "2",
                "@tg",
                "@z",
                "GROUPBY",
                "1",
                "@tg",
                "REDUCE",
                "MIN",
                "1",
                "@z",
                "AS",
                "m",
            ),
            [{"tg": "mm", "m": "Banana"}],
        )
        check(
            "reduce_sum_avg_empty_nan",
            lambda: agg(
                "h",
                "@tg:{mm}",
                "LOAD",
                "2",
                "@tg",
                "@n",
                "GROUPBY",
                "1",
                "@tg",
                "REDUCE",
                "SUM",
                "1",
                "@n",
                "AS",
                "s",
            ),
            [{"tg": "mm", "s": "0"}],
        )
        check(
            "sortby_max_follows_limit",
            lambda: len(
                agg(
                    "h",
                    "@tg:{many}",
                    "LOAD",
                    "1",
                    "@n",
                    "SORTBY",
                    "2",
                    "@n",
                    "ASC",
                    "LIMIT",
                    "0",
                    "1000",
                )
            ),
            10,
        )

        # ---- FT.AGGREGATE: missing fields and nil values ----
        # h:s2 has no z. (1.2.0 rejects a TEXT field in an expression.)
        check(
            "apply_drops_missing_field",
            lambda: agg(
                "h", "@tg:{sk} @n:[2 2]", "LOAD", "1", "@z", "APPLY", "@z", "AS", "x"
            ),
            [{}],
        )
        check(
            "expr_missing_propagates",
            lambda: agg(
                "h", "@tg:{sk} @n:[2 2]", "LOAD", "1", "@z", "APPLY", "!(@z)", "AS", "x"
            ),
            [{"x": "1"}],
        )
        check(
            "aggregate_nil_alias_named",
            lambda: agg("h", "@tg:{one}", "APPLY", 'abs("abc")', "AS", "x"),
            [{}],
        )

        # ---- FT.AGGREGATE: expression semantics ----
        check("ft_aggregate_divide_by_zero", lambda: apply("1/0"), "nan")
        check("ft_aggregate_pow_precedence", lambda: apply("2*3^2"), "36")
        check("ft_aggregate_logical_precedence", lambda: apply("1||0&&0"), "0")
        check("empty_string_not_numeric", lambda: apply('abs("")'), "0")
        check(
            "expr_numeric_params",
            lambda: apply("$p", "PARAMS", "2", "p", "1.50"),
            "1.50",
        )
        # At 1.2.0 nothing on the right of `||` can raise an error (the typed
        # comparisons and string functions that do are 1.3.0 fixes too), so
        # evaluating it changes no reply: only the usage count tells the two
        # paths apart.
        check("expr_short_circuit", lambda: apply('1 || (@n < "abc")'), "1")
        check("expr_string_fn_types", lambda: apply("strlen(123)"), "3")
        check("expr_typed_comparison", lambda: apply('@n < "abc"'), "1")

        # ---- 1.2.1 rows ----
        check("asbool_string_truthy", lambda: apply('!"abc"'), "1")
        check("date_fn_negative_ts_to_nil", lambda: apply("year(-86400)"), "1969")
        check("lower_non_string_to_nil", lambda: apply("lower(12)"), "12")
        check("upper_non_string_to_nil", lambda: apply("upper(12)"), "12")
        # 1700000000 is 2023-11-14; the 1st is 1698796800.
        check("month_mday_off_by_one", lambda: apply("month(1700000000)"), "1698710400")
        check("numeric_unary_nan_on_unparsable", lambda: apply('abs("abc")'), ABSENT)
        check(
            "parsetime_format_mismatch_to_nil",
            lambda: apply('parsetime("x", "%Y")'),
            "-2209075200",
        )
        check("timefmt_empty_format_to_nil", lambda: apply('timefmt(0, "")'), "")

        return failures
