import pytest
from .generate import BaseCompatibilityTest

# Operand-value set V used per slot for every operator/function sweep.
# Each is the *literal string* that will appear in the APPLY expression.
OPERAND_VALUES_GENERIC = [
    "0", "-1", "3.14", "-0.5", "+inf", "-inf",
    '""', '"a"', '"hello"',
]
OPERAND_VALUES_NUMERIC_DATASET = OPERAND_VALUES_GENERIC + ["@n1", "@t1", "@v1"]
OPERAND_VALUES_TEXT_DATASET    = OPERAND_VALUES_GENERIC + ["@title", "@color", "@price"]
OPERAND_VALUES_BAD_DATASET     = OPERAND_VALUES_GENERIC + ["@n1", "@t1", "@v1"]

SMALL_NUMERIC_VALUES = ["0", "1", "-1", "2", "100", "-1000"]  # substr offset/len

DYADIC_OPS = ["+", "-", "*", "/", "^",
              "<", "<=", "==", "!=", ">=", ">",
              "||", "&&"]
UNARY_FUNCS = ["abs", "ceil", "exp", "floor", "log", "log2", "sqrt",
               "lower", "upper", "strlen", "exists",
               "dayofweek", "dayofmonth", "dayofyear", "monthofyear",
               "year", "minute", "hour", "day", "month",
               "timefmt", "parsetime"]
BINARY_FUNCS = ["startswith", "contains", "timefmt", "parsetime"]

# Date-component functions that consume a numeric timestamp and convert to
# a calendar field (or rounded-down period). Both engines hit undefined
# behavior when handed +inf / -inf — they cast to (time_t), hit gmtime_r's
# partial-write-on-overflow path, and produce implementation-specific
# garbage. valkey now guards and returns Nil; Redisearch still returns
# garbage; they will never agree on these inputs without one engine
# changing. Skip ±inf operands for these functions in the sweep.
DATE_COMPONENT_FNS = {"dayofweek", "dayofmonth", "dayofyear", "monthofyear",
                      "year", "minute", "hour", "day", "month"}
INFINITY_LITERALS = {"+inf", "-inf"}

# (filter, operand_values). The filter must match the same set of rows in
# Redisearch and valkey_search — otherwise the per-row APPLY results are
# compared against different row sets and every test cascades into a
# row_count mismatch.
#
# The `bad numbers` dataset was previously included to exercise missing /
# wrong-typed field paths. It has been removed: the two engines apply
# different rules for which documents survive indexing (Redisearch rejects
# the whole doc on wrong-size vectors / non-numeric numerics; valkey
# accepts more leniently), so *no* filter expression matches the same row
# set in both engines on that dataset, and every APPLY result cascades into
# a row_count mismatch unrelated to the expression engine itself.
# Missing-value behavior of APPLY is still exercised indirectly through
# `@v1` (vector field, no clean string conversion) on `hard numbers`.
DATASET_CONFIG = {
    "hard numbers": ("@n1:[-inf inf]", OPERAND_VALUES_NUMERIC_DATASET),
    "pure text":    ("@price:[-inf inf]", OPERAND_VALUES_TEXT_DATASET),
}


@pytest.mark.parametrize("key_type", ["json", "hash"])
@pytest.mark.parametrize("dataset", list(DATASET_CONFIG.keys()))
class TestExprCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "expr-answers.pickle.gz"

    def _apply(self, key_type, filter_query, expr):
        self.execute_command([
            "ft.aggregate", f"{key_type}_idx1",
            filter_query,
            "load", "*",
            "apply", expr, "as", "result",
            "limit", "0", "100",
            "DIALECT", "2",
        ])

    def test_binary_ops(self, key_type, dataset):
        self.setup_data(dataset, key_type)
        filter_q, operands = DATASET_CONFIG[dataset]
        for op in DYADIC_OPS:
            for l in operands:
                for r in operands:
                    self._apply(key_type, filter_q, f"({l}){op}({r})")

    def test_unary_not(self, key_type, dataset):
        self.setup_data(dataset, key_type)
        filter_q, operands = DATASET_CONFIG[dataset]
        for v in operands:
            self._apply(key_type, filter_q, f"!({v})")

    def test_unary_funcs(self, key_type, dataset):
        self.setup_data(dataset, key_type)
        filter_q, operands = DATASET_CONFIG[dataset]
        for fn in UNARY_FUNCS:
            ops = operands
            if fn in DATE_COMPONENT_FNS:
                ops = [v for v in operands if v not in INFINITY_LITERALS]
            for v in ops:
                self._apply(key_type, filter_q, f"{fn}({v})")

    def test_binary_funcs(self, key_type, dataset):
        self.setup_data(dataset, key_type)
        filter_q, operands = DATASET_CONFIG[dataset]
        for fn in BINARY_FUNCS:
            for l in operands:
                for r in operands:
                    self._apply(key_type, filter_q, f"{fn}({l},{r})")

    def test_substr(self, key_type, dataset):
        self.setup_data(dataset, key_type)
        filter_q, operands = DATASET_CONFIG[dataset]
        for s in operands:
            for off in SMALL_NUMERIC_VALUES:
                for ln in SMALL_NUMERIC_VALUES:
                    self._apply(key_type, filter_q, f"substr({s},{off},{ln})")

    def test_concat(self, key_type, dataset):
        self.setup_data(dataset, key_type)
        filter_q, operands = DATASET_CONFIG[dataset]
        self._apply(key_type, filter_q, "concat()")
        for v in operands:
            self._apply(key_type, filter_q, f"concat({v})")
        for l in operands:
            for r in operands:
                self._apply(key_type, filter_q, f"concat({l},{r})")
