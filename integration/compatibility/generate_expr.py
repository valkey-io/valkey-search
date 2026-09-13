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

# A vector field has no clean string conversion: on a hash its raw value is a
# binary blob containing NUL bytes. Redisearch's `contains` scans the needle as
# a C string rather than honoring its length, so a blob needle never terminates
# the scan -- `contains(<any string-valued operand>, @v1)` never returns and
# pins the Redis server at 100% CPU. valkey-search answers all of these
# normally, so this is a defect in the reference engine, not in valkey-search;
# there is simply no reference answer to capture. Skip the vector field in the
# needle (second) position of `contains` on every dataset. The haystack
# position is fine and stays in the sweep. This guard is deliberately
# independent of _skip_hash_case below: that helper excludes @v1 on hash for
# typing reasons which may later be narrowed, and the hang must stay excluded
# regardless.
VECTOR_OPERANDS = {"@v1"}

# --- Redisearch-incompatibility exclusions (HASH only; JSON agrees) --------
# All of these diverge from Redisearch ONLY on the HASH index; the JSON index
# carries real types (numbers, arrays) and agrees, so we exclude by key_type.
#
# 1. @v1 is a raw float32 blob on HASH with no well-defined string/numeric
#    coercion. Redisearch coerces it arbitrarily (blob->0, empty-prefix match);
#    valkey returns Nil/NaN. There is no correct answer to match.
# 2. @n1/@price are numeric fields that valkey types as a double while
#    Redisearch treats them as context-dependent strings -- and Redisearch is
#    itself inconsistent (e.g. "0" is truthy in `(-1)&&@n1` but 0 is falsy in
#    `@n1&&@n2`), so no single typing rule can match it. It only surfaces under
#    typing-sensitive operators/functions (boolean, comparison, logical-not,
#    lower/upper); arithmetic and the rest coerce cleanly and agree.
HASH_BLOB_FIELDS = {"@v1"}
HASH_NUMERIC_FIELDS = {"@n1", "@price"}
BOOLEAN_OPS = {"||", "&&", "!"}
COMPARISON_OPS = {"<", "<=", "==", "!=", ">=", ">"}
STRING_COERCION_FNS = {"lower", "upper"}
# Operands that coerce cleanly to a number, so a numeric-field comparison
# against them stays numeric (and agrees) in both engines.
NUMERIC_OPERANDS = {"0", "-1", "3.14", "-0.5", "+inf", "-inf"} | HASH_NUMERIC_FIELDS


def _skip_hash_case(key_type, name, operands):
    """True to skip a HASH expression that cannot match Redisearch for a
    documented, unfixable reason (see above). `name` is the operator/function;
    `operands` the operand literals/field-refs it is applied to."""
    if key_type != "hash":
        return False
    # @v1 is a raw float32 blob: Redisearch's coercion is arbitrary, so exclude
    # every op/function that touches it (some happen to agree, but only by
    # coincidence of the coercion, not by any stable rule).
    if any(o in HASH_BLOB_FIELDS for o in operands):
        return True
    if any(o in HASH_NUMERIC_FIELDS for o in operands):
        # Boolean ops and lower/upper on a hash numeric field: Redisearch's
        # numeric-vs-string typing is self-inconsistent -> exclude.
        if name in BOOLEAN_OPS or name in STRING_COERCION_FNS:
            return True
        # Comparisons diverge only when the numeric field is compared against a
        # non-numeric (string/tag/text) operand; numeric-vs-numeric agrees, so
        # keep those.
        if name in COMPARISON_OPS and any(o not in NUMERIC_OPERANDS
                                          for o in operands):
            return True
    return False

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
                    if _skip_hash_case(key_type, op, (l, r)):
                        continue
                    self._apply(key_type, filter_q, f"({l}){op}({r})")

    def test_unary_not(self, key_type, dataset):
        self.setup_data(dataset, key_type)
        filter_q, operands = DATASET_CONFIG[dataset]
        for v in operands:
            if _skip_hash_case(key_type, "!", (v,)):
                continue
            self._apply(key_type, filter_q, f"!({v})")

    def test_unary_funcs(self, key_type, dataset):
        self.setup_data(dataset, key_type)
        filter_q, operands = DATASET_CONFIG[dataset]
        for fn in UNARY_FUNCS:
            ops = operands
            if fn in DATE_COMPONENT_FNS:
                ops = [v for v in operands if v not in INFINITY_LITERALS]
            for v in ops:
                if _skip_hash_case(key_type, fn, (v,)):
                    continue
                self._apply(key_type, filter_q, f"{fn}({v})")

    def test_binary_funcs(self, key_type, dataset):
        self.setup_data(dataset, key_type)
        filter_q, operands = DATASET_CONFIG[dataset]
        for fn in BINARY_FUNCS:
            for l in operands:
                for r in operands:
                    if fn == "contains" and r in VECTOR_OPERANDS:
                        continue  # hangs Redisearch, see VECTOR_OPERANDS
                    if _skip_hash_case(key_type, fn, (l, r)):
                        continue
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
