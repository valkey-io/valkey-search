import pytest
import re

from .generate import BaseCompatibilityTest

# Compatibility answers for ARRAY values used as *input* to a later stage.
#
# The only way to obtain an ARRAY in the aggregate pipeline is the TOLIST
# reducer, so every case here has the same shape:
#
#   ft.aggregate ... groupby 1 @t1 reduce tolist ... <stage under test>
#
# The prologue turns the "array inputs" dataset into one row per t1 group
# carrying the arrays named below; the tail is the SORTBY / GROUPBY / APPLY
# that consumes them.
DATASET = "array inputs"
# Same shape, but group "ge" has no n2/t2 at all, so TOLIST over those fields
# collects nothing.
DATASET_EMPTY = "array inputs empty"
# Group shapes that tell apart the rules for comparing two arrays.
DATASET_COMPARE = "array compare"

# The base query only selects rows; it plays no part in what is being tested.
FILTER_QUERY = "@n1:[-inf inf]"

# name -> reducer that produces it. Contents per t1 group (ga / gb / gc):
#   items    {1,2,3}  {-1,0.5,4}  {1,2,3}     numeric; ga and gc are equal
#   items2   {10,20,30}  {5,25}   {100,200}   numeric; shorter than items in gb/gc
#   sitems   {apple,Banana,cherry} ...        strings, mixed case
#   tsitems  three unix timestamps            numeric
#   ditems   one date string                  single-element string array
ARRAY_REDUCERS = {
    "items": "reduce tolist 1 @n1 as items",
    "items2": "reduce tolist 1 @n2 as items2",
    "sitems": "reduce tolist 1 @t2 as sitems",
    "tsitems": "reduce tolist 1 @n3 as tsitems",
    "ditems": "reduce tolist 1 @t3 as ditems",
}

DYADIC_OPS = ["+", "-", "*", "/", "^",
              "<", "<=", "==", "!=", ">=", ">",
              "||", "&&"]

# Operand shapes each dyadic operator is tried in.
DYADIC_OPERANDS = [
    ("@items", "2"),        # numeric array on the left
    ("2", "@items"),        # numeric array on the right
    ("@items", "@items2"),  # array/array: equal lengths in ga, mismatched in gb/gc
    ("@sitems", '"a"'),     # string array against a string literal
    ("@sitems", "@items"),  # string array against a numeric array
]

MATH_FUNCS = ["abs", "ceil", "exp", "floor", "log", "log2", "sqrt"]

DATE_COMPONENT_FUNCS = ["dayofweek", "dayofmonth", "dayofyear", "monthofyear",
                        "year", "minute", "hour", "day", "month"]

# Implemented in src/expr/value.cc but not registered in expr::function_table.
ARRAY_FUNCS = ["arraylen(@items)", "arraylen(@sitems)", "arrayat(@items,0)",
               "isarray(@items)", "flatten(@items,1)"]


@pytest.mark.parametrize("key_type", ["json", "hash"])
class TestArrayInputCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "array-input-answers.pickle.gz"

    def _pipeline(self, key_type, tail):
        """One FT.AGGREGATE whose pipeline continues past the TOLIST reducer.

        Only the arrays `tail` names are collected, so the reply carries the
        array under test and nothing else; a tail naming none (a scalar control
        case) still gets `items`, so an array is present in the row.
        """
        named = set(re.findall(r"@(\w+)", tail))
        wanted = [name for name in ARRAY_REDUCERS if name in named] or ["items"]
        reducers = " ".join(ARRAY_REDUCERS[name] for name in wanted)
        # FILTER_QUERY contains a space, so it goes in as one argument.
        cmd = ["ft.aggregate", f"{key_type}_idx1", FILTER_QUERY]
        cmd += (f"load 7 @__key @n1 @n2 @n3 @t1 @t2 @t3 "
                f"groupby 1 @t1 {reducers} {tail}").split()
        self.execute_command(cmd + ["DIALECT", "2"])

    def _apply(self, key_type, expr):
        self._pipeline(key_type, f"apply {expr} as result")

    def _empty_pipeline(self, key_type, tail):
        """Same, over the dataset where one group collects an empty array."""
        cmd = ["ft.aggregate", f"{key_type}_idx1", FILTER_QUERY]
        cmd += ("load 5 @__key @n1 @n2 @t1 @t2 groupby 1 @t1 "
                "reduce tolist 1 @n2 as items "
                f"reduce tolist 1 @t2 as sitems {tail}").split()
        self.execute_command(cmd + ["DIALECT", "2"])

    ### APPLY ###

    def test_apply_dyadic_ops(self, key_type):
        self.setup_data(DATASET, key_type)
        for op in DYADIC_OPS:
            for l, r in DYADIC_OPERANDS:
                self._apply(key_type, f"({l}){op}({r})")

    def test_apply_unary_not(self, key_type):
        self.setup_data(DATASET, key_type)
        self._apply(key_type, "!(@items)")
        self._apply(key_type, "!(@sitems)")

    def test_apply_math_funcs(self, key_type):
        self.setup_data(DATASET, key_type)
        for fn in MATH_FUNCS:
            self._apply(key_type, f"{fn}(@items)")
            self._apply(key_type, f"{fn}(@sitems)")

    def test_apply_string_funcs(self, key_type):
        self.setup_data(DATASET, key_type)
        for fn in ["lower", "upper", "strlen", "exists"]:
            self._apply(key_type, f"{fn}(@items)")
            self._apply(key_type, f"{fn}(@sitems)")
        self._apply(key_type, "substr(@sitems,0,3)")
        self._apply(key_type, "substr(@items,0,1)")
        # Array in a position argument rather than the subject.
        self._apply(key_type, "substr(@sitems,@items,2)")
        self._apply(key_type, 'startswith(@sitems,"a")')
        self._apply(key_type, 'startswith("apple",@sitems)')
        self._apply(key_type, 'startswith(@items,"1")')
        self._apply(key_type, 'contains(@sitems,"an")')
        self._apply(key_type, 'contains("banana",@sitems)')
        self._apply(key_type, 'contains(@items,"1")')
        self._apply(key_type, 'concat(@sitems,"-x")')
        self._apply(key_type, 'concat("x-",@sitems)')
        self._apply(key_type, 'concat(@items,"-x")')
        self._apply(key_type, "concat(@sitems,@sitems)")
        self._apply(key_type, "concat(@sitems,@items)")

    def test_apply_time_funcs(self, key_type):
        self.setup_data(DATASET, key_type)
        for fn in DATE_COMPONENT_FUNCS:
            self._apply(key_type, f"{fn}(@tsitems)")
            self._apply(key_type, f"{fn}(@sitems)")
        self._apply(key_type, "timefmt(@tsitems)")
        self._apply(key_type, 'timefmt(@tsitems,"%Y-%m-%d")')
        self._apply(key_type, "timefmt(@sitems)")
        self._apply(key_type, 'parsetime(@ditems,"%Y-%m-%d")')
        self._apply(key_type, 'parsetime(@sitems,"%Y-%m-%d")')

    def test_apply_array_funcs(self, key_type):
        self.setup_data(DATASET, key_type)
        for expr in ARRAY_FUNCS:
            self._apply(key_type, expr)

    def test_apply_chained(self, key_type):
        """An array flowing through more than one APPLY."""
        self.setup_data(DATASET, key_type)
        self._pipeline(key_type, "apply (@items)+(1) as a apply (@a)*(2) as result")
        self._pipeline(key_type, "apply abs(@items) as a apply strlen(@a) as result")
        self._pipeline(key_type, "apply (@items)+(@items) as a apply (@a)-(@items) as result")

    def test_empty_array(self, key_type):
        """A group whose TOLIST field is absent from every record."""
        self.setup_data(DATASET_EMPTY, key_type)
        for tail in [
            "",  # is an empty array present in the reply at all?
            "apply !(@items) as result",
            "apply (@items)&&(1) as result",
            "apply (@items)||(0) as result",
            "apply !(@sitems) as result",
            "apply exists(@items) as result",
            'apply (@items)<("a") as result',
            'apply (@items)==("") as result',
            "apply (@items)+(1) as result",
            "apply abs(@items) as result",
            "apply strlen(@items) as result",
            "sortby 2 @items asc",
            "sortby 4 @items asc @t1 asc",
            "groupby 1 @items reduce count 0 as cnt",
            "groupby 1 @t1 reduce tolist 1 @items as flat",
            "groupby 1 @t1 reduce count_distinct 1 @items as ncd",
            "groupby 1 @t1 reduce sum 1 @items as nsum",
        ]:
            self._empty_pipeline(key_type, tail)

    def test_array_vs_array_compare(self, key_type):
        """Comparing two arrays -- a query Redisearch accepts.

        Both engines compare lexicographically, but each over its own element
        order, so the answers only agree when those orders happen to agree.
        """
        self.setup_data(DATASET_COMPARE, key_type)
        for op in ["<", "<=", "==", "!=", ">=", ">"]:
            cmd = ["ft.aggregate", f"{key_type}_idx1", FILTER_QUERY]
            cmd += ("load 3 @n1 @n2 @t1 groupby 1 @t1 "
                    "reduce tolist 1 @n1 as items "
                    "reduce tolist 1 @n2 as items2 "
                    f"apply (@items){op}(@items2) as result").split()
            self.execute_command(cmd + ["DIALECT", "2"])

    ### SORTBY ###

    def test_sortby_array(self, key_type):
        self.setup_data(DATASET, key_type)
        for tail in [
            "sortby 2 @items asc",
            "sortby 2 @items desc",
            "sortby 2 @sitems asc",
            "sortby 2 @sitems desc",
            # LIMIT/MAX make the ordering observable in the row *set*, not just
            # in the row order the harness normalizes away.
            "sortby 2 @items asc limit 0 2",
            "sortby 2 @items desc limit 0 2",
            "sortby 2 @items asc max 2",
            "sortby 4 @items asc @t1 asc",
            "sortby 4 @t1 asc @items asc",
            "sortby 2 @t1 asc",  # control: scalar key, array along for the ride
        ]:
            self._pipeline(key_type, tail)

    ### GROUPBY ###

    def test_groupby_array(self, key_type):
        """The array as a GROUPBY key. ga and gc produce equal arrays."""
        self.setup_data(DATASET, key_type)
        for tail in [
            "groupby 1 @items reduce count 0 as cnt",
            "groupby 1 @sitems reduce count 0 as cnt",
            "groupby 2 @items @sitems reduce count 0 as cnt",
            "groupby 1 @items reduce tolist 1 @items as items3",
            "groupby 1 @items reduce sum 1 @items as nsum",
            "groupby 1 @t1 reduce count 0 as cnt",  # control: scalar key
        ]:
            self._pipeline(key_type, tail)

    def test_reduce_over_array(self, key_type):
        """The array as a reducer argument, with a scalar group key."""
        self.setup_data(DATASET, key_type)
        for tail in [
            "groupby 1 @t1 reduce sum 1 @items as nsum",
            "groupby 1 @t1 reduce min 1 @items as nmin reduce max 1 @items as nmax",
            "groupby 1 @t1 reduce avg 1 @items as navg",
            "groupby 1 @t1 reduce stddev 1 @items as nstddev",
            "groupby 1 @t1 reduce count_distinct 1 @items as ncd",
            "groupby 1 @t1 reduce count_distinct 1 @sitems as ncd",
            # TOLIST over an array exercises its one-level flattening path.
            "groupby 1 @t1 reduce tolist 1 @items as flat",
            "groupby 1 @t1 reduce tolist 1 @sitems as sflat",
        ]:
            self._pipeline(key_type, tail)
