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

# String functions taking more than one argument, tried with the array as the
# subject, as the other argument, and -- for substr -- as a position argument.
STRING_FUNC_EXPRS = [
    "substr(@sitems,0,3)",
    "substr(@items,0,1)",
    # Array in a position argument rather than the subject.
    "substr(@sitems,@items,2)",
    'startswith(@sitems,"a")',
    'startswith("apple",@sitems)',
    'startswith(@items,"1")',
    'contains(@sitems,"an")',
    'contains("banana",@sitems)',
    'contains(@items,"1")',
    'concat(@sitems,"-x")',
    'concat("x-",@sitems)',
    'concat(@items,"-x")',
    "concat(@sitems,@sitems)",
    "concat(@sitems,@items)",
]


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
            for lhs, rhs in DYADIC_OPERANDS:
                self._apply(key_type, f"({lhs}){op}({rhs})")

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
        for expr in STRING_FUNC_EXPRS:
            self._apply(key_type, expr)

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

    def _empty_reduce(self, key_type, tail):
        """Reducers straight over the dataset where group ge lacks n2 and t2."""
        cmd = ["ft.aggregate", f"{key_type}_idx1", FILTER_QUERY]
        cmd += f"load 5 @__key @n1 @n2 @t1 @t2 groupby 1 @t1 {tail}".split()
        self.execute_command(cmd + ["DIALECT", "2"])

    def _missing_pipeline(self, key_type, tail):
        """A stage reading fields straight off the empty dataset.

        Unlike _empty_pipeline there is no TOLIST prologue, so t2 and n2 are
        genuinely absent for the ge rows rather than collected into an empty
        array. That is the distinction the empty-array tests do not draw.
        """
        cmd = ["ft.aggregate", f"{key_type}_idx1", FILTER_QUERY]
        cmd += f"load 5 @__key @n1 @n2 @t1 @t2 {tail}".split()
        self.execute_command(cmd + ["DIALECT", "2"])

    def test_reduce_all_nil(self, key_type):
        """A reducer whose argument is absent from every record in its group.

        Group ge has neither n2 nor t2, so these pin down what each engine
        replies when a reducer never saw a value: the alias named with a nil,
        the alias omitted, or an identity value. COUNT rides along so the
        group is visible even when the reducer's own field is dropped.
        """
        self.setup_data(DATASET_EMPTY, key_type)
        for reducer in [
            "reduce min 1 @n2 as m",
            "reduce max 1 @n2 as m",
            "reduce min 1 @t2 as m",
            "reduce max 1 @t2 as m",
            "reduce sum 1 @n2 as m",
            "reduce avg 1 @n2 as m",
            "reduce stddev 1 @n2 as m",
            "reduce count_distinct 1 @n2 as m",
            "reduce tolist 1 @n2 as m",
            "reduce first_value 3 @n2 BY @n1 as m",
            "reduce first_value 3 @t2 BY @n1 as m",
            "reduce first_value 4 @n2 BY @n1 desc as m",
        ]:
            self._empty_reduce(key_type, f"reduce count 0 as c {reducer}")

    def test_apply_missing_field(self, key_type):
        """APPLY over a field absent from some records.

        The same question as test_reduce_all_nil, but through an APPLY alias,
        which is the other place a computed output can carry a default nil.
        """
        self.setup_data(DATASET_EMPTY, key_type)
        for tail in [
            "apply @t2 as x",
            "apply @n2 as x",
            'apply concat(@t2,"!") as x',
            "apply (@n2)+(1) as x",
            "apply exists(@t2) as x",
        ]:
            cmd = ["ft.aggregate", f"{key_type}_idx1", FILTER_QUERY]
            cmd += f"load 5 @__key @n1 @n2 @t1 @t2 {tail}".split()
            self.execute_command(cmd + ["DIALECT", "2"])

    def test_apply_missing_through_functions(self, key_type):
        """A missing field reaching each class of function.

        Dyadic::Evaluate propagates missingness, so `(@t2)+(1)` over an absent
        t2 stays missing and the record goes. Nothing else does: Not reads a
        nil as false and answers true, and every function proxy evaluates its
        arguments and returns its own reason. These pin down which of those
        Redisearch agrees with.
        """
        self.setup_data(DATASET_EMPTY, key_type)
        for tail in [
            "apply !(@t2) as x",  # Not: valkey answers true, keeps the record
            "apply abs(@n2) as x",  # monadic, numeric
            "apply lower(@t2) as x",  # monadic, string
            "apply upper(@t2) as x",
            "apply strlen(@t2) as x",
            "apply dayofweek(@n2) as x",  # monadic, time
            "apply timefmt(@n2) as x",
            'apply parsetime(@t2,"%Y-%m-%d") as x',
            'apply startswith(@t2,"a") as x',  # dyadic function
            'apply contains(@t2,"a") as x',
            "apply substr(@t2,0,1) as x",  # triadic function
            'apply concat(@t2,"!") as x',  # variadic function
            "apply (@n2)&&(1) as x",  # dyadic operator, already propagating
            "apply (@n2)||(0) as x",
            "apply (@t2)==(@t2) as x",  # missing on both sides
        ]:
            self._missing_pipeline(key_type, tail)

    def test_apply_missing_chained(self, key_type):
        """Missingness surviving an alias hop.

        The first APPLY names an alias from an absent field; the second reads
        that alias. If the record survives the first stage at all, this says
        whether what it carries is still missing.
        """
        self.setup_data(DATASET_EMPTY, key_type)
        for tail in [
            "apply @t2 as x apply exists(@x) as y",
            "apply @t2 as x apply (@x)==(@t2) as y",
            'apply @t2 as x apply concat(@x,"!") as y',
            "apply @n2 as x apply (@x)+(1) as y",
        ]:
            self._missing_pipeline(key_type, tail)

    def test_filter_missing_field(self, key_type):
        """FILTER over an absent field.

        Filter keeps a record when the predicate IsTrue(), and a nil is falsy,
        so valkey drops these -- by falsiness rather than by missingness. The
        negations are the interesting half: Not answers true for a missing
        operand, so valkey keeps those.
        """
        self.setup_data(DATASET_EMPTY, key_type)
        for tail in [
            "filter @t2",
            "filter !(@t2)",
            "filter @n2",
            "filter !(@n2)",
            "filter exists(@t2)",
            "filter !(exists(@t2))",
            'filter (@t2)==("apple")',
            "filter (@n2)>(5)",
        ]:
            self._missing_pipeline(key_type, tail)

    def test_groupby_missing_field(self, key_type):
        """GROUPBY on an absent field.

        The key value is missing, so ReplyWithValue leaves the group key out
        of the reply entirely -- a group identified by nothing. This says
        whether Redisearch groups such records, drops them, or names the key.
        """
        self.setup_data(DATASET_EMPTY, key_type)
        for tail in [
            "groupby 1 @t2 reduce count 0 as c",
            "groupby 1 @n2 reduce count 0 as c",
            "groupby 2 @t1 @t2 reduce count 0 as c",
            "groupby 1 @t2 reduce tolist 1 @n1 as items",
        ]:
            self._missing_pipeline(key_type, tail)

    def test_sortby_missing_field(self, key_type):
        """SORTBY on an absent field.

        Compare answers kUNORDERED against a nil and SortFunctor treats that
        as a tie, so valkey leaves such records in scan order.
        """
        self.setup_data(DATASET_EMPTY, key_type)
        for tail in [
            "sortby 2 @t2 asc",
            "sortby 2 @t2 desc",
            "sortby 2 @n2 asc",
            "sortby 4 @t1 asc @n2 asc",
        ]:
            self._missing_pipeline(key_type, tail)

    def test_array_vs_array_compare(self, key_type):
        """Comparing two arrays -- a query Redisearch accepts.

        Both engines compare lexicographically, but each over its own element
        order, so the answers only agree when those orders happen to agree.
        """
        self.setup_data(DATASET_COMPARE, key_type)
        # The ordered comparisons -- "<", "<=", ">=", ">" -- are left out: both
        # engines compare element by element, so their answer follows whichever
        # element each engine happens to hold first, and Redisearch's order is
        # its hash table's. Equality is unaffected by that for these shapes.
        for op in ["==", "!="]:
            cmd = ["ft.aggregate", f"{key_type}_idx1", FILTER_QUERY]
            cmd += ("load 3 @n1 @n2 @t1 groupby 1 @t1 "
                    "reduce tolist 1 @n1 as items "
                    "reduce tolist 1 @n2 as items2 "
                    f"apply (@items){op}(@items2) as result").split()
            self.execute_command(cmd + ["DIALECT", "2"])

    ### FILTER ###

    def test_filter_array(self, key_type):
        """The array reaching a FILTER stage predicate."""
        self.setup_data(DATASET, key_type)
        for tail in [
            "filter @items",  # the array's own truthiness
            "filter !(@items)",
            "filter (@items)>(2)",
            "filter (@items)==(2)",
            'filter (@sitems)==("apple")',
            'filter startswith(@sitems,"a")',
            "filter exists(@items)",
            "filter (@items)==(@items2)",
            'filter (@t1)==("ga")',  # control: scalar predicate
        ]:
            self._pipeline(key_type, tail)

    def test_filter_empty_array(self, key_type):
        """A FILTER predicate over a group whose TOLIST collected nothing."""
        self.setup_data(DATASET_EMPTY, key_type)
        for tail in [
            "filter @items",
            "filter !(@items)",
            "filter exists(@items)",
            'filter (@items)==("")',
        ]:
            self._empty_pipeline(key_type, tail)

    ### SORTBY ###

    def test_sortby_array(self, key_type):
        self.setup_data(DATASET, key_type)
        for tail in [
            "sortby 2 @items asc",
            "sortby 2 @items desc",
            "sortby 2 @sitems asc",
            "sortby 2 @sitems desc",
            # LIMIT and MAX would make the ordering observable in the row *set*
            # rather than in the row order the harness normalizes away, and the
            # two engines order arrays by whichever element each holds first:
            #   sortby 2 @items asc limit 0 2
            #   sortby 2 @items desc limit 0 2
            #   sortby 2 @items asc max 2
            # Redisearch's element order is its hash table's, so there is no
            # ordering to match; these are left out deliberately.
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

    def test_first_value_over_array(self, key_type):
        """FIRST_VALUE with an array as the value, and as the BY key.

        Simple mode is left out for the reason upstream's generate.py gives:
        without BY the pick depends on retrieval order, which the two engines
        do not share. The one-row-per-group shapes have a single record, so
        the pick is deterministic whatever the BY key compares like; the
        collapsed shapes group every row together and sort on scalar t1.
        """
        self.setup_data(DATASET, key_type)
        for tail in [
            "groupby 1 @t1 reduce first_value 3 @items BY @t1 as fv",
            "groupby 1 @t1 reduce first_value 4 @items BY @t1 desc as fv",
            "groupby 1 @t1 reduce first_value 3 @sitems BY @t1 asc as fv",
            # The array as the sort key rather than the returned value.
            "groupby 1 @t1 reduce first_value 3 @t1 BY @items as fv",
            # Collapse every row into one group so the reducer actually picks.
            'apply "x" as k groupby 1 @k reduce first_value 4 @items BY @t1 asc as fv',
            'apply "x" as k groupby 1 @k reduce first_value 4 @sitems BY @t1 desc as fv',
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
