"""Query construction for the scoring compatibility suite.

Every query is built together with its predicted hit set, so generate_scoring.py
can check the capture against it. Most shapes first pick an "anchor" document
and build the query around it, which guarantees at least one hit.
"""

import random
import struct
from dataclasses import dataclass, field

from .data_sets import (
    SCORING_DT_TIERS,
    SCORING_NUM_DOCS,
    SCORING_TAG_FREQS,
    SCORING_VECTOR_CLUSTERS,
    VECTOR_DIM,
    compute_scoring_corpus,
)
from .text_query_builder import sample_shape

SCORING_QUERY_SEED = 7331

# Queries per sampled shape. single_term (every pool term), numeric_only and
# match_all are fixed lists instead.
SHAPE_COUNTS = {
    "and": 200,
    "or": 200,
    "mixed": 200,
    "weight": 200,
    "text_numeric_tag": 200,
    "tag_only": 200,
    "text_vector": 200,
    "text_numeric_tag_vector": 200,
}

# --- text ---
# terms per flat and/or query
FLAT_TERM_COUNTS = [2, 3, 4]
# sample_shape depths for mixed; depth 1 is and/or
MIXED_DEPTHS = [2, 3]
# text shapes the combined (text + numeric/tag/vector) shapes draw from
TEXT_KINDS = ["single_term", "and", "or", "mixed", "weight"]
# ceiling on a union leg's dt: a dt 450 leg is ~450 rows for a few overlap rows
OR_LEG_MAX_DT = 250

# --- weights ---
# Positive (valkey-search rejects <= 0) and float32-exact, so rounding cannot differ.
WEIGHTS = [0.125, 0.25, 0.5, 1.5, 2.0, 3.0, 4.0, 6.0]
# chance a group in a weighted tree also takes a weight
GROUP_WEIGHT_RATE = 0.3

# --- tag / numeric ---
# values per tag union; 1 is a single tag
TAG_VALUE_COUNTS = [1, 2, 3, 4]
# half-width of the @n1 window around the anchor doc
NUMERIC_WINDOW = 20
# n1 == doc id, so each range's hit set is the range itself
NUMERIC_RANGES = [(0, 9), (10, 19), (0, 49), (100, 149), (245, 255),
                  (250, 250), (300, 399), (400, 499), (0, 249),
                  (250, 499), (499, 499), (0, SCORING_NUM_DOCS - 1)]

# --- vector ---
# KNN k, drawn uniformly; the top end is just past one cluster's worth (62 docs)
VECTOR_K_RANGE = (1, 65)

# The default LIMIT 0 10 would cut inside a score tie, exposing each engine's tie-break.
SEARCH_LIMIT = SCORING_NUM_DOCS


def build_scoring_queries(seed=SCORING_QUERY_SEED):
    """{shape: [{shape, query, hits, params}]} over the recorded corpus."""
    return _QueryBuilder(random.Random(seed)).build()


def search_args(index, descriptor):
    """The full FT.SEARCH argument list for one descriptor."""
    # SCORER is explicit (RediSearch defaults to TFIDF); NOCONTENT drops the doc bodies.
    return ["FT.SEARCH", index, descriptor["query"],
            "SCORER", "BM25STD", "WITHSCORES", "NOCONTENT",
            "LIMIT", "0", str(SEARCH_LIMIT),
            *descriptor["params"], "DIALECT", "2"]


def _weight(query, w):
    """Attach a weight block; the group must already be parenthesized."""
    return f"{query} => {{ $weight: {w} }}"


def _root_op(shape):
    """A sample_shape tree's operator under any group wrappers."""
    while shape != "A" and shape[0] == "G":
        shape = shape[1]
    return shape if shape == "A" else shape[0]


def _ops_of(shape):
    return set() if shape == "A" else {shape[0]}.union(*map(_ops_of, shape[1:]))


@dataclass
class _Tree:
    """Per-tree state while rendering one sample_shape tree."""
    anchor_terms: list          # terms of the anchor doc
    weighted: set               # leaf indexes (render order) that get a weight
    used: set = field(default_factory=set)


class _QueryBuilder:
    # Call order matters: every draw comes from one seeded rng, so reordering
    # anything changes every later query.

    def __init__(self, rng):
        self.rng = rng
        self.docs, self.terms = compute_scoring_corpus()   # terms: {term: {doc: tf}}

        tiers = [sorted(t for t, posting in self.terms.items() if len(posting) == dt)
                 for _, dt in SCORING_DT_TIERS]
        self.or_tiers = [tier for tier, (_, dt) in zip(tiers, SCORING_DT_TIERS)
                         if dt <= OR_LEG_MAX_DT]
        # terms a non-anchored leaf may use
        self.free_terms = sorted(t for tier in self.or_tiers for t in tier)

        self.doc_terms = {doc: [] for doc in self.docs}
        for term, posting in self.terms.items():
            for doc in posting:
                self.doc_terms[doc].append(term)
        for terms_in_doc in self.doc_terms.values():
            terms_in_doc.sort()

        self.tag_docs = {value: {d for d, f in self.docs.items() if f["t1"] == value}
                         for value in SCORING_TAG_FREQS}

    def build(self):
        self.shapes = {}
        emit, rng = self._emit, self.rng

        for term in sorted(self.terms):
            emit("single_term", term, self.terms[term])

        for _ in range(SHAPE_COUNTS["and"]):
            _, drawn = self._terms_from_doc(rng.choice(FLAT_TERM_COUNTS))
            emit("and", " ".join(drawn), self._all_of(drawn))

        for _ in range(SHAPE_COUNTS["or"]):
            drawn = self._terms_across_tiers(rng.choice(FLAT_TERM_COUNTS))
            emit("or", " | ".join(drawn), self._any_of(drawn))

        for _ in range(SHAPE_COUNTS["mixed"]):
            emit("mixed", *self._tree()[1:])

        for i in range(SHAPE_COUNTS["weight"]):
            # every 5th is a weighted tag leaf; the rest weighted text trees
            if i % 5 == 4:
                value = sorted(SCORING_TAG_FREQS)[i % len(SCORING_TAG_FREQS)]
                emit("weight", _weight(f"(@t1:{{{value}}})", WEIGHTS[i % len(WEIGHTS)]),
                     self.tag_docs[value])
            else:
                emit("weight", *self._tree(weight_leaves=True)[1:])

        # t1 is single-valued, so a tag AND never matches; unions only
        for _ in range(SHAPE_COUNTS["tag_only"]):
            values = rng.sample(sorted(SCORING_TAG_FREQS), rng.choice(TAG_VALUE_COUNTS))
            emit("tag_only", self._tag_query(values), self._tag_hits(values))

        # a numeric leaf scores 0 on every row
        for low, high in NUMERIC_RANGES:
            emit("numeric_only", f"@n1:[{low} {high}]", range(low, high + 1))

        for _ in range(SHAPE_COUNTS["text_numeric_tag"]):
            emit("text_numeric_tag", *self._text_numeric_tag())

        emit("match_all", "*", self.docs)

        # Hybrid scores are the text score alone, so these cover KNN eviction only.
        for _ in range(SHAPE_COUNTS["text_vector"]):
            _, text, hits = self._text(rng.choice(TEXT_KINDS))
            emit("text_vector", *self._with_knn(text, hits))

        for _ in range(SHAPE_COUNTS["text_numeric_tag_vector"]):
            emit("text_numeric_tag_vector", *self._with_knn(*self._text_numeric_tag()))

        return self.shapes

    def _emit(self, shape, query, hits, params=()):
        self.shapes.setdefault(shape, []).append(
            {"shape": shape, "query": query, "hits": set(hits), "params": tuple(params)})

    # --- terms ---

    def _terms_from_doc(self, count):
        """(doc, `count` distinct terms of that doc)."""
        while True:
            doc = self.rng.randrange(SCORING_NUM_DOCS)
            if len(self.doc_terms[doc]) >= count:
                return doc, self.rng.sample(self.doc_terms[doc], count)

    def _terms_across_tiers(self, count):
        """One term from each of `count` distinct document-frequency tiers."""
        return [self.rng.choice(tier) for tier in self.rng.sample(self.or_tiers, count)]

    def _all_of(self, drawn):
        return set.intersection(*(set(self.terms[t]) for t in drawn))

    def _any_of(self, drawn):
        return set.union(*(set(self.terms[t]) for t in drawn))

    # --- text ---

    def _text(self, kind, parent_op=None):
        """(anchor doc, query, hits) for one TEXT_KINDS shape, parenthesized so it
        can be embedded in a larger query."""
        rng = self.rng
        if kind in ("mixed", "weight"):
            return self._tree(weight_leaves=(kind == "weight"), parent_op=parent_op)
        if kind == "single_term":
            doc, (term,) = self._terms_from_doc(1)
            return doc, term, set(self.terms[term])
        count = rng.choice(FLAT_TERM_COUNTS)
        if kind == "or":
            # one leg from the anchor doc, the rest free
            doc, (term,) = self._terms_from_doc(1)
            drawn = [term] + rng.sample([t for t in self.free_terms if t != term],
                                        count - 1)
            return doc, f"({' | '.join(drawn)})", self._any_of(drawn)
        doc, drawn = self._terms_from_doc(count)
        return doc, f"({' '.join(drawn)})", self._all_of(drawn)

    def _tree(self, weight_leaves=False, parent_op=None):
        """(anchor doc, query, hits) for a random tree mixing AND and OR.

        weight_leaves weights 1+ leaves (and some groups). parent_op is the
        operator the tree will be embedded in, if any.
        """
        rng = self.rng
        shape = sample_shape(rng.choice(MIXED_DEPTHS), rng)
        while not {"AND", "OR"} <= _ops_of(shape):
            shape = sample_shape(rng.choice(MIXED_DEPTHS), rng)
        leaves = str(shape).count("'A'")
        doc, _ = self._terms_from_doc(leaves)
        weighted = (set(rng.sample(range(leaves), rng.randint(1, leaves)))
                    if weight_leaves else set())
        tree = _Tree(self.doc_terms[doc], weighted)
        return (doc, *self._render(shape, tree, True, parent_op))

    def _render(self, shape, tree, anchored, parent_op):
        """(query, hits) for a subtree. An anchored subtree matches the anchor doc:
        AND anchors both sides, OR one random side."""
        rng = self.rng
        if shape == "A":
            pool = tree.anchor_terms if anchored else self.free_terms
            term = rng.choice([t for t in pool if t not in tree.used])
            index = len(tree.used)
            tree.used.add(term)
            # a weight never changes the hit set
            if index in tree.weighted:
                return _weight(f"({term})", rng.choice(WEIGHTS)), set(self.terms[term])
            return term, set(self.terms[term])

        if shape[0] == "G":
            query, hits = self._render(shape[1], tree, anchored, parent_op)
            return self._maybe_weight_group(f"({query})", shape, tree, parent_op), hits

        op, left, right = shape
        left_anchored = anchored and (op == "AND" or rng.random() < 0.5)
        right_anchored = anchored and (op == "AND" or not left_anchored)
        lq, lh = self._render(left, tree, left_anchored, op)
        rq, rh = self._render(right, tree, right_anchored, op)
        if op == "AND":
            query, hits = f"({lq} {rq})", lh & rh
        else:
            query, hits = f"({lq} | {rq})", lh | rh
        return self._maybe_weight_group(query, shape, tree, parent_op), hits

    def _maybe_weight_group(self, query, shape, tree, parent_op):
        """Sometimes weight a group, but only in a tree with weighted leaves."""
        # Redis flattens an OR in an OR (AND in an AND) and spreads the inner
        # weight over the whole parent; see known_differences.md.
        if _root_op(shape) == parent_op:
            return query
        if tree.weighted and self.rng.random() < GROUP_WEIGHT_RATE:
            return _weight(query, self.rng.choice(WEIGHTS))
        return query

    # --- tag / numeric ---

    def _tag_query(self, values):
        """A tag union, spelled either as one braces group or as separate clauses."""
        if self.rng.random() < 0.5 or len(values) == 1:
            return f"@t1:{{{' | '.join(values)}}}"
        return " | ".join(f"@t1:{{{v}}}" for v in values)

    def _tag_hits(self, values):
        return set().union(*(self.tag_docs[v] for v in values))

    def _text_numeric_tag(self):
        """(query, hits) ANDing a text shape, an @n1 window and a tag union, all
        around one anchor doc."""
        rng = self.rng
        # the text sits in the top-level AND, so it must not weight an AND root
        doc, text, text_hits = self._text(rng.choice(TEXT_KINDS), parent_op="AND")
        low = max(0, doc - NUMERIC_WINDOW)
        high = min(SCORING_NUM_DOCS - 1, doc + NUMERIC_WINDOW)
        # the doc's own tag plus others, so the doc matches the union
        own = self.docs[doc]["t1"]
        others = rng.sample(sorted(v for v in SCORING_TAG_FREQS if v != own),
                            rng.choice(TAG_VALUE_COUNTS) - 1)
        values = [own] + others
        rng.shuffle(values)
        tag = self._tag_query(values)
        if " | @" in tag:
            tag = f"({tag})"
        return (f"{text} @n1:[{low} {high}] {tag}",
                text_hits & set(range(low, high + 1)) & self._tag_hits(values))

    # --- vector ---

    def _with_knn(self, query, hits):
        """(query, hits, params) using `query` as the filter of a random KNN."""
        rng = self.rng
        cluster = rng.randrange(SCORING_VECTOR_CLUSTERS)
        k = rng.randint(*VECTOR_K_RANGE)
        blob = struct.pack(f"<{VECTOR_DIM}f", *([float(cluster)] * VECTOR_DIM))
        # v1 distances to the probe are all distinct, so the k nearest are exact
        nearest = sorted(hits, key=lambda d: abs(self.docs[d]["v1"][0] - cluster))
        # a trailing $weight must not run into the KNN arrow
        if " " in query:
            query = f"({query})"
        return f"{query}=>[KNN {k} @v1 $q]", nearest[:k], ("PARAMS", "2", "q", blob)
