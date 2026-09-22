"""Query construction for the scoring compatibility suite."""

import random
import struct

from .data_sets import (
    SCORING_DT_TIERS,
    SCORING_NUM_DOCS,
    SCORING_TAG_FREQS,
    SCORING_VECTOR_CLUSTERS,
    VECTOR_DIM,
    compute_scoring_corpus,
)

SCORING_QUERY_SEED = 7331

# Single term is exhaustive over the pool; the combinatorial shapes are sampled.
SHAPE_COUNTS = {
    "and2": 30,
    "and3": 30,
    "or2": 30,
    "or3": 30,
    "mixed": 30,
    "cross_field": 10,       # x3 spellings
    "leaf_weight": 25,
    "nested_weight": 25,
    "text_numeric_tag": 25,
}

# Positive (valkey-search rejects <= 0) and float32-exact, so rounding cannot differ.
WEIGHTS = [0.25, 0.5, 2.0, 3.0, 4.0]

# A k this large cannot truncate a filter set drawn from a single cluster.
VECTOR_K_FULL = SCORING_NUM_DOCS // SCORING_VECTOR_CLUSTERS
VECTOR_K_TRUNCATED = 5

# The default LIMIT 0 10 would cut inside a score tie, exposing each engine's tie-break.
SEARCH_LIMIT = SCORING_NUM_DOCS


def _weight(query, w):
    """Attach a QMA weight block; the group must already be parenthesized."""
    return f"{query} => {{ $weight: {w} }}"


def _tiers(terms):
    """Pool terms bucketed by document frequency, richest tier first."""
    return [sorted(t for t, posting in terms.items() if len(posting) == dt)
            for _, dt in SCORING_DT_TIERS]


def _field_incidence(docs, terms):
    """{field: {term: {doc ids}}}, which the doc-wide TF in `terms` cannot say."""
    incidence = {"title": {t: set() for t in terms}, "body": {t: set() for t in terms}}
    for doc, fields in docs.items():
        for field in ("title", "body"):
            for word in set(fields[field]):
                if word in terms:          # skip filler, which has no recorded dt
                    incidence[field][word].add(doc)
    return incidence


def build_scoring_queries(seed=SCORING_QUERY_SEED):
    """{shape: [{shape, query, hits, params}]} over the recorded corpus."""
    rng = random.Random(seed)
    docs, terms = compute_scoring_corpus()
    tiers = _tiers(terms)
    incidence = _field_incidence(docs, terms)

    # Terms per doc, so an AND leg drawn from one doc always matches that doc.
    doc_terms = {doc: [] for doc in docs}
    for term, posting in terms.items():
        for doc in posting:
            doc_terms[doc].append(term)
    for terms_in_doc in doc_terms.values():
        terms_in_doc.sort()

    tag_docs = {value: {doc for doc, f in docs.items() if f["t1"] == value}
                for value in SCORING_TAG_FREQS}

    shapes = {}

    def emit(shape, query, hits, params=()):
        shapes.setdefault(shape, []).append(
            {"shape": shape, "query": query, "hits": set(hits),
             "params": tuple(params)})

    def draw_from_doc(count):
        """`count` distinct terms sharing a document, plus that document."""
        while True:
            doc = rng.randrange(SCORING_NUM_DOCS)
            if len(doc_terms[doc]) >= count:
                return doc, rng.sample(doc_terms[doc], count)

    def draw_across_tiers(count):
        """One term from each of `count` distinct document-frequency tiers."""
        return [rng.choice(tier) for tier in rng.sample(tiers, count)]

    def hits_of(*drawn):
        return set.intersection(*(set(terms[t]) for t in drawn))

    def union_of(*drawn):
        return set.union(*(set(terms[t]) for t in drawn))

    # Single term, every tier. `dt` is the hit count by construction.
    for term in sorted(terms):
        emit("single_term", term, terms[term])

    for count, shape in ((2, "and2"), (3, "and3")):
        for _ in range(SHAPE_COUNTS[shape]):
            _, drawn = draw_from_doc(count)
            emit(shape, " ".join(drawn), hits_of(*drawn))

    for count, shape in ((2, "or2"), (3, "or3")):
        for _ in range(SHAPE_COUNTS[shape]):
            drawn = draw_across_tiers(count)
            emit(shape, " | ".join(drawn), union_of(*drawn))

    for i in range(SHAPE_COUNTS["mixed"]):
        if i % 2:
            # AND leg from one doc, OR leg free.
            _, (a, b) = draw_from_doc(2)
            c = rng.choice(rng.choice(tiers))
            emit("mixed", f"({a} {b}) | {c}", hits_of(a, b) | set(terms[c]))
        else:
            # All three legs from one doc, so the AND cannot be empty.
            _, (a, c, d) = draw_from_doc(3)
            emit("mixed", f"{a} ({c} | {d})",
                 set(terms[a]) & union_of(c, d))

    # A term in both fields of some doc, so each field spelling subsets the bare one.
    both_fields = sorted(t for t in terms
                         if incidence["title"][t] & incidence["body"][t])
    for term in rng.sample(both_fields, SHAPE_COUNTS["cross_field"]):
        emit("cross_field", f"@title:{term}", incidence["title"][term])
        emit("cross_field", f"@body:{term}", incidence["body"][term])
        emit("cross_field", f"(@title:{term} | @body:{term})", terms[term])

    for i in range(SHAPE_COUNTS["leaf_weight"]):
        w = WEIGHTS[i % len(WEIGHTS)]
        if i % 5 == 4:
            value = sorted(SCORING_TAG_FREQS)[i % len(SCORING_TAG_FREQS)]
            emit("leaf_weight", _weight(f"(@t1:{{{value}}})", w), tag_docs[value])
        else:
            term = rng.choice(rng.choice(tiers))
            emit("leaf_weight", _weight(f"({term})", w), terms[term])

    for i in range(SHAPE_COUNTS["nested_weight"]):
        inner_a, inner_b, outer = (WEIGHTS[i % len(WEIGHTS)],
                                   WEIGHTS[(i + 1) % len(WEIGHTS)],
                                   WEIGHTS[(i + 2) % len(WEIGHTS)])
        _, (a, b) = draw_from_doc(2)
        joiner, hits = (" ", hits_of(a, b)) if i % 2 else (" | ", union_of(a, b))
        legs = joiner.join([_weight(f"({a})", inner_a), _weight(f"({b})", inner_b)])
        emit("nested_weight", _weight(f"({legs})", outer), hits)

    for value in sorted(SCORING_TAG_FREQS):
        emit("tag_only", f"@t1:{{{value}}}", tag_docs[value])

    # n1 == doc id, so the hit set is the range; a numeric leaf scores 0 on every row.
    for low, high in ((0, 9), (10, 19), (45, 55), (50, 50), (90, 99), (0, 99)):
        emit("numeric_only", f"@n1:[{low} {high}]", range(low, high + 1))

    for _ in range(SHAPE_COUNTS["text_numeric_tag"]):
        doc, (term,) = draw_from_doc(1)
        low, high = max(0, doc - 5), min(SCORING_NUM_DOCS - 1, doc + 5)
        value = docs[doc]["t1"]
        emit("text_numeric_tag",
             f"{term} @n1:[{low} {high}] @t1:{{{value}}}",
             set(terms[term]) & set(range(low, high + 1)) & tag_docs[value])

    emit("match_all", "*", docs)

    # Hybrid scores are the text score alone, so this shape covers KNN eviction only.
    rare, common = tiers[3], tiers[1]
    for cluster in range(SCORING_VECTOR_CLUSTERS):
        blob = struct.pack(f"<{VECTOR_DIM}f", *([float(cluster)] * VECTOR_DIM))
        params = ("PARAMS", "2", "q", blob)
        # k past the filter set: every text match survives, so hits == dt.
        term = rare[cluster % len(rare)]
        emit("text_vector", f"{term}=>[KNN {VECTOR_K_FULL} @v1 $q]",
             terms[term], params)
        # k inside the filter set: only the k nearest survive.
        term = common[cluster % len(common)]
        nearest = sorted(terms[term],
                         key=lambda d: abs(docs[d]["v1"][0] - cluster))
        emit("text_vector", f"{term}=>[KNN {VECTOR_K_TRUNCATED} @v1 $q]",
             nearest[:VECTOR_K_TRUNCATED], params)

    return shapes


def search_args(index, descriptor):
    """The full FT.SEARCH argument list for one descriptor."""
    # SCORER is explicit (RediSearch defaults to TFIDF); NOCONTENT drops the doc bodies.
    return ["FT.SEARCH", index, descriptor["query"],
            "SCORER", "BM25STD", "WITHSCORES", "NOCONTENT",
            "LIMIT", "0", str(SEARCH_LIMIT),
            *descriptor["params"], "DIALECT", "2"]
