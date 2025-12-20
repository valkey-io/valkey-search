from dataclasses import dataclass
from typing import List, Union
import random

class BaseTerm:
    pass

@dataclass(frozen=True)
class WordTerm(BaseTerm):
    value: str


@dataclass(frozen=True)
class PrefixTerm(BaseTerm):
    value: str   # e.g. "app*"


@dataclass(frozen=True)
class SuffixTerm(BaseTerm):
    value: str   # e.g. "*ple"


@dataclass(frozen=True)
class ExactPhraseTerm(BaseTerm):
    words: List[str]   # ["apple", "banana"]

# renders

class TermRenderer:
    def render(self, term: Union[BaseTerm, List[BaseTerm]]) -> str:
        if isinstance(term, list):
            return " ".join(self._render_single(t) for t in term)
        return self._render_single(term)
    
    def _render_single(self, term: BaseTerm) -> str:
        if isinstance(term, WordTerm):
            return term.value
        if isinstance(term, PrefixTerm):
            return term.value
        if isinstance(term, SuffixTerm):
            return term.value
        if isinstance(term, ExactPhraseTerm):
            return '"' + " ".join(term.words) + '"'
        raise TypeError(f"Unknown term type: {type(term)}")

renderer = TermRenderer()

def AND(*parts: Union[BaseTerm, str]) -> str:
    terms = [p for p in parts if isinstance(p, BaseTerm)]
    return renderer.render(terms)

def OR(left: str, right: str) -> str:
    return f"{left} | {right}"

def GROUP(expr: str) -> str:
    return f"({expr})"

def render_shape(shape, vocab, rng) -> str:
    if shape == "A":
        return renderer.render(gen_atom(vocab, rng))

    if isinstance(shape, tuple):
        op = shape[0]

        if op == "G":
            inner = render_shape(shape[1], vocab, rng)
            return f"({inner})"

        if op == "AND":
            left = render_shape(shape[1], vocab, rng)
            right = render_shape(shape[2], vocab, rng)
            return f"({left} {right})"   # 👈 ADD PARENS

        if op == "OR":
            left = render_shape(shape[1], vocab, rng)
            right = render_shape(shape[2], vocab, rng)
            return f"({left} | {right})" # 👈 ADD PARENS

    raise ValueError(f"Unknown shape: {shape}")

def sample_shape_upto(depth: int, rng: random.Random):
    """Generate a shape with depth <= given depth."""
    if depth == 0:
        return "A"

    # Optionally stop early
    if rng.random() < 0.3:
        return "A"

    op = rng.choice(["G", "AND", "OR"])

    if op == "G":
        return ("G", sample_shape_upto(depth - 1, rng))

    return (
        op,
        sample_shape_upto(depth - 1, rng),
        sample_shape_upto(depth - 1, rng),
    )

def sample_shape_exact(depth: int, rng: random.Random):
    if depth == 0:
        return "A"

    if depth == 1:
        op = rng.choice(["AND", "OR"])
    else:
        op = rng.choice(["G", "AND", "OR"])

    if op == "G":
        return ("G", sample_shape_exact(depth - 1, rng))

    if rng.random() < 0.5:
        left = sample_shape_exact(depth - 1, rng)
        right = sample_shape_upto(depth - 1, rng)
    else:
        left = sample_shape_upto(depth - 1, rng)
        right = sample_shape_exact(depth - 1, rng)

    return (op, left, right)

# generators

def gen_atom(vocab: List[str], rng: random.Random) -> WordTerm:
    return WordTerm(rng.choice(vocab))

def gen_word(vocab: List[str]) -> WordTerm:
    count = random.randint(1, 3)
    return [WordTerm(random.choice(vocab)) for _ in range(count)]

def gen_prefix(vocab: List[str]) -> PrefixTerm:
    count = random.randint(1, 2)
    result = []
    for _ in range(count):
        w = random.choice(vocab)
        n = min(len(w), 3)
        result.append(PrefixTerm(w[:n] + "*"))
    return result

def gen_suffix(vocab: List[str]) -> SuffixTerm:
    """Generate 1-2 suffix terms."""
    count = random.randint(1, 2)
    result = []
    for _ in range(count):
        w = random.choice(vocab)
        n = min(len(w), 3)
        result.append(SuffixTerm("*" + w[-n:]))
    return result

def gen_exact_phrase(vocab: List[str], length: int = 2) -> ExactPhraseTerm:
    """Generate one exact phrase with 2-3 words."""
    length = random.randint(2, 3)
    if length <= len(vocab):
        words = random.sample(vocab, length)
    else:
        words = [random.choice(vocab) for _ in range(length)]
    return ExactPhraseTerm(words)

def gen_group_query(vocab, rng: random.Random, depth: int) -> str:
    shape = sample_shape_exact(depth, rng)
    return render_shape(shape, vocab, rng)

def gen_depth1(vocab, rng):
    return gen_group_query(vocab, rng, depth=1)

def gen_depth2(vocab, rng):
    return gen_group_query(vocab, rng, depth=2)

def gen_depth3(vocab, rng):
    return gen_group_query(vocab, rng, depth=3)