from dataclasses import dataclass
from typing import List, Union, Literal
import random


# ============================================================================
# Term Types
# ============================================================================

class BaseTerm:
    """Base class for all query term types."""
    pass


@dataclass(frozen=True)
class WordTerm(BaseTerm):
    """Represents a single word in a query."""
    value: str


@dataclass(frozen=True)
class PrefixTerm(BaseTerm):
    """Represents a prefix wildcard term (e.g., 'app*')."""
    value: str


@dataclass(frozen=True)
class SuffixTerm(BaseTerm):
    """Represents a suffix wildcard term (e.g., '*ple')."""
    value: str


@dataclass(frozen=True)
class ExactPhraseTerm(BaseTerm):
    """Represents an exact phrase match with multiple words."""
    words: List[str]


# ============================================================================
# Term Renderer
# ============================================================================

class TermRenderer:
    """Converts term objects into query strings."""
    
    def render(self, term: Union[BaseTerm, List[BaseTerm]]) -> str:
        """Render a single term or list of terms into a query string."""
        if isinstance(term, list):
            return " ".join(self._render_single(t) for t in term)
        return self._render_single(term)
    
    def _render_single(self, term: BaseTerm) -> str:
        """Render a single term based on its type."""
        if isinstance(term, WordTerm):
            return term.value
        if isinstance(term, PrefixTerm):
            return term.value
        if isinstance(term, SuffixTerm):
            return term.value
        if isinstance(term, ExactPhraseTerm):
            return '"' + " ".join(term.words) + '"'
        raise TypeError(f"Unknown term type: {type(term)}")


# ============================================================================
# Shape Rendering (for complex queries)
# ============================================================================

def render_shape(
    shape, 
    vocab: List[str], 
    rng: random.Random, 
) -> str:
    """Recursively render a query shape into a query string."""
    if shape == "A":
        return TermRenderer().render(gen_atom(vocab, rng))
    if isinstance(shape, tuple):
        op = shape[0]
        match op:
            case "G":
                inner = render_shape(shape[1], vocab, rng)
                return f"({inner})"
            case "AND":
                left = render_shape(shape[1], vocab, rng)
                right = render_shape(shape[2], vocab, rng)
                return f"({left} {right})"
            case "OR":
                left = render_shape(shape[1], vocab, rng)
                right = render_shape(shape[2], vocab, rng)
                return f"({left} | {right})"
            case _:
                raise ValueError(f"Unknown shape operator: {op}")
    raise ValueError(f"Unknown shape: {shape}")


# ============================================================================
# Shape Generators
# ============================================================================
Mode = Literal["exact", "upto"]

OPS_BINARY = ["AND", "OR"]
OPS_UNARY = ["G"]

def sample_shape(depth: int, rng: random.Random):
    """Generate a valid query shape with exact depth."""
    if depth == 0:
        return "A"
    
    # At depth=1, grouping is meaningless, so only use binary operators
    if depth == 1:
        op = rng.choice(OPS_BINARY)
    else:
        op = rng.choice(OPS_BINARY + OPS_UNARY)
    
    if op == "G":
        # Unary group does NOT reduce structural diversity
        return ("G", sample_shape(depth - 1, rng))
    
    # Binary operators: randomly choose which side reaches full depth
    if rng.random() < 0.5:
        left = sample_shape(depth - 1, rng)
        right = sample_shape(rng.randint(0, depth - 1), rng)
    else:
        left = sample_shape(rng.randint(0, depth - 1), rng)
        right = sample_shape(depth - 1, rng)
    
    return (op, left, right)


# ============================================================================
# Term Generators
# ============================================================================

def gen_atom(vocab: List[str], rng: random.Random) -> WordTerm:
    """Generate a single word term (used internally by shape rendering)."""
    return WordTerm(rng.choice(vocab))


def gen_word(vocab: List[str], rng: random.Random) -> List[WordTerm]:
    """Generate 1-3 word terms."""
    count = rng.randint(1, 3)
    return [WordTerm(rng.choice(vocab)) for _ in range(count)]


def gen_prefix(vocab: List[str], rng: random.Random) -> List[PrefixTerm]:
    """Generate 1-2 prefix terms."""
    count = rng.randint(1, 2)
    result = []
    for _ in range(count):
        word = rng.choice(vocab)
        prefix_len = min(len(word), 3)
        result.append(PrefixTerm(word[:prefix_len] + "*"))
    return result


def gen_suffix(vocab: List[str], rng: random.Random) -> List[SuffixTerm]:
    """Generate 1-2 suffix terms."""
    count = rng.randint(1, 2)
    result = []
    for _ in range(count):
        word = rng.choice(vocab)
        suffix_len = min(len(word), 3)
        result.append(SuffixTerm("*" + word[-suffix_len:]))
    return result


def gen_exact_phrase(vocab: List[str], rng: random.Random) -> ExactPhraseTerm:
    """Generate one exact phrase with 2-3 words."""
    length = rng.randint(2, 3)
    if length <= len(vocab):
        words = rng.sample(vocab, length)
    else:
        words = [rng.choice(vocab) for _ in range(length)]
    return ExactPhraseTerm(words)


# ============================================================================
# Complex Query Generators
# ============================================================================

def gen_depth1(vocab: List[str], rng: random.Random) -> str:
    """Generate a depth-1 grouped query."""
    shape = sample_shape(1, rng)
    return render_shape(shape, vocab, rng)


def gen_depth2(vocab: List[str], rng: random.Random) -> str:
    """Generate a depth-2 grouped query."""
    shape = sample_shape(2, rng)
    return render_shape(shape, vocab, rng)


def gen_depth3(vocab: List[str], rng: random.Random) -> str:
    """Generate a depth-3 grouped query."""
    shape = sample_shape(3, rng)
    return render_shape(shape, vocab, rng)