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


@dataclass(frozen=True)
class UnescapedTerm(BaseTerm):
    """Represents an uescaped term with puntuation."""
    value: str


@dataclass(frozen=True)
class EscapedTerm(BaseTerm):
    """Represents an escaped term with punctuation."""
    value: str


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
        if isinstance(term, UnescapedTerm):
            return term.value
        if isinstance(term, EscapedTerm):
            return term.value
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


def gen_atom(vocab: List[str], rng: random.Random) -> WordTerm:
    """Generate a single word term (used internally by shape rendering)."""
    return WordTerm(rng.choice(vocab))


def gen_depth2(vocab: List[str], rng: random.Random) -> str:
    """Generate a depth-2 grouped query."""
    shape = sample_shape(2, rng)
    return render_shape(shape, vocab, rng)


def gen_depth3(vocab: List[str], rng: random.Random) -> str:
    """Generate a depth-3 grouped query."""
    shape = sample_shape(3, rng)
    return render_shape(shape, vocab, rng)


def gen_unescaped_word(vocab: List[str], rng: random.Random) -> List[str]:
    count = rng.randint(1, 3)
    return [UnescapedTerm(rng.choice(vocab)) for _ in range(count)]


def gen_escaped_word(vocab: List[str], rng: random.Random) -> List[str]:
    count = rng.randint(1, 3)

    # add extra pair of backslashes to make query work in Valkey
    result = []
    for _ in range(count):
        word = rng.choice(vocab)
        # Double the backslash: \, -> \\, to match server-side escaping
        query = word.replace('\\', '\\\\')
        result.append(EscapedTerm(query))
    return result

    # return [EscapedTerm(rng.choice(vocab)) for _ in range(count)]


def effective_levenshtein_distance(term: str, requested: int) -> int:
    n = len(term)
    if n <= 2:
        return 0
    if n == 3:
        return min(requested, 1)
    return min(requested, n // 2)


def apply_levenshtein_transform(
    word: str,
    distance: int,
    rng: random.Random,
) -> str:
    chars = list(word)
    alphabet = list('abcdefghijklmnopqrstuvwxyz')
    for _ in range(distance):
        # If empty, only insertion is possible
        if not chars:
            chars.append(rng.choice(alphabet))
            continue

        op = rng.choice(("substitute", "insert", "delete"))

        if op == "substitute":
            i = rng.randrange(len(chars))
            original = chars[i]
            chars[i] = rng.choice([c for c in alphabet if c != original])

        elif op == "insert":
            i = rng.randrange(len(chars) + 1)
            chars.insert(i, rng.choice(alphabet))

        else:  # delete
            i = rng.randrange(len(chars))
            chars.pop(i)

    return "".join(chars)


def gen_fuzzy_1(vocab: List[str], rng: random.Random) -> str:
    """Generate a fuzzy term with Levenshtein distance 1: %word%"""
    word = rng.choice(vocab)
    eff_dist = effective_levenshtein_distance(word, 1)
    transformed = apply_levenshtein_transform(word, eff_dist, rng)
    return f"%{transformed}%"


def gen_fuzzy_2(vocab: List[str], rng: random.Random) -> str:
    """Generate a fuzzy term with Levenshtein distance 2: %%word%%"""
    word = rng.choice(vocab)
    eff_dist = effective_levenshtein_distance(word, 2)
    transformed = apply_levenshtein_transform(word, eff_dist, rng)
    return f"%%{transformed}%%"


def gen_fuzzy_3(vocab: List[str], rng: random.Random) -> str:
    """Generate a fuzzy term with Levenshtein distance 3: %%%word%%%"""
    word = rng.choice(vocab)
    eff_dist = effective_levenshtein_distance(word, 3)
    transformed = apply_levenshtein_transform(word, eff_dist, rng)
    return f"%%%{transformed}%%%"
