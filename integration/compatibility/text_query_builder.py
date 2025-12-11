from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Union
import random


# =========================
# AST NODE DEFINITIONS
# =========================

class BaseTerm:
    """Abstract base for term payloads."""
    pass


@dataclass
class WordTerm(BaseTerm):
    value: str


@dataclass
class PrefixTerm(BaseTerm):
    prefix: str


@dataclass
class SuffixTerm(BaseTerm):
    suffix: str


@dataclass
class TagTerm(BaseTerm):
    tag_expr: str


@dataclass
class NumericTerm(BaseTerm):
    min_value: float
    max_value: float
    field: str = "price"


@dataclass
class PhraseTerm(BaseTerm):
    words: List[str]
    slop: Optional[int] = None
    inorder: bool = False


@dataclass
class Term:
    field: Optional[str]
    base: BaseTerm


@dataclass
class Group:
    expr: "Expression"


Primary = Union[Term, Group]


@dataclass
class UnaryClause:
    op: str
    primary: Primary


@dataclass
class Clause:
    parts: List[UnaryClause]


@dataclass
class Expression:
    clauses: List[Clause]


@dataclass
class Query:
    expr: Expression


# =========================
# TERM BUDGET TRACKER
# =========================

class TermBudget:
    """Tracks remaining term slots during query generation."""
    
    def __init__(self, max_terms: int):
        self.max_terms = max_terms
        self.used = 0

    @property
    def remaining(self) -> int:
        return self.max_terms - self.used

    def can_add_term(self) -> bool:
        return self.remaining > 0

    def consume_term(self) -> None:
        if not self.can_add_term():
            raise RuntimeError("Term budget exceeded")
        self.used += 1


# =========================
# QUERY BUILDER CLASS
# =========================

class TextQueryBuilder:
    """
    Main class for building, rendering, and generating RediSearch queries.
    
    Query Structure:
     └── Expression (OR level)
          ├── Clause (AND level)
          │    ├── UnaryClause
          │    │     └── Primary
          │    │           ├── Term (with BaseTerm variants)
          │    │           └── Group
          │    └── ...
          └── ...
    """
    
    def __init__(self,
                 vocab: List[str],
                 text_fields: List[str],
                 tag_values: dict,
                 numeric_ranges: dict):
        """
        Initialize TextQueryBuilder with schema configuration.
        
        Args:
            vocab: List of words to use in queries
            text_fields: List of TEXT field names
            tag_values: Dict of {field: [values]} for TAG fields
            numeric_ranges: Dict of {field: (min, max)} for NUMERIC fields
        """
        self.vocab = vocab
        self.text_fields = text_fields
        self.tag_values = tag_values
        self.numeric_ranges = numeric_ranges
    
    # =========================
    # TERM COUNTING
    # =========================
    
    def count_terms(self, query: Query) -> int:
        """Count total number of terms in a query."""
        return self._count_terms_expr(query.expr)
    
    def _count_terms_expr(self, expr: Expression) -> int:
        return sum(self._count_terms_clause(c) for c in expr.clauses)
    
    def _count_terms_clause(self, clause: Clause) -> int:
        return sum(self._count_terms_unary(u) for u in clause.parts)
    
    def _count_terms_unary(self, unary: UnaryClause) -> int:
        return self._count_terms_primary(unary.primary)
    
    def _count_terms_primary(self, primary: Primary) -> int:
        if isinstance(primary, Term):
            return 1
        elif isinstance(primary, Group):
            return self._count_terms_expr(primary.expr)
        return 0
    
    # =========================
    # RENDERING
    # =========================
    
    def render(self, query: Query) -> str:
        """Convert Query AST to query string."""
        return self._render_expression(query.expr)
    
    def _render_base_term(self, base: BaseTerm) -> str:
        if isinstance(base, WordTerm):
            return base.value
        
        if isinstance(base, PrefixTerm):
            return base.prefix
        
        if isinstance(base, SuffixTerm):
            return base.suffix
        
        if isinstance(base, TagTerm):
            return base.tag_expr
        
        if isinstance(base, NumericTerm):
            return f"@{base.field}:[{base.min_value} {base.max_value}]"
        
        if isinstance(base, PhraseTerm):
            phrase = " ".join(base.words)
            parts = [f"\"{phrase}\""]
            
            if base.slop is not None:
                parts.append(f"SLOP {base.slop}")
            
            if base.inorder:
                parts.append("INORDER")
            
            return " ".join(parts)
        
        raise TypeError(f"Unknown BaseTerm type: {type(base)}")
    
    def _render_primary(self, primary: Primary) -> str:
        if isinstance(primary, Term):
            base_str = self._render_base_term(primary.base)
            if isinstance(primary.base, (WordTerm, PrefixTerm, SuffixTerm, PhraseTerm)):
                if primary.field is not None:
                    return f"@{primary.field}:{base_str}"
            return base_str
        elif isinstance(primary, Group):
            return f"({self._render_expression(primary.expr)})"
        else:
            raise TypeError(f"Unknown Primary type: {type(primary)}")
    
    def _render_unary(self, unary: UnaryClause) -> str:
        return f"{unary.op}{self._render_primary(unary.primary)}".strip()
    
    def _render_clause(self, clause: Clause) -> str:
        return " ".join(self._render_unary(u) for u in clause.parts)
    
    def _render_expression(self, expr: Expression) -> str:
        return " | ".join(self._render_clause(c) for c in expr.clauses)
    
    # =========================
    # TERM GENERATION
    # =========================
    
    def _generate_word_term(self) -> WordTerm:
        return WordTerm(value=random.choice(self.vocab))
    
    def _generate_prefix_term(self) -> PrefixTerm:
        word = random.choice(self.vocab)
        prefix = word[:3] + "*"
        return PrefixTerm(prefix=prefix)
    
    def _generate_suffix_term(self) -> SuffixTerm:
        word = random.choice(self.vocab)
        suffix = "*" + word[-3:]
        return SuffixTerm(suffix=suffix)
    
    def _generate_tag_term(self) -> TagTerm:
        field = random.choice(list(self.tag_values.keys()))
        value = random.choice(self.tag_values[field])
        return TagTerm(tag_expr=f"@{field}:{{{value}}}")
    
    def _generate_numeric_term(self) -> NumericTerm:
        field = random.choice(list(self.numeric_ranges.keys()))
        min_val, max_val = self.numeric_ranges[field]
        lo = random.randint(min_val, max_val - 1)
        hi = random.randint(lo + 1, max_val)
        return NumericTerm(min_value=lo, max_value=hi, field=field)
    
    def _generate_phrase_term(self, max_phrase_words: int) -> PhraseTerm:
        length = random.randint(1, max_phrase_words)
        if length <= len(self.vocab):
            words = random.sample(self.vocab, length)
        else:
            words = [random.choice(self.vocab) for _ in range(length)]
        slop = random.choice([None, 1, 2])
        inorder = random.choice([False, True])
        return PhraseTerm(words=words, slop=slop, inorder=inorder)
    
    def _generate_base_term(self, max_phrase_words: int) -> BaseTerm:
        kind = random.choice(["word", "prefix", "suffix", "tag", "numeric", "phrase"])
        
        if kind == "word":
            return self._generate_word_term()
        if kind == "prefix":
            return self._generate_prefix_term()
        if kind == "suffix":
            return self._generate_suffix_term()
        if kind == "tag":
            return self._generate_tag_term()
        if kind == "numeric":
            return self._generate_numeric_term()
        if kind == "phrase":
            return self._generate_phrase_term(max_phrase_words)
        
        return self._generate_word_term()
    
    def _generate_term(self, budget: TermBudget, max_phrase_words: int) -> Term:
        if not budget.can_add_term():
            raise RuntimeError("No term budget left to generate a Term")
        budget.consume_term()
        
        base = self._generate_base_term(max_phrase_words)
        
        if isinstance(base, (WordTerm, PrefixTerm, SuffixTerm, PhraseTerm)):
            field = random.choice(self.text_fields + [None])
        else:
            field = None
        
        return Term(field=field, base=base)
    
    def _generate_primary(self, budget: TermBudget, max_phrase_words: int, max_depth: int) -> Primary:
        if not budget.can_add_term():
            raise RuntimeError("No term budget left to generate a Primary")
        
        can_group = (budget.remaining >= 2) and (max_depth > 0)
        if can_group and random.random() < 0.3:
            expr = self._generate_expression(budget, max_phrase_words, max_depth - 1)
            return Group(expr=expr)
        else:
            return self._generate_term(budget, max_phrase_words)
    
    def _generate_unary_clause(self, budget: TermBudget, max_phrase_words: int, max_depth: int) -> UnaryClause:
        primary = self._generate_primary(budget, max_phrase_words, max_depth)
        return UnaryClause(op="", primary=primary)
    
    def _generate_clause(self, budget: TermBudget, max_phrase_words: int, max_depth: int) -> Clause:
        parts: List[UnaryClause] = []
        parts.append(self._generate_unary_clause(budget, max_phrase_words, max_depth))
        
        while budget.can_add_term() and random.random() < 0.5:
            parts.append(self._generate_unary_clause(budget, max_phrase_words, max_depth))
        
        return Clause(parts=parts)
    
    def _generate_expression(self, budget: TermBudget, max_phrase_words: int, max_depth: int) -> Expression:
        clauses: List[Clause] = []
        clauses.append(self._generate_clause(budget, max_phrase_words, max_depth))
        
        while budget.can_add_term() and random.random() < 0.3:
            clauses.append(self._generate_clause(budget, max_phrase_words, max_depth))
        
        return Expression(clauses=clauses)
    
    # =========================
    # PUBLIC API
    # =========================
    
    def generate(self,
                 max_terms: int,
                 max_phrase_words: int = 3,
                 max_depth: int = 2,
                 seed: Optional[int] = None) -> Query:
        """
        Generate a random Query AST.
        
        Args:
            max_terms: Maximum number of Term nodes in the query
            max_phrase_words: Maximum number of words inside a PhraseTerm
            max_depth: Maximum group nesting depth
            seed: Random seed for reproducibility
            
        Returns:
            Query object
        """
        if max_terms <= 0:
            raise ValueError("max_terms must be >= 1")
        if max_phrase_words <= 0:
            raise ValueError("max_phrase_words must be >= 1")
        
        if seed is not None:
            random.seed(seed)
        
        budget = TermBudget(max_terms=max_terms)
        expr = self._generate_expression(budget, max_phrase_words, max_depth)
        query = Query(expr=expr)
        
        # Sanity checks
        total_terms = self.count_terms(query)
        assert 1 <= total_terms <= max_terms, f"Term count {total_terms} out of bounds"
        self._check_phrase_lengths(query, max_phrase_words)
        
        return query
    
    def _check_phrase_lengths(self, query: Query, max_phrase_words: int) -> None:
        """Verify that all PhraseTerm nodes obey the max_phrase_words constraint."""
        def visit_primary(primary: Primary):
            if isinstance(primary, Term):
                if isinstance(primary.base, PhraseTerm):
                    if len(primary.base.words) > max_phrase_words:
                        raise AssertionError(
                            f"Phrase with {len(primary.base.words)} words exceeds max {max_phrase_words}"
                        )
            elif isinstance(primary, Group):
                for clause in primary.expr.clauses:
                    for unary in clause.parts:
                        visit_primary(unary.primary)
        
        for clause in query.expr.clauses:
            for unary in clause.parts:
                visit_primary(unary.primary)
