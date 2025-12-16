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
# CONFIGURATION
# =========================

@dataclass
class QueryGenerationConfig:
    """Configuration for controlling query generation features."""
    
    # Term types to include
    allow_exact_match: bool = True      # Regular word terms
    allow_prefix: bool = True           # word*
    allow_suffix: bool = True           # *word
    allow_phrase: bool = True           # "word1 word2"
    allow_tag: bool = True              # @field:{value}
    allow_numeric: bool = True          # @field:[min max]
    
    # Operators
    allow_and: bool = True              # Multiple terms in same clause
    allow_or: bool = True               # Multiple clauses (|)
    allow_not: bool = False             # -term (negation)
    allow_optional: bool = False        # ~term (optional)
    
    # Field matching
    allow_field_match: bool = True      # @field:term
    force_field_match: bool = False     # Always use @field:term
    
    # Grouping
    allow_groups: bool = True           # (...)
    max_depth: int = 2                  # Max nesting depth
    
    # Phrase options
    max_phrase_words: int = 3
    allow_phrase_slop: bool = True
    allow_phrase_inorder: bool = True
    force_phrase_inorder: bool = False
    force_phrase_slop: bool = False
    
    # Query complexity
    max_terms: int = 3
    min_terms: int = 1
    
    # Probabilities (0.0 to 1.0)
    prob_add_and_term: float = 0.5      # Probability of adding another AND term
    prob_add_or_clause: float = 0.3     # Probability of adding another OR clause
    prob_use_group: float = 0.3         # Probability of creating a group
    prob_use_field: float = 0.5         # Probability of specifying field


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
                 numeric_ranges: dict,
                 config: Optional[QueryGenerationConfig] = None):
        """
        Initialize TextQueryBuilder with schema configuration.
        
        Args:
            vocab: List of words to use in queries
            text_fields: List of TEXT field names
            tag_values: Dict of {field: [values]} for TAG fields
            numeric_ranges: Dict of {field: (min, max)} for NUMERIC fields
            config: Configuration for controlling query generation
        """
        self.vocab = vocab
        self.text_fields = text_fields
        self.tag_values = tag_values
        self.numeric_ranges = numeric_ranges
        self.config = config or QueryGenerationConfig()
    
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
            
            if base.inorder or base.slop is None:
                parts.append("INORDER")
            
            return " ".join(parts)
        
        raise TypeError(f"Unknown BaseTerm type: {type(base)}")
    
    def _render_primary(self, primary: Primary) -> str:
        if isinstance(primary, Term):
            base_str = self._render_base_term(primary.base)
            if isinstance(primary.base, (WordTerm, PrefixTerm, SuffixTerm, PhraseTerm)):
                if primary.field is not None:
                    return f"@{primary.field}:{base_str}"
            elif isinstance(primary.base, (TagTerm, NumericTerm)):
                return f"({base_str})"
            return base_str
        elif isinstance(primary, Group):
            return f"({self._render_expression(primary.expr)})"
        else:
            raise TypeError(f"Unknown Primary type: {type(primary)}")
    
    def _render_unary(self, unary: UnaryClause) -> str:
        return f"{unary.op}{self._render_primary(unary.primary)}".strip()
    
    def _render_clause(self, clause: Clause) -> str:
        """Render a clause, reordering to avoid parentheses at first position."""
        if not clause.parts:
            return ""
        
        # Separate terms that will be wrapped vs not wrapped
        wrapped_terms = []
        unwrapped_terms = []
        
        for unary in clause.parts:
            will_be_wrapped = self._will_term_be_wrapped(unary.primary)
            if will_be_wrapped:
                wrapped_terms.append(unary)
            else:
                unwrapped_terms.append(unary)
        
        # Reorder: unwrapped terms first, then wrapped terms
        reordered = unwrapped_terms + wrapped_terms
        
        return " ".join(self._render_unary(u) for u in reordered)

    def _will_term_be_wrapped(self, primary: Primary) -> bool:
        """Check if a primary will be wrapped in parentheses when rendered."""
        if isinstance(primary, Group):
            return True
        
        if isinstance(primary, Term):
            if isinstance(primary.base, (WordTerm, PrefixTerm, SuffixTerm, PhraseTerm)):
                if primary.field is not None:
                    return True
            elif isinstance(primary.base, (TagTerm, NumericTerm)):
                return True
        
        return False
    
    def _render_expression(self, expr: Expression) -> str:
        return " | ".join(self._render_clause(c) for c in expr.clauses)
    
    # =========================
    # TERM GENERATION
    # =========================
    
    def _get_allowed_term_types(self) -> List[str]:
        """Get list of allowed term types based on config."""
        types = []
        if self.config.allow_exact_match:
            types.append("word")
        if self.config.allow_prefix:
            types.append("prefix")
        if self.config.allow_suffix:
            types.append("suffix")
        if self.config.allow_phrase:
            types.append("phrase")
        if self.config.allow_tag and self.tag_values:
            types.append("tag")
        if self.config.allow_numeric and self.numeric_ranges:
            types.append("numeric")
        
        if not types:
            raise ValueError("At least one term type must be allowed")
        
        return types
    
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
    
    def _generate_phrase_term(self) -> PhraseTerm:
        """Generate phrase term respecting config."""
        length = random.randint(1, self.config.max_phrase_words)
        if length <= len(self.vocab):
            words = random.sample(self.vocab, length)
        else:
            words = [random.choice(self.vocab) for _ in range(length)]

        # Handle slop
        slop = None
        if self.config.force_phrase_slop:
            slop = random.choice([1, 2])
        elif self.config.allow_phrase_slop and random.random() < 0.3:
            slop = random.choice([1, 2])
        
        # Handle inorder
        inorder = False
        if self.config.force_phrase_inorder:
            inorder = True
        elif self.config.allow_phrase_inorder and random.random() < 0.3:
            inorder = True
        
        return PhraseTerm(words=words, slop=slop, inorder=inorder)
    
    def _generate_base_term(self) -> BaseTerm:
        """Generate a base term respecting config constraints."""
        allowed_types = self._get_allowed_term_types()
        kind = random.choice(allowed_types)
        
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
            return self._generate_phrase_term()
        
        return self._generate_word_term()
    
    def _generate_term(self, budget: TermBudget) -> Term:
        """Generate term respecting field matching config."""
        if not budget.can_add_term():
            raise RuntimeError("No term budget left to generate a Term")
        budget.consume_term()
        
        base = self._generate_base_term()
        
        # Determine if we should use field matching
        field = None
        if isinstance(base, (WordTerm, PrefixTerm, SuffixTerm, PhraseTerm)):
            if self.config.force_field_match:
                field = random.choice(self.text_fields)
            elif self.config.allow_field_match and random.random() < self.config.prob_use_field:
                field = random.choice(self.text_fields)
        
        return Term(field=field, base=base)
    
    def _generate_primary(self, budget: TermBudget, max_depth: int) -> Primary:
        """Generate primary respecting grouping config."""
        if not budget.can_add_term():
            raise RuntimeError("No term budget left to generate a Primary")
        
        can_group = (self.config.allow_groups and 
                     budget.remaining >= 2 and 
                     max_depth > 0)
        
        if can_group and random.random() < self.config.prob_use_group:
            expr = self._generate_expression(budget, max_depth - 1)
            return Group(expr=expr)
        else:
            return self._generate_term(budget)
    
    def _generate_unary_clause(self, budget: TermBudget, max_depth: int) -> UnaryClause:
        """Generate unary clause with optional NOT/OPTIONAL operators."""
        primary = self._generate_primary(budget, max_depth)
        
        # Determine operator
        op = ""
        if self.config.allow_not and random.random() < 0.2:
            op = "-"
        elif self.config.allow_optional and random.random() < 0.2:
            op = "~"
        
        return UnaryClause(op=op, primary=primary)
    
    def _generate_clause(self, budget: TermBudget, max_depth: int) -> Clause:
        """Generate clause respecting AND config."""
        parts: List[UnaryClause] = []
        parts.append(self._generate_unary_clause(budget, max_depth))
        
        if self.config.allow_and:
            while budget.can_add_term() and random.random() < self.config.prob_add_and_term:
                parts.append(self._generate_unary_clause(budget, max_depth))
        
        return Clause(parts=parts)
    
    def _generate_expression(self, budget: TermBudget, max_depth: int) -> Expression:
        """Generate expression respecting OR config."""
        clauses: List[Clause] = []
        clauses.append(self._generate_clause(budget, max_depth))
        
        if self.config.allow_or:
            while budget.can_add_term() and random.random() < self.config.prob_add_or_clause:
                clauses.append(self._generate_clause(budget, max_depth))
        
        return Expression(clauses=clauses)
    
    # =========================
    # PUBLIC API
    # =========================
    
    def generate(self, seed: Optional[int] = None) -> Query:
        """
        Generate a random Query AST using config settings.
        
        Args:
            seed: Random seed for reproducibility
            
        Returns:
            Query object
        """
        if self.config.max_terms <= 0:
            raise ValueError("max_terms must be >= 1")
        
        if seed is not None:
            random.seed(seed)
        
        budget = TermBudget(max_terms=self.config.max_terms)
        expr = self._generate_expression(budget, self.config.max_depth)
        query = Query(expr=expr)
        
        # Sanity checks
        total_terms = self.count_terms(query)
        assert self.config.min_terms <= total_terms <= self.config.max_terms, \
            f"Term count {total_terms} out of bounds [{self.config.min_terms}, {self.config.max_terms}]"
        self._check_phrase_lengths(query)
        
        return query
    
    def _check_phrase_lengths(self, query: Query) -> None:
        """Verify that all PhraseTerm nodes obey the max_phrase_words constraint."""
        def visit_primary(primary: Primary):
            if isinstance(primary, Term):
                if isinstance(primary.base, PhraseTerm):
                    if len(primary.base.words) > self.config.max_phrase_words:
                        raise AssertionError(
                            f"Phrase with {len(primary.base.words)} words exceeds max {self.config.max_phrase_words}"
                        )
            elif isinstance(primary, Group):
                for clause in primary.expr.clauses:
                    for unary in clause.parts:
                        visit_primary(unary.primary)
        
        for clause in query.expr.clauses:
            for unary in clause.parts:
                visit_primary(unary.primary)
