import pytest
import random
import re
import os
import traceback
from . import data_sets
from .data_sets import load_data
from .generate import BaseCompatibilityTest
from .text_query_builder import *

# exclude some edge cases with known Redis bugs
excluded_queries = set([
    # HASH
    # test_text_search_group_depth2_inorder[hash-2-nostem]
    "((dog | eagle) eagle)",
    "((drive | potato) (jump | fly))",

    # test_text_search_group_depth3_inorder[hash-2-nostem]
    "(slow ((quiet eagle) | (cat build)))",
    "(((slow | fast) (city | lettuce)))",

    # test_text_search_group_depth2_slop[hash-2-nostem]
    "((loud | fly) (loud | ocean))",

    # test_text_search_group_depth3_slop[hash-2-nostem]
    "(((city | tomato) warm))",
    "(((sharp | silent) (movie | silent)))",
    "(((melon | window) (puzzle | bright)))",

    # test_text_search_group_depth2_inorder_slop[hash-2-nostem]
    "((kiwi | game) (banana | window))",
    "((dog | eagle) eagle)",
    "(((sharp | silent) (movie | silent)))",

    # JSON
    # test_text_search_group_depth2_inorder[json-2-nostem]
    "((lemon | peach) (bright | banana))",
    "((dog | eagle) eagle)",
    "((shark | cold) (cold | river))",

    # test_text_search_group_depth3_inorder[json-2-nostem]
    "(((shark | cold) (river | cold)))",
    
    # test_text_search_group_depth2_slop[json-2-nostem]
    "((swim | river) (tiger | swim))",
    "((shark | tiger) (slow | tiger))",
    "(quick (quick | silent))",

    # test_text_search_group_depth3_slop[json-2-nostem]
    "(((tiger | slow) (tiger | swim)))",
    "((shark | tiger) (slow | tiger))",
    "(((shark | cold) (river | cold)))",
    "(((tomato | tiger) (river | tomato)))",
    "(((desert warm) | (village onion)) (warm | fly))",
    "(((onion | drive) (drive | dog)))",
    "(((book | apple) (book | lemon)))",
    "((game | apple) ((banana | movie)))",
    "(bright ((silent silent) | (peach mango)))",

    # test_text_search_group_depth2_inorder_slop[json-2-nostem]
    "((shark | tiger) (slow | tiger))",
    "((dog | eagle) eagle)",
    
    # test_text_search_group_depth3_inorder_slop[json-2-nostem]
    "((shark | tiger) ((slow | tiger)))",
    "(((shark | cold) (river | cold)))",
    "((plum | heavy) ((music | desk)))",

    # test_text_search_unescaped[hash-2-nostem]
    "minus-subtract",
    "minus-subtract left&right",

    # test_text_search_unescaped[json-nostem]
    'many"few many"few',
])

@pytest.mark.parametrize("schema_type", ["default", "nostem"])
@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["hash", "json"])
class TestTextSearchCompatibility(BaseCompatibilityTest):
    TEXT_QUERY_TEST_SEED = 3948
    MAX_QUERIES = 1000
    ANSWER_FILE_NAME = "text-search-answers.pickle.gz"

    def setup_data(self, data_set_name, key_type, schema_type):
        """Override to specify text data source."""        
        self.data_set_name = data_set_name
        self.key_type = key_type
        self.schema_type = schema_type
        self.client.execute_command("FLUSHALL SYNC")
        load_data(self.client, data_set_name, key_type, data_source='text', schema_type=schema_type)
    
    def execute_command(self, cmd):
        """Override to include schema_type in answer."""
        answer = {"cmd": cmd,
                "key_type": self.key_type,
                "data_set_name": self.data_set_name,
                "schema_type": self.schema_type,
                "testname": os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0],
                "traceback": "".join(traceback.format_stack())}
        try:
            print("Cmd:", *cmd)
            answer["result"] = self.client.execute_command(*cmd)
            answer["exception"] = False
            if answer["result"] != [0]:
                self.__class__.replied_count += 1
            print(f"replied: {answer['result']} (count: {self.__class__.replied_count})")
        except Exception as exc:
            print(f"Got exception for Error: '{exc}', Cmd:{cmd}")
            answer["result"] = {}
            answer["exception"] = True
        self.answers.append(answer)

    def parse_explaincli_to_query(self, index_name: str, query: str, dialect: int = 2) -> tuple[str, bool]:
        """
        Parse the result of Redis FT.EXPLAINCLI into query
        Check and filter out queries with different parsing
        """
        lines = self._decode_explaincli_result(index_name, query, dialect)
        stack = self._parse_tree_from_lines(lines)
        reconstructed = self._reconstruct_query(stack)
        original_normalized = self._normalize_query(query)
        reconstructed_normalized = self._normalize_query(reconstructed)
        return reconstructed, original_normalized == reconstructed_normalized

    def _decode_explaincli_result(self, index_name: str, query: str, dialect: int) -> list[str]:
        """Execute FT.EXPLAINCLI and return decoded lines."""
        result = self.client.execute_command("FT.EXPLAINCLI", index_name, query, "DIALECT", str(dialect))
        if isinstance(result, bytes):
            result = result.decode('utf-8')
        elif isinstance(result, list):
            result = '\n'.join(r.decode('utf-8') if isinstance(r, bytes) else str(r) for r in result)
        return result.split('\n')

    def _parse_tree_from_lines(self, lines: list[str]) -> list:
        """Parse EXPLAINCLI output lines into a tree of dicts/strings."""
        stack = []
        for line in lines:
            cleaned = re.sub(r'^\d+\)\s*', '', line.strip())
            if not cleaned:
                continue
            if 'INTERSECT' in cleaned:
                stack.append({'type': 'AND', 'children': []})
            elif 'UNION' in cleaned:
                stack.append({'type': 'OR', 'children': []})
            elif cleaned == '{':
                continue
            elif cleaned == '}':
                if len(stack) > 1:
                    completed = stack.pop()
                    stack[-1]['children'].append(completed)
            elif cleaned.startswith('+') or '(expanded)' in cleaned:
                continue
            else:
                if re.match(r'^[a-zA-Z0-9_*-]+$', cleaned):
                    if stack:
                        stack[-1]['children'].append(cleaned)
                    else:
                        stack.append(cleaned)
        return stack

    @staticmethod
    def _reconstruct_query(stack: list) -> str:
        """Reconstruct a query string from a parsed tree."""
        def reconstruct(node):
            if isinstance(node, str):
                return node
            if isinstance(node, dict):
                children = [reconstruct(c) for c in node['children']]
                if not children:
                    return ''
                if node['type'] == 'OR':
                    children.sort()
                    joiner = ' | '
                else:
                    joiner = ' '
                result = joiner.join(children)
                return f'({result})' if len(children) > 1 else result
            return ''

        if not stack:
            return ''
        if len(stack) == 1:
            return reconstruct(stack[0])
        return ' '.join(reconstruct(node) for node in stack)

    @staticmethod
    def _normalize_query(q: str) -> str:
        """Normalize a query string so semantically equivalent queries produce
        the same string. OR operands are sorted since OR is commutative."""

        def tokenize(s):
            """Split query into tokens: '(', ')', '|', and words."""
            tokens = []
            i = 0
            s = s.strip()
            while i < len(s):
                if s[i] in '()':
                    tokens.append(s[i])
                    i += 1
                elif s[i] == '|':
                    tokens.append('|')
                    i += 1
                elif s[i].isspace():
                    i += 1
                else:
                    # Read a word token
                    j = i
                    while j < len(s) and s[j] not in '()|' and not s[j].isspace():
                        j += 1
                    tokens.append(s[j:j] if i == j else s[i:j])
                    i = j
            return tokens

        def parse_or(tokens, pos):
            """Parse an OR expression"""
            node, pos = parse_and(tokens, pos)
            children = [node]
            while pos < len(tokens) and tokens[pos] == '|':
                pos += 1  # skip '|'
                child, pos = parse_and(tokens, pos)
                children.append(child)
            if len(children) == 1:
                return children[0], pos
            children.sort()  # OR is commutative, sort for canonical form
            return ('OR', children), pos

        def parse_and(tokens, pos):
            """Parse an AND expression"""
            node, pos = parse_atom(tokens, pos)
            children = [node]
            while pos < len(tokens) and tokens[pos] not in ('|', ')'):
                child, pos = parse_atom(tokens, pos)
                children.append(child)
            if len(children) == 1:
                return children[0], pos
            return ('AND', children), pos

        def parse_atom(tokens, pos):
            """Parse a single term or a parenthesized sub-expression."""
            if tokens[pos] == '(':
                pos += 1  # skip '('
                node, pos = parse_or(tokens, pos)
                pos += 1  # skip ')'
                return node, pos
            word = tokens[pos]
            return word, pos + 1

        def tree_to_string(tree):
            """Convert a parse tree back to a normalized string."""
            if isinstance(tree, str):
                return tree
            op, children = tree
            child_strs = [tree_to_string(c) for c in children]
            if op == 'OR':
                return '(' + ' | '.join(child_strs) + ')'
            else:
                result = ' '.join(child_strs)
                return f'({result})' if len(child_strs) > 1 else result

        tokens = tokenize(q)
        if not tokens:
            return ''
        tree, _ = parse_or(tokens, 0)
        return tree_to_string(tree)

    def _is_redis_server(self) -> bool:
        """Check if the connected server is Redis (vs Valkey)."""
        try:
            info = self.client.execute_command("INFO", "SERVER")
            if isinstance(info, bytes):
                info = info.decode('utf-8')
            return "server_name:valkey" not in info
        except Exception:
            return False

    @staticmethod
    def _build_query(builder_fn, vocab, rng, renderer, query_str=None):
        """Generate a query string from builder_fn, or return query_str if provided."""
        if query_str:
            return query_str
        result = builder_fn(vocab, rng)
        return result if isinstance(result, str) else renderer.render(result)

    def _validate_parsing(self, key_type, query, dialect):
        """Check if Redis parses the query consistently via FT.EXPLAINCLI.
        Returns True if valid or skipped, False if mismatched."""
        reconstructed, is_valid = self.parse_explaincli_to_query(
            f"{key_type}_idx1", query, dialect
        )
        if not is_valid:
            print(f"Redis parsing is inconsistent for query:")
            print(f"  Original:      {query}")
            print(f"  Reconstructed: {reconstructed}")
        return is_valid

    @staticmethod
    def _build_search_args(key_type, query, dialect, inorder, slop, rng):
        """Build the FT.SEARCH command argument list."""
        args = ["FT.SEARCH", f"{key_type}_idx1", query]
        if inorder:
            args.append("INORDER")
        if slop:
            args.extend(["SLOP", str(rng.randint(1, 2))])
        args.extend(["DIALECT", str(dialect)])
        return args

    def _run_test(self, builder_fn, data_set_name, key_type, dialect, schema_type,
              inorder=False, slop=False, check_parsing=False, field=None, query_str=None, exclude_all=False):
        """Helper to run a test with given term builder function
        Args:
            builder_fn: Function that takes (vocab, rng) and returns term(s) or query string
            data_set_name: Name of the data set to test against
            key_type: Type of key ("json" or "hash")
           dialect: Query dialect version
        """
        is_redis = self._is_redis_server()
        matched, mismatched = 0, 0
        self.setup_data(data_set_name, key_type, schema_type)
        rng = random.Random(self.TEXT_QUERY_TEST_SEED)
        renderer = TermRenderer()
        vocab_by_field = data_sets.extract_vocab_by_field_from_text_data(data_set_name, key_type)

        seen = set()
        query_count = 0
        attempts = 0
        max_queries = 1 if query_str else self.MAX_QUERIES
        max_attempts = self.MAX_QUERIES * 20

        while query_count < max_queries and attempts < max_attempts:
            attempts += 1
            selected_field = field if field is not None else rng.choice(list(vocab_by_field.keys()))
            vocab = vocab_by_field[selected_field]

            try:
                current_query = self._build_query(builder_fn, vocab, rng, renderer, query_str)
                if current_query in seen:
                    continue
                seen.add(current_query)

                # if the query has known difference in Redis or with parsing difference
                # skip comparison in Redis and add to answer file with excluded flag
                # will run in valkey for no-crash check only
                is_excluded = current_query in excluded_queries or exclude_all
                if not is_excluded and is_redis and check_parsing:
                    if self._validate_parsing(key_type, current_query, dialect):
                        matched += 1
                    else:
                        mismatched += 1
                        is_excluded = True
                    print(f"Matched: {matched}, Mismatched: {mismatched}")

                if is_excluded:
                    print(f"Query excluded: {current_query}")
                    excluded_args = ["FT.SEARCH", f"{key_type}_idx1", current_query, "DIALECT", str(dialect)]
                    self.answers.append({
                        "cmd": excluded_args,
                        "key_type": self.key_type,
                        "data_set_name": self.data_set_name,
                        "schema_type": self.schema_type,
                        "testname": os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0],
                        "excluded": True,
                    })
                    query_count += 1
                    continue

                args = self._build_search_args(key_type, current_query, dialect, inorder, slop, rng)
                self.check(*args)
                query_count += 1

            except Exception as e:
                print(f"Error generating query: {e}")
                continue

        print(f"Generated {query_count} unique queries from {attempts} attempts")

    # ========================================================================
    # Base term types
    # ========================================================================

    def test_text_search_exact_match(self, key_type, dialect, schema_type):
        """Test exact word matching queries."""
        self._run_test(gen_word, "pure text", key_type, dialect, schema_type)

    def test_text_search_prefix(self, key_type, dialect, schema_type):
        """Test prefix wildcard queries."""
        self._run_test(gen_prefix, "pure text", key_type, dialect, schema_type)

    def test_text_search_suffix(self, key_type, dialect, schema_type):
        """Test suffix wildcard queries."""
        self._run_test(gen_suffix, "pure text", key_type, dialect, schema_type)

    # ========================================================================
    # Complex grouped queries
    # ========================================================================

    def test_text_search_group_depth2(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 2."""
        self._run_test(gen_depth2, "pure text", key_type, dialect, schema_type)

    def test_text_search_group_depth3(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 3."""
        self._run_test(gen_depth3, "pure text", key_type, dialect, schema_type)
    @pytest.mark.skip(reason="Not sure when these got broken")
    def test_text_search_group_depth2_inorder(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 2."""
        self._run_test(gen_depth2, "pure text", key_type, dialect, schema_type, inorder=True, check_parsing=True)

    @pytest.mark.skip(reason="Not sure when these got broken")
    def test_text_search_group_depth3_inorder(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 3."""
        self._run_test(gen_depth3, "pure text", key_type, dialect, schema_type, inorder=True, check_parsing=True)
    @pytest.mark.skip(reason="Not sure when these got broken")
    def test_text_search_group_depth2_slop(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 2."""
        self._run_test(gen_depth2, "pure text", key_type, dialect, schema_type, slop=True, check_parsing=True)

    @pytest.mark.skip(reason="Not sure when these got broken")
    def test_text_search_group_depth3_slop(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 3."""
        self._run_test(gen_depth3, "pure text", key_type, dialect, schema_type, slop=True, check_parsing=True)

    @pytest.mark.skip(reason="Not sure when these got broken")
    def test_text_search_group_depth2_inorder_slop(self, key_type, dialect, schema_type):
        self._run_test(gen_depth2, "pure text", key_type, dialect, schema_type, inorder=True, slop=True, check_parsing=True)

    @pytest.mark.skip(reason="Not sure when these got broken")
    def test_text_search_group_depth3_inorder_slop(self, key_type, dialect, schema_type):
        self._run_test(gen_depth3, "pure text", key_type, dialect, schema_type, inorder=True, slop=True, check_parsing=True)

    # ========================================================================
    # text with special characters
    # ========================================================================
    def test_text_search_unescaped(self, key_type, dialect, schema_type):
        """Test unescaped special characters in title field."""
        self._run_test(gen_unescaped_word, "punctuation", key_type, dialect, schema_type, field='title')

    def test_text_search_escaped(self, key_type, dialect, schema_type):
        """Test escaped special characters in body field."""
        exclude_all = True if key_type == "json" else False
        self._run_test(gen_escaped_word, "punctuation", key_type, dialect, schema_type, field='body', exclude_all=exclude_all)

    # ========================================================================
    # fuzzy search
    # ========================================================================
    def test_text_search_fuzzy(self, key_type, dialect, schema_type):
        """Test fuzzy search with Levenshtein distance 1."""
        self._run_test(gen_fuzzy_1, "pure text", key_type, dialect, schema_type)


# ============================================================================
# Multi-language compatibility tests
# ============================================================================
# All supported non-English languages from the Language enum in index_schema.proto
MULTILANG_LANGUAGES = [
    "french", "german", "spanish", "italian", "portuguese",
    "russian", "swedish", "turkish", "dutch", "indonesian", "arabic",
]

# Map language name to its dataset name in TEXT_DATASETS
LANG_TO_DATASET = {lang: f"{lang} text" for lang in MULTILANG_LANGUAGES}


def _edit_distance(s: str, t: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    m, n = len(s), len(t)
    if m == 0:
        return n
    if n == 0:
        return m
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, n + 1):
            temp = dp[j]
            if s[i - 1] == t[j - 1]:
                dp[j] = prev
            else:
                dp[j] = 1 + min(prev, dp[j], dp[j - 1])
            prev = temp
    return dp[n]


def _compute_safe_fuzzy_vocab(vocab_by_field: dict, language: str) -> dict:
    """Filter vocab to words safe for fuzzy compatibility testing.

    When running fuzzy queries against RediSearch, we observe that fuzzy matches
    can occur against stemmed forms of indexed terms — i.e. RediSearch appears to
    match fuzzy queries against stems in addition to original tokens. Valkey Search
    does not do this; fuzzy matching operates only on the original indexed forms.

    To avoid this known behavioral divergence in the compatibility suite, we
    exclude any vocab word W where a 1-char mutation of W could land within
    edit-distance 1 of a stem. Since gen_fuzzy_1 mutates W by exactly 1
    character, the resulting query is distance 1 from W. For that query to also
    be within distance 1 of a stem S, W must be within distance 2 of S (by
    triangle inequality). Therefore we conservatively exclude any word W where
    edit_distance(W, S) <= 2 for any stem S that differs from its original.

    Returns a new vocab_by_field dict with only safe words per field.
    If a field has no safe words, it is omitted from the result.
    """
    import snowballstemmer  # dev-only dep; used at generation time (regenerate.sh), not CI runtime
    stemmer = snowballstemmer.stemmer(language)

    # Collect all unique stems across all fields that differ from the original
    all_words = set()
    for words in vocab_by_field.values():
        all_words.update(w.lower() for w in words)
    divergent_stems = set()
    for word in all_words:
        stem = stemmer.stemWord(word)
        if stem != word:
            divergent_stems.add(stem)

    if not divergent_stems:
        return vocab_by_field  # No stems differ from originals — all words are safe

    # Filter each field's vocab to only safe words
    safe_vocab = {}
    for field, words in vocab_by_field.items():
        safe_words = []
        for word in words:
            w_lower = word.lower()
            is_safe = all(
                _edit_distance(w_lower, stem) > 2 for stem in divergent_stems
            )
            if is_safe:
                safe_words.append(word)
        if safe_words:
            safe_vocab[field] = safe_words

    return safe_vocab



@pytest.mark.parametrize("language", MULTILANG_LANGUAGES)
@pytest.mark.parametrize("schema_type", ["default", "nostem"])
@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["hash", "json"])
class TestMultiLangTextSearchCompatibility(BaseCompatibilityTest):
    """Compatibility tests for non-English language text search.

    Exercises the same query patterns as TestTextSearchCompatibility but
    against language-specific datasets with LANGUAGE set in FT.CREATE.
    """
    TEXT_QUERY_TEST_SEED = 7721
    MAX_QUERIES = 200  # Fewer per-language to keep total pickle size manageable
    ANSWER_FILE_NAME = "text-search-multilang-answers.pickle.gz"

    def setup_data(self, data_set_name, key_type, schema_type):
        """Override to specify text data source with language."""
        self.data_set_name = data_set_name
        self.key_type = key_type
        self.schema_type = schema_type
        self.client.execute_command("FLUSHALL SYNC")
        load_data(self.client, data_set_name, key_type, data_source='text', schema_type=schema_type)

    def execute_command(self, cmd):
        """Override to include schema_type and language in answer."""
        answer = {"cmd": cmd,
                "key_type": self.key_type,
                "data_set_name": self.data_set_name,
                "schema_type": self.schema_type,
                "testname": os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0],
                "traceback": "".join(traceback.format_stack())}
        try:
            print("Cmd:", *cmd)
            answer["result"] = self.client.execute_command(*cmd)
            answer["exception"] = False
            if answer["result"] != [0]:
                self.__class__.replied_count += 1
            print(f"replied: {answer['result']} (count: {self.__class__.replied_count})")
        except Exception as exc:
            print(f"Got exception for Error: '{exc}', Cmd:{cmd}")
            answer["result"] = {}
            answer["exception"] = True
        self.answers.append(answer)

    def _run_test(self, builder_fn, data_set_name, key_type, dialect, schema_type,
                  language, inorder=False, slop=False, field=None, vocab_override=None):
        """Run a test with given term builder function against a language dataset."""
        self.setup_data(data_set_name, key_type, schema_type)
        rng = random.Random(self.TEXT_QUERY_TEST_SEED)
        renderer = TermRenderer()
        vocab_by_field = vocab_override if vocab_override is not None else \
            data_sets.extract_vocab_by_field_from_text_data(data_set_name, key_type)

        seen = set()
        query_count = 0
        attempts = 0
        max_attempts = self.MAX_QUERIES * 20

        while query_count < self.MAX_QUERIES and attempts < max_attempts:
            attempts += 1
            selected_field = field if field is not None else rng.choice(list(vocab_by_field.keys()))
            vocab = vocab_by_field[selected_field]

            try:
                result = builder_fn(vocab, rng)
                current_query = result if isinstance(result, str) else renderer.render(result)
                if current_query in seen:
                    continue
                seen.add(current_query)

                args = ["FT.SEARCH", f"{key_type}_idx1", current_query]
                if inorder:
                    args.append("INORDER")
                if slop:
                    args.extend(["SLOP", str(rng.randint(1, 2))])
                args.extend(["DIALECT", str(dialect)])
                self.check(*args)
                query_count += 1

            except Exception as e:
                print(f"Error generating query: {e}")
                continue

        print(f"Generated {query_count} unique queries from {attempts} attempts")

    # ========================================================================
    # Core query types — exercised per language
    # ========================================================================

    def test_multilang_exact_match(self, key_type, dialect, schema_type, language):
        """Test exact word matching in the given language."""
        self._run_test(gen_word, LANG_TO_DATASET[language], key_type, dialect, schema_type, language)

    def test_multilang_prefix(self, key_type, dialect, schema_type, language):
        """Test prefix wildcard queries in the given language."""
        self._run_test(gen_prefix, LANG_TO_DATASET[language], key_type, dialect, schema_type, language)

    def test_multilang_suffix(self, key_type, dialect, schema_type, language):
        """Test suffix wildcard queries in the given language."""
        self._run_test(gen_suffix, LANG_TO_DATASET[language], key_type, dialect, schema_type, language)

    def test_multilang_group_depth2(self, key_type, dialect, schema_type, language):
        """Test grouped queries with depth 2 in the given language."""
        self._run_test(gen_depth2, LANG_TO_DATASET[language], key_type, dialect, schema_type, language)

    def test_multilang_fuzzy(self, key_type, dialect, schema_type, language):
        """Test fuzzy search with Levenshtein distance 1 in the given language."""
        dataset = LANG_TO_DATASET[language]
        full_vocab = data_sets.extract_vocab_by_field_from_text_data(dataset, key_type)
        safe_vocab = _compute_safe_fuzzy_vocab(full_vocab, language)
        if not safe_vocab:
            pytest.skip(
                f"{language}: no vocab words safe for fuzzy compat testing "
                f"(all within edit-distance 2 of a stem)"
            )
        self._run_test(
            gen_fuzzy_1, dataset, key_type, dialect, schema_type,
            language, vocab_override=safe_vocab
        )

