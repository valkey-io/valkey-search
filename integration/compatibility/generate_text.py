import pytest
import random
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
])

# uncomment when stemming is finished
# @pytest.mark.parametrize("schema_type", ["default", "nostem"])
@pytest.mark.parametrize("schema_type", ["nostem"])
@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["hash"])
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
        import os, traceback
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

    # helper function to parse the result of Redis FT.EXPLAINCLI
    # use it to detect parsing differences
    def parse_explaincli_to_query(self, index_name: str, query: str, dialect: int = 2) -> tuple[str, bool]:
        import re
        
        # Execute FT.EXPLAINCLI command
        result = self.client.execute_command("FT.EXPLAINCLI", index_name, query, "DIALECT", str(dialect))
        
        # Decode if bytes
        if isinstance(result, bytes):
            result = result.decode('utf-8')
        elif isinstance(result, list):
            result = '\n'.join(r.decode('utf-8') if isinstance(r, bytes) else str(r) for r in result)
        
        lines = result.split('\n')
        
        # Parse the tree structure
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
        
        # Reconstruct query
        def reconstruct(node):
            if isinstance(node, str):
                return node
            elif isinstance(node, dict):
                op_type = node['type']
                children = node['children']
                
                if not children:
                    return ''
                
                reconstructed_children = [reconstruct(child) for child in children]
                
                if op_type == 'AND':
                    result = ' '.join(reconstructed_children)
                    return f'({result})' if len(reconstructed_children) > 1 else result
                elif op_type == 'OR':
                    # Sort OR terms for consistent comparison (OR is commutative)
                    reconstructed_children.sort()
                    result = ' | '.join(reconstructed_children)
                    return f'({result})' if len(reconstructed_children) > 1 else result
            return ''
        
        if not stack:
            reconstructed = ''
        elif len(stack) == 1:
            reconstructed = reconstruct(stack[0])
        else:
            reconstructed = ' '.join(reconstruct(node) for node in stack)
        
        # Normalize function that handles Redis query normalization rules
        def normalize(q):
            # Remove all whitespace first for easier processing
            q = re.sub(r'\s+', ' ', q.strip())
            
            # Parse into a tree structure
            def parse_to_tree(s):
                s = s.strip()
                if not s:
                    return None
                
                # Remove outer parentheses if they wrap everything
                while s.startswith('(') and s.endswith(')'):
                    # Check if these parens actually wrap the whole expression
                    depth = 0
                    wraps_all = True
                    for i, c in enumerate(s[1:-1], 1):
                        if c == '(':
                            depth += 1
                        elif c == ')':
                            depth -= 1
                        if depth < 0:
                            wraps_all = False
                            break
                    if wraps_all and depth == 0:
                        s = s[1:-1].strip()
                    else:
                        break
                
                # Check for OR operator at top level
                depth = 0
                or_positions = []
                for i, c in enumerate(s):
                    if c == '(':
                        depth += 1
                    elif c == ')':
                        depth -= 1
                    elif depth == 0 and i > 0 and s[i-1:i+1] == ' |':
                        or_positions.append(i-1)
                
                if or_positions:
                    # Split by OR
                    parts = []
                    last = 0
                    for pos in or_positions:
                        parts.append(s[last:pos].strip())
                        last = pos + 2  # Skip ' |'
                    parts.append(s[last:].strip())
                    
                    children = [parse_to_tree(p) for p in parts if p]
                    children.sort()  # Sort OR terms
                    return ('OR', children)
                
                # Check for AND (space-separated at top level)
                depth = 0
                and_positions = []
                i = 0
                while i < len(s):
                    if s[i] == '(':
                        depth += 1
                    elif s[i] == ')':
                        depth -= 1
                    elif depth == 0 and s[i] == ' ' and (i + 1 >= len(s) or s[i+1] != '|'):
                        and_positions.append(i)
                    i += 1
                
                if and_positions:
                    # Split by AND
                    parts = []
                    last = 0
                    for pos in and_positions:
                        part = s[last:pos].strip()
                        if part:
                            parts.append(part)
                        last = pos + 1
                    part = s[last:].strip()
                    if part:
                        parts.append(part)
                    
                    if len(parts) > 1:
                        children = [parse_to_tree(p) for p in parts]
                        return ('AND', children)
                
                # It's a single term
                return s
            
            # Convert tree back to normalized string
            def tree_to_string(tree):
                if isinstance(tree, str):
                    return tree
                op, children = tree
                child_strs = [tree_to_string(c) for c in children]
                if op == 'OR':
                    result = ' | '.join(child_strs)
                    return f'({result})'
                else:  # AND
                    result = ' '.join(child_strs)
                    return f'({result})' if len(child_strs) > 1 else result
            
            tree = parse_to_tree(q)
            return tree_to_string(tree) if tree else ''
        
        original_normalized = normalize(query)
        reconstructed_normalized = normalize(reconstructed)
        
        is_valid = original_normalized == reconstructed_normalized
        
        return reconstructed, is_valid

    # extract only words from the result of FT.EXPLAINCLI
    # return a list of words only
    def extract_words_from_explaincli(self, index_name: str, query: str, dialect: int = 2) -> list:
        import re

        # Execute FT.EXPLAINCLI command
        result = self.client.execute_command("FT.EXPLAINCLI", index_name, query, "DIALECT", str(dialect))
        
        # Decode if bytes
        if isinstance(result, bytes):
            result = result.decode('utf-8')
        elif isinstance(result, list):
            result = '\n'.join(r.decode('utf-8') if isinstance(r, bytes) else str(r) for r in result)
        
        # Extract terms
        terms = []
        for line in result.split('\n'):
            # Remove leading numbers and closing parentheses
            cleaned = re.sub(r'^\d+\)\s*', '', line.strip())
            
            # Skip empty lines, operators, braces, and expanded terms
            if not cleaned or cleaned in ['{', '}'] or cleaned.startswith('+'):
                continue
            if any(keyword in cleaned for keyword in ['UNION', 'INTERSECT', 'NOT', '(expanded)']):
                continue
            
            # If it's a simple word, add it
            if re.match(r'^[a-zA-Z0-9_-]+$', cleaned):
                terms.append(cleaned)
        
        return terms
    
    # extract only words from the result of FT.EXPLAINCLI
    # return a list of words only
    def extract_words_from_query(self, query: str) -> list:
        import re
        # Remove all non-alphanumeric characters except spaces and hyphens/underscores in words
        # This removes: ( ) | { } => $ @ * % " and other special chars
        cleaned = re.sub(r'[^\w\s-]', ' ', query)
        
        # Split by whitespace and filter out empty strings
        words = [word for word in cleaned.split() if word]
        
        return words

    def _run_test(self, builder_fn, data_set_name, key_type, dialect, schema_type, inorder=False, slop=False, check_parsing=False, field=None, query_str=None):
        """Helper to run a test with given term builder function.
        
        Args:
            builder_fn: Function that takes (vocab, rng) and returns term(s) or query string
            data_set_name: Name of the data set to test against
            key_type: Type of key ("json" or "hash")
            dialect: Query dialect version
        """
        try:
            info = self.client.execute_command("INFO", "SERVER")
            # Decode if bytes
            if isinstance(info, bytes):
                info = info.decode('utf-8')
            is_redis = "redis_version" in info
        except Exception:
            is_redis = False
        
        matched_count = 0
        mismatched_count = 0
        self.setup_data(data_set_name, key_type, schema_type)
        rng = random.Random(self.TEXT_QUERY_TEST_SEED)
        renderer = TermRenderer()

        vocab_by_field = data_sets.extract_vocab_by_field_from_text_data(
            data_set_name, key_type
        )

        seen = set()
        query_count = 0
        attempts = 0
        max_queries = 1 if query_str else self.MAX_QUERIES
        max_attempts = self.MAX_QUERIES * 20
        while query_count < max_queries and attempts < max_attempts:
            attempts += 1

            if field is not None:
                selected_field = field
            else:
                selected_field = rng.choice(list(vocab_by_field.keys()))
            vocab = vocab_by_field[selected_field]

            try:
                if query_str:
                    current_query = query_str
                else:
                    result = builder_fn(vocab, rng)
                    current_query = result if isinstance(result, str) else renderer.render(result)

                if current_query not in seen:
                    seen.add(current_query)

                    # check if Redis parsing is correct by calling FT.EXPLAINCLI, 
                    # and compare the words order with the query string
                    if is_redis and check_parsing:
                        reconstructed_query, is_valid = self.parse_explaincli_to_query(
                            f"{key_type}_idx1", 
                            current_query, 
                            dialect
                        )
                        if is_valid:
                            matched_count += 1
                        else:
                            mismatched_count += 1
                            print(f"Redis parsing is inconsistent for query:")
                            print(f"  Original:      {current_query}")
                            print(f"  Reconstructed: {reconstructed_query}")
                            continue
                        print(f"Matched: {matched_count}, Mismatched: {mismatched_count}")  

                    # exclude queries with known bugs in Redis
                    if current_query in excluded_queries:
                        print(f"Query excluded with known Redis bug: {current_query}")
                        continue  

                    args = [
                        "FT.SEARCH",
                        f"{key_type}_idx1",
                        current_query,
                    ]
                    if inorder:
                        args.append("INORDER")
                    if slop:
                        slop_value = str(rng.randint(1, 2))
                        args.append("SLOP")
                        args.append(slop_value)
                    args.extend([
                        "DIALECT",
                        str(dialect)
                    ])
                    self.check(*args)
                    query_count += 1
                    
            except Exception:
                # Skip invalid queries and continue
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
    
    def test_text_search_group_depth2_inorder(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 2."""
        self._run_test(gen_depth2, "pure text", key_type, dialect, schema_type, inorder=True, check_parsing=True)

    def test_text_search_group_depth3_inorder(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 3."""
        self._run_test(gen_depth3, "pure text", key_type, dialect, schema_type, inorder=True, check_parsing=True)
    
    def test_text_search_group_depth2_slop(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 2."""
        self._run_test(gen_depth2, "pure text", key_type, dialect, schema_type, slop=True, check_parsing=True)

    def test_text_search_group_depth3_slop(self, key_type, dialect, schema_type):
        """Test grouped queries with depth 3."""
        self._run_test(gen_depth3, "pure text", key_type, dialect, schema_type, slop=True, check_parsing=True)

    def test_text_search_group_depth2_inorder_slop(self, key_type, dialect, schema_type):
        self._run_test(gen_depth2, "pure text", key_type, dialect, schema_type, inorder=True, slop=True, check_parsing=True)

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
        self._run_test(gen_escaped_word, "punctuation", key_type, dialect, schema_type, field='body')
    # ========================================================================
    # fuzzy search
    # ========================================================================
    def test_text_search_fuzzy(self, key_type, dialect, schema_type):
        """Test fuzzy search with Levenshtein distance 1."""
        self._run_test(gen_fuzzy_1, "pure text", key_type, dialect, schema_type)