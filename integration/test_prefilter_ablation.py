"""Check the optimized path against exhaustive filtered signed dot products."""
import struct

from test_managed_sparse import sparse, search
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.util import waiters


class TestSparsePrefilter(ValkeySearchTestCaseBase):
    def test_boolean_numeric_and_inkeys(self):
        """Filter selection, dedup and zero-overlap behavior preserve exact top-K."""
        c = self.server.get_new_client()
        c.execute_command('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', 1, 'doc:',
                          'SCHEMA', 's', 'SPARSE', 'region', 'TAG', 'price', 'NUMERIC')
        docs = []
        for i in range(100):
            entries = [(1, float(i % 9 - 4)), (3, float(i % 7 - 3))]
            if i % 10 == 0: entries = [(99, 5.)]
            key = f'doc:{i:03d}'.encode()
            docs.append((i, key, entries))
            c.hset(key, mapping={'s': sparse(entries), 'region': str(i % 3), 'price': i})
        q = sparse([(1, 2.), (3, -1.)])
        waiters.wait_for_true(lambda: c.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', 0, 0)[0] == 100, timeout=10)
        expected_count = sum(any(dict(entries).get(d, 0) for d in [1, 3]) for _, _, entries in docs)
        waiters.wait_for_true(lambda: len(search(c, q, 100)) == expected_count, timeout=10)
        predicates = [
            ('@region:{0}', lambda i: i % 3 == 0),
            ('(@region:{0}|@region:{1})', lambda i: i % 3 in [0, 1]),
            ('-@region:{1}', lambda i: i % 3 != 1),
            ('@region:{0} @price:[3 40]', lambda i: i % 3 == 0 and 3 <= i <= 40),
            ('(@price:[0 30]|@price:[20 50])', lambda i: i <= 50),
            ('@region:{missing}', lambda i: False),
            ('*', lambda i: True),
        ]
        for predicate, accept in predicates:
            expected = []
            for i, key, entries in docs:
                weights = dict(entries)
                if accept(i) and any(weights.get(d, 0) for d in [1, 3]):
                    expected.append((-(weights.get(1, 0)*2 - weights.get(3, 0)), key))
            expected.sort()
            assert search(c, q, 100, predicate) == [(key, score) for score, key in expected]
        response = c.execute_command('FT.SEARCH', 'idx', '*=>[KNN 100 @s $q AS d]',
                                     'PARAMS', 2, 'q', q, 'INKEYS', 4,
                                     'doc:005', 'doc:010', 'doc:099', 'missing',
                                     'RETURN', 1, 'd', 'LIMIT', 0, 100)
        assert set(response[1::2]) == {b'doc:005', b'doc:099'}
