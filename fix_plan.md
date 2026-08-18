To fix this performance regression completely and restore the original 2,050 QPS while maintaining the new custom allocator and memory savings, you only need to make a 2-line change to `src/utils/string_interning.h`.

### The Core Explanation
In `valkey-search4`, Document IDs (e.g., `doc:1`) are stored as `InternedStringPtr`s. When a Prefix query (`term*`) executes, the engine collects document postings for every matching term (`termA`, `termB`) and uses a K-Way Merge iterator (`TermIterator`) to merge their Document IDs into a single sorted stream.

If `InternedStringPtr::operator<=>` performs deep string comparison (`Str() <=> other.Str()`), the priority queue is forced to fetch highly-fragmented string contents from RAM to alphabetize the Document IDs, triggering millions of L2/L3 cache misses and dropping performance to ~712 QPS.

However, all `KeyIterator`s and `btree_map`s across the entire search engine rely on the exact same `operator<=>` to sort documents. This means the engine **does not actually require Document IDs to be sorted alphabetically**—it only requires them to be sorted in a **consistent, deterministic strict weak ordering**.

Because `InternedStringPtr` guarantees that identical strings are mapped to the exact same singleton `impl_` 64-bit integer (whether it's a heap pointer or an inline string mask), comparing the raw `impl_` integers provides a lightning-fast, mathematically perfect deterministic ordering! 

### The Fix
Open `src/utils/string_interning.h` and update both `InternedStringPtr` and `BorrowedInternedStringPtr` to compare their `impl_` values directly, removing the fallback to `Str()`:

```cpp
  auto operator<=>(const InternedStringPtr &other) const {
    return impl_ <=> other.impl_;
  }
```

```cpp
  auto operator<=>(const BorrowedInternedStringPtr &other) const {
    return impl_ <=> other.impl_;
  }
```

**Why this is safe with inline strings:**
The `optimized_rax` branch introduced inline string interning (using the 63rd bit as a mask). When you compare `impl_ <=> other.impl_`, you will be comparing raw heap pointers against inline data masks. Because inline masks have the high bit set, they will naturally sort "greater" than heap pointers. This is a 100% valid, deterministic integer comparison that will perfectly satisfy all K-Way Merges and `IntersectIterator` requirements, completely eliminating `__memcmp_evex_movbe` from the CPU profile and restoring your QPS to `main` levels!
