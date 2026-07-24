# Agent Guidelines - Valkey Search

This file provides critical context, architectural constraints, and concurrency guidelines for AI Agents (and humans) contributing to the `valkey-search` repository. Adhering to these rules prevents regressions, concurrency deadlocks, and testing failures.

## 🎯 Core Engineering Philosophy
`valkey-search` is built exclusively for workloads requiring **high performance** and **low memory overhead**. Every architectural decision must prioritize efficiency:
- Avoid unnecessary object allocations, wrappers, or indirection layers.
- Code should be highly optimized and memory-efficient.
- Micro-optimizations, cache-friendly data structures, and lock-free concurrency patterns should be favored wherever they improve throughput or reduce latency.

## 🧵 Concurrency & Thread-Safety Model

Valkey Search operates in a hybrid multi-threaded environment where the Valkey Main Thread and background Writer/Reader threads coexist. 

### 1. The Main Thread Guarantee
- **`vmsdk::VerifyMainThread()` is absolute**: Code running under this check is strictly single-threaded with respect to other main-thread callers. 
- Therefore, thread-local or main-thread-only structures (such as statistical counters that are only read/written by the main thread) **do not require mutex protection**. 
- Always prefer lock-free single-thread accesses where this guarantee applies.

### 2. Time-Sliced Phase Mutex vs. Exclusive Guards
- **Search vs. Ingestion Coordination**: `valkey-search` maintains global, dedicated worker thread pools for Read operations and Write operations. The **Time-Sliced Phase Mutex** is a member of the `IndexSchema` object and strictly ensures mutual exclusion between read operations (`FT.SEARCH`/`FT.AGGREGATE`) and write operations from the ingestion flow. During the search and aggregate phase on an index, there is no risk of index mutations happening concurrently on that same index.
- **Cross-Thread Concurrent Mutations**: Data structures that are actively mutated across multiple threads simultaneously (e.g., accessed by both background writer threads and the main thread) MUST be protected by explicit exclusive guards rather than the time-sliced mutex. For example, `IndexKeyInfoMap index_key_info_` relies entirely on `mutated_records_mutex_`.
- Always annotate phase constraints precisely using `ABSL_SHARED_LOCKS_REQUIRED` and `ABSL_EXCLUSIVE_LOCKS_REQUIRED`.

### 3. Patching Third-Party Algorithms (HNSW / Flat)
- **Lock-Free Reads**: During the search phase, concurrent readers should be able to read index metadata (like capacity or element counts) without acquiring heavy phase locks.
- **Atomic Members**: Always make the underlying data structure members atomic (e.g., `hnswlib::ChunkedArray::element_count_` as a `std::atomic<size_t>`) and use relaxed memory ordering (`std::memory_order_relaxed`) for the reads.
- **Do NOT add wrappers**: Never add atomic capacities or synchronization wrappers to the Valkey abstraction classes (e.g., `VectorFlat`). Always patch the third-party implementation directly to avoid unnecessary overhead and architectural pollution.

## 🧪 Integration Testing Architecture

### 1. Passing Module Arguments via Configuration Files
- **Command-line limitation**: Valkey (and Redis) translates command-line arguments into an in-memory, line-by-line configuration file on startup. Because of this, appending module arguments directly to the command line (e.g., `--loadmodule /path/to/libsearch.so --use-coordinator`) results in fatal parsing errors because the arguments are treated as separate top-level configuration directives.
- **Rule**: When programmatically starting `valkey-server` processes in test suites (e.g., `testing/integration/utils.py`), **always generate a temporary `.conf` file** and write the `loadmodule` directives exactly as they should be parsed. Never pass them via the process argument string.

### 2. Devcontainer Networking
- The devcontainer is configured with `--network host`, sharing the host's networking stack. 
- Running integration tests directly inside the devcontainer will cause failures due to port collisions on standard ports (e.g., `6379`) if a Valkey/Redis instance is already running on the host machine. Tests must be executed using dynamic non-privileged ports or wrapped in a dedicated bridge-network Docker container.

## ⚠️ Coding Gotchas & Clang-Tidy Hygiene

### 1. Bugprone Use-After-Move
- Always be vigilant about moving `shared_ptr` or heavy objects into constructors or class members.
- If an object is moved via `std::move(obj)`, **never** access the original variable on subsequent lines. Always access the class member (e.g., use `obj_->GetRawVector()` instead of `obj->GetRawVector()`).
- Avoid passing by `const T&` if you intend to move the object, as it defeats the purpose and triggers static analysis warnings.

### 2. Strict Synchronization Assertions
- Always compile and validate changes using strict `clang-tidy` thread-safety annotations:
  ```bash
  clang-tidy -p .build-release/compile_commands.json \
    src/indexes/vector_flat.cc \
    src/indexes/vector_hnsw.cc \
    src/vector_registry.cc \
    src/index_schema.cc \
    -warnings-as-errors='*'
  ```
- Any violation of mutex guards or thread annotations should be treated as a blocking release error.

## 🏷️ Commit & DCO Hygiene
- **Developer Certificate of Origin (DCO)**: The repository strictly enforces DCO compliance on all PR contributions.
- **Rule**: Every single git commit MUST be signed off using the `-s` flag (`git commit -s`). Commits lacking a valid `Signed-off-by:` line will trigger an immediate, blocking check failure in GitHub Actions.
