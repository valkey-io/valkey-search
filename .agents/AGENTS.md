# Agent Guidelines - Valkey Search

## 🎯 Core Engineering Philosophy
Prioritize efficiency and minimal memory overhead:
- Avoid unnecessary object allocations, wrappers, and indirection layers.
- Favor cache-friendly data structures and lock-free concurrency patterns.

## 🧵 Concurrency & Thread-Safety Model

### 1. The Main Thread Guarantee
- **`vmsdk::VerifyMainThread()` is absolute**: Code under this check is strictly single-threaded.
- Main-thread-only data structures (such as main-thread stats) must not use mutexes.

### 2. Time-Sliced Phase Mutex vs. Exclusive Guards
- **Phase Mutex**: Member of `IndexSchema` ensuring mutual exclusion between read queries (`FT.SEARCH`/`FT.AGGREGATE`) and ingestion mutations.
- **Cross-Thread Concurrent Mutations**: Data structures mutated across multiple threads simultaneously (e.g., `IndexKeyInfoMap`) must use explicit exclusive mutexes.
- Annotate phase constraints with `ABSL_SHARED_LOCKS_REQUIRED` and `ABSL_EXCLUSIVE_LOCKS_REQUIRED`.

### 3. Patching Third-Party Algorithms (HNSW / Flat)
- **Lock-Free Reads**: Use atomic members with relaxed memory ordering (`std::memory_order_relaxed`) for metadata reads during search.
- **No Wrappers**: Patch third-party data structures directly rather than wrapping them in Valkey abstraction classes.

## 🧪 Testing Architecture & Verification

### 1. Passing Module Arguments via Configuration Files
- In integration/test scripts starting `valkey-server`, always pass module arguments via a temporary `.conf` file. Never pass them directly via command-line arguments.

### 2. Devcontainer Execution
- Run integration tests, formatting, and clang-tidy only via `.devcontainer/run_in_docker.sh`.

### 3. Testing Commands
- **Unit Tests**:
  ```bash
  ./build.sh --debug --run-tests
  ```
- **Integration Tests**:
  ```bash
  .devcontainer/run_in_docker.sh ./build.sh --run-integration-tests --debug
  ```
- **Fast Iteration**:
  ```bash
  ninja -C .build-debug-container <target> && ./.build-debug-container/tests/<target> --gtest_brief=1
  ```

## ⚠️ Coding Gotchas, Clang-Format & Clang-Tidy Hygiene

### 1. Bugprone Use-After-Move
- Never access an object after `std::move(obj)`. Access the assigned member instead.
- Do not pass parameters by `const T&` if they are moved.

### 2. Strict Synchronization Assertions
- Validate thread safety with clang-tidy via the devcontainer:
  ```bash
  .devcontainer/run_in_docker.sh clang-tidy -p .build-release/compile_commands.json <files> -warnings-as-errors='*'
  ```

### 3. Clang-Format inside Devcontainer
- Run formatting checks via devcontainer before committing:
  ```bash
  .devcontainer/run_in_docker.sh ./ci/clang-format-changes.sh
  ```

## 🏷️ Commit & DCO Hygiene
- **DCO Signoff**: Every commit and merge must include `-s` / `--signoff` (`git commit -s`, `git merge --signoff <branch>`).
- **Post-Merge Formatting**: Run `./ci/clang-format-changes.sh` after resolving merge conflicts.
