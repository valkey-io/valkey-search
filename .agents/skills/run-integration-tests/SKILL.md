---
name: run-integration-tests
description: Run and diagnose Valkey Search C++ and Python integration harnesses, including standalone, cluster/coordinator, vector-search, and stability tests. Use when executing or troubleshooting integration tests in a valkey-search checkout.
---

# Run Integration Tests

Run and diagnose the Valkey Search integration suites. The repository has two
top-level integration harnesses:

- **Abseil/C++ harness** under `testing/integration/`. It runs the
  `vector_search_integration` and `stability` suites through
  `testing/integration/run.sh`.
- **OSS Python harness** under `integration/`. It runs pytest tests through the
  Valkey test framework. Its test cases cover standalone command mode (CMD)
  and cluster/coordinator mode (CME).

## Common Steps

1. Check the build prerequisites. If CMake reports a missing dependency from
   `/opt/valkey-search-deps`, bootstrap with:

   ```bash
   .devcontainer/run_in_docker.sh ./build.sh --no-system-modules --debug
   ```

   Use `--no-system-modules` again only after cleaning or reconfiguring an
   incomplete build. Classify missing dependencies in newly generated
   repository-owned bundles as packaging/build failures; classify stale or
   externally misconfigured dependencies as environment failures.

2. Run the full integration suite through the devcontainer:

   ```bash
   .devcontainer/run_in_docker.sh ./build.sh --run-integration-tests --debug
   ```

   Do not run integration servers directly on the host.

3. For a requested focused run or fast iteration, inspect:

   ```bash
   ./build.sh --help
   testing/integration/run.sh --help
   ```

   Use `build.sh --run-integration-tests=<pattern>` for Python tests or
   `testing/integration/run.sh --test <suite>` for C++ suite selection.

## Python Cluster Tests

For coordinator or replica changes, run the applicable test in standalone and
cluster modes. For fan-out coverage, use data whose matching documents span at
least two shards. Add failover or recovery tests only when the changed path
requires them.

## Failure Reporting

Report the command, selector, harness, mode, and relevant logs. Classify the
result as a product, harness, cluster-readiness/timing, or environment failure.
