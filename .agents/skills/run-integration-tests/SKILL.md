---
name: run-integration-tests
description: Run or troubleshoot Valkey Search C++ and Python integration tests, including focused development runs, full-suite verification, and packaging/build or runtime failure classification.
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

2. During development, run the smallest focused test that exercises the
   changed path. Inspect the available selectors with:

   ```bash
   ./build.sh --help
   testing/integration/run.sh --help
   ```

   Use `build.sh --run-integration-tests=<pattern>` for Python tests or
   `testing/integration/run.sh --test <suite>` for C++ suite selection.

3. Before opening or updating a PR, run the full integration suite through the
   devcontainer:

   ```bash
   .devcontainer/run_in_docker.sh ./build.sh --run-integration-tests --debug
   ```

   Do not run integration servers directly on the host.

## Failure Reporting

Report the command, selector, harness, mode, and relevant logs. Classify the
result as a product, packaging/build, harness, cluster-readiness/timing, or
environment failure.
