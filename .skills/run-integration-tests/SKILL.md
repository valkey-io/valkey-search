---
name: run-integration-tests
description: Run and diagnose Valkey Search C++ and Python integration harnesses, including standalone, cluster/coordinator, vector-search, stability, and compatibility tests. Use when building, selecting, executing, or troubleshooting integration tests in a valkey-search checkout.
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
   incomplete build. Otherwise reuse the configured build without it.
   Classify dependency failures before test execution by source: missing
   dependencies in newly generated repository-owned bundles are packaging/build
   failures, while stale or externally misconfigured dependencies are
   environment failures. Newly generated dependency bundles should contain all
   dependencies required by the selected build. Regenerate an existing
   incomplete bundle; use the fallback command only until that is done.

2. Inspect the requested test and identify its harness and mode:
   standalone CMD, cluster CME, C++ vector-search, or C++ stability.

3. Run through the devcontainer:

   ```bash
   .devcontainer/run_in_docker.sh ./build.sh --run-integration-tests --debug
   ```

   Follow `AGENTS.md` for networking requirements. Do not run integration
   servers directly on the host.

4. For focused runs, inspect:

   ```bash
   ./build.sh --help
   testing/integration/run.sh --help
   ```

   Use `build.sh --run-integration-tests=<pattern>` to select Python tests;
   this skips the C++ harness. Use
   `testing/integration/run.sh --test <suite>` for C++ suite selection.

## Python Cluster Tests

For coordinator, shard fan-out, replica, failover, and recovery changes:

- Test standalone and cluster/coordinator modes where the path is shared.
- Cover single-shard and cross-shard keys, indexing, backfill, search, and
  aggregate fan-out/reduction.
- Include partial shard failures, replicas, failover, and recovery where
  applicable.
- Confirm the harness created the expected nodes, slots, replicas, and
  generated per-node configuration files.

The harness starts nodes through `integration/valkeytestframework`, writes
module arguments into configuration files, creates clusters with
`valkey-cli`, and waits for topology convergence.

## Compatibility Tests

RDB or module-version tests need the exact historical server/module fixtures,
with architecture-matched binaries. Do not treat a current server plus an old
module as full historical-runtime coverage. Report whether the test verifies
module serialization compatibility, server compatibility, or both.

## Failure Reporting

Preserve and report the exact command, selector, harness, mode, node ports,
replica count, module arguments, generated config paths, cluster state,
topology, and per-node logs. Classify the result as a product, harness,
cluster-readiness/timing, or environment failure.

Do not claim cluster coverage from unit tests or standalone tests. A cluster
claim requires an executed multi-node test or an explicit explanation of why
the changed path cannot run in cluster mode.
