# Performance Benchmarks

valkey-search runs performance benchmarks on a PR by adding a label. The suite
covers all core index types — TEXT, TAG, NUMERIC, and VECTOR (FLAT and HNSW) —
on HASH-schema indexes. The benchmark builds the search module at the PR's
merge commit and at the base branch, runs the same benchmark against each, and
posts a comparison table as a PR comment.

## Running a benchmark on a PR

A repository collaborator or maintainer adds one of these labels to the PR —
either from the Labels menu or by posting the label name as a slash-command
comment (e.g. `/label run-search-benchmark`):

- `run-search-benchmark` — full run, including profiling (flamegraphs).
- `run-search-benchmark-skip-profiling` — faster run, skips profiling.

That's it. When the run finishes it posts a comparison comment on the PR and
automatically removes the label.

Reference PR: https://github.com/valkey-io/valkey-search/pull/1263

### Notes

- The PR must have a mergeable commit (no conflicts with the base branch). If it
  doesn't, the run fails with a message asking you to retry once merge is
  computed or resolve conflicts.
- Re-adding the same label cancels any in-progress run for that PR/label and
  starts fresh.
- The base branch must contain `ci/install_deps.sh`. If a PR targets an older
  branch without it, rebase onto a revision that includes the installer.

## Running a benchmark manually

From the Actions tab, run the **PR Search Benchmark** workflow
(`workflow_dispatch`) and provide the PR number. This path exposes extra inputs:

- `core_commit` — pin a specific valkey core commit (defaults to latest HEAD of
  `unstable`).
- `num_runs` — number of times to run each configuration (default `3`).
- `config_file` — benchmark config from `valkey-perf-benchmark/configs/`.
- `cluster_mode` — empty (use config), `true` (cluster only), or `false`
  (standalone only).
- `skip_profiling` — skip flamegraph generation for a faster run.
- `skip_config_set` — use the default config set.
- `timeout_minutes` — job timeout (default `1440`).

Both the label and manual paths run the exact same benchmark and post the same
comparison comment.
