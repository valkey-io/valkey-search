---
name: redisearch-compat-testing
description: Establish RediSearch's actual behavior as the parity reference for valkey-search work. Points to the repo's canonical compatibility harness (integration/compatibility/ — generators, redis:latest reference pickles, known_differences.md) as the primary tool, and provides a throwaway RediSearch-in-Docker probe for quick "what does RediSearch actually do here?" checks. Use when a valkey-search change hinges on unused/undefined PARAMS, DIALECT differences, SORTBY/LIMIT/MAX retention, ADDSCORES and score semantics, reply-format shape, FT.SEARCH vs FT.AGGREGATE differences, or any VALKEY_SEARCH_COMPATIBILITY_FIX decision, and you need a ground-truth measurement rather than an assumption.
---

# RediSearch Compatibility Testing

## Overview

`valkey-io/valkey-search` is a RediSearch-compatible module: its entire query surface (FT.SEARCH, FT.AGGREGATE, PARAMS, DIALECT, reply formats, scoring) is defined *relative to RediSearch*. Whenever a change turns on "what does RediSearch actually do in this case?", the answer must be **measured against real RediSearch**, not assumed — the repo even bakes RediSearch-measured constants into code via `VALKEY_SEARCH_COMPATIBILITY_FIX(...)`.

There are two levels to this, and you should reach for them in order:

1. **The repo's own compatibility harness** — `integration/compatibility/` — is the *primary, canonical* tool. It captures RediSearch reference answers into checked-in `*.pickle.gz` files and replays them against valkey-search in CI. Prefer extending it over ad-hoc probing whenever the behavior can be expressed as a generator case, because that makes the parity check permanent and regression-guarded.
2. **A throwaway Docker probe** (the `scripts/redisearch-docker.sh` helper here) is the *quick-check* complement: when you just need to see what RediSearch does for one command shape before writing a generator case or a `COMPATIBILITY.md` row, run RediSearch in a disposable container and probe it directly.

## Core Concepts

- **Two binaries, one contract.** `redis`/`redis-stack-server` is the *RediSearch reference*; `valkey-server` + `libsearch.so` is the *code under test*. A parity claim compares the two — measuring only the reference tells you the target, not whether valkey-search hits it.
- **Compatibility is a contract, not internal equivalence.** `COMPATIBILITY.md` defines which observable behaviors must match (command/arg syntax, query semantics, reply shapes, index types) and which are explicit non-goals (RDB/replication format, performance, error-message *wording*, attribute order, float precision). Classify a divergence against it before measuring.
- **The reference engine is version-scoped.** `COMPATIBILITY.md`'s version-mapping table picks the reference for the valkey-search version under test (1.3 → Redis 8 = `redis:latest`); it is not a free choice.
- **Gated fixes via `search.emulate-release`.** A compat fix that changes existing behavior ships gated with the old behavior as default; the in-repo harness replays with the gate maxed so every gated fix is active.
- **Harness over hand-probe.** A case captured as an `integration/compatibility/` generator is a permanent, CI-guarded parity check; a manual Docker probe is a one-off that evaporates when the shell closes.

## The canonical in-repo harness (`integration/compatibility/`)

Before (or instead of) hand-probing, check the repo's existing compatibility suite — it is the source of truth and likely already covers your area:

- **`integration/compatibility/`** holds `generate_*.py` generators (aggregate, text, array, filter, sortkey, return, expr), each subclassing `BaseCompatibilityTest` with its own `ANSWER_FILE_NAME`, plus their captured `*.pickle.gz` answer files. The `GENERATORS` list in `integration/compatibility/__init__.py` is the registry; `regenerate.sh` and `compatibility_test.py` both read from it.
- **The reference engine is `redis:latest` (Redis 8, which bundles RediSearch natively)** — *not* `redis-stack-server`. This matters: `known_differences.md §2` records cases where `redis:latest` and `redis/redis-stack-server` **disagree**, and several `COMPATIBILITY.md` rules were re-measured when the reference moved from redis-stack to redis:latest. Use `redis:latest` as the primary reference and only bring in redis-stack to explain a reference-vs-reference disagreement.
- **`known_differences.md`** and **`unsupported_tests.md`** (both in `integration/compatibility/`) are the authoritative record of *open* divergences, cases no generator covers, and where the two reference engines disagree. **Read them first** — your "new" divergence may already be documented and deliberately excluded.
- **`regenerate.sh`** spins up `redis:latest` in Docker (container `Generate-search-NNNN` on a docker-assigned port), runs the generators via pytest, and rewrites the pickles. Each pickle stores a SHA256 of every `.py` in the directory; `compatibility_test.py` verifies it on load and **fails if the sources changed but the pickle was not regenerated** — so editing any generator/`data_sets.py`/`__init__.py` forces a `./integration/compatibility/regenerate.sh` + commit of the updated `*.pickle.gz`.
- **`compatibility_test.py`** replays the pickles against valkey-search with *semantic* comparison (`compare_row`, `compare_number_eq` via `math.isclose`, sorted TOLIST arrays, negative-zero/NaN tolerance), and pins the replay to `emulate-release = 65535.255.255` so every release-gated compatibility fix is enabled regardless of the version it shipped in (debug-mode base classes).

**To make a new parity check permanent:** add a `generate_xxx.py` (subclass `BaseCompatibilityTest`, set `ANSWER_FILE_NAME`), register it in the `GENERATORS` list, run `./integration/compatibility/regenerate.sh`, and commit the new pickle. That is preferred over a one-off Docker probe whenever the case can be generated.

This skill's Docker helper below is for the quick-look step *before* you commit to a generator — or for behaviors the generators cannot express.

## Established project practices (do not reinvent)

These are the project's own documented conventions. Follow them rather than an ad-hoc equivalent.

- **The validation method is defined in `COMPATIBILITY.md`.** In its words: *"identical requests are sent to Valkey Search and Redisearch and the responses are compared for equality."* That is exactly what `integration/compatibility/` automates; a manual probe is the same method done by hand.
- **Pick the reference engine from the version mapping in `COMPATIBILITY.md`, not by habit.** Each Valkey Search version has a documented compatibility target:

  | Valkey Search version | Compatibility target |
  | --- | --- |
  | 1.3 | Redis 8 (`redis:latest`) |
  | 1.2 | RediSearch 2 (`redis/redis-stack-server`) |
  | < 1.2 | Unspecified (manual validation) |

  Current `main` targets **Redis 8**, which is why the harness records against `redis:latest`. If you are validating behavior for an older gated/default version, match its target from this table.
- **Breaking compat changes land behind the `search.emulate-release` gate, old behavior as default.** Per `COMPATIBILITY.md` ("Sunsetting of Incompatible Behavior"), a compatibility fix that changes existing behavior is selected by the `search.emulate-release` config: setting it below the fix's release keeps the old behavior, at/above enables the compatible one; the default is the current major, so a fix is opt-in until the next major promotes it. A fix that *cannot* preserve the old behavior is ungated and simply waits for the next major. This is the mechanism `compatibility_test.py` exercises by pinning the replay to the max `emulate-release` (`65535.255.255`), and what `VALKEY_SEARCH_COMPATIBILITY_FIX(...)` marks in code. A new gated fix needs a `COMPATIBILITY.md` row.
- **Stricter input validation is intentional, not a bug** (`COMPATIBILITY.md`). Where RediSearch tolerantly accepts extraneous/malformed syntax and valkey-search rejects it, that divergence is by design — *unless* the specific case is being deliberately relaxed for parity (as in #1372, unused PARAMS). Read this note before "fixing" a rejection.
- **Attribute ordering and float precision are explicit non-goals** (`COMPATIBILITY.md`): the *set* of returned attributes must match but not their order, and small floating-point differences and exact float text are tolerated. The harness already absorbs these (sorted rows, `compare_number_eq`); a hand diff must too — do not report an ordering/precision difference as a bug.
- **Run valkey-search the way `AGENTS.md` mandates** when you do the diff step:
  - Pass module arguments via a temporary `.conf` file when starting `valkey-server` — **never** on the command line.
  - Run integration tests, formatting, and clang-tidy **only** via `.devcontainer/run_in_docker.sh` (e.g. `.devcontainer/run_in_docker.sh ./build.sh --run-integration-tests --debug`); filter with `TEST_PATTERN=` (see `integration/README.md`).
  - Every commit is DCO-signed (`git commit -s`).

## Usage

Use this skill when:

- A valkey-search fix changes behavior toward (or away from) RediSearch and you need the reference behavior confirmed. Examples seen on this project:
  - **Unused / undefined PARAMS** — does RediSearch error or tolerate? (issue #1372: it tolerates.)
  - **SORTBY / LIMIT / MAX retention bounds** over many documents.
  - **FT.SEARCH vs FT.AGGREGATE** divergence for the same query.
  - **ADDSCORES / WITHSCORES** and score exposure semantics.
  - **DIALECT** (1 vs 2 vs 3) parsing and result differences.
  - **Reply-format shape** (array vs map, field ordering, RESP2 vs RESP3).
- You are writing or updating a `VALKEY_SEARCH_COMPATIBILITY_FIX` and need the source-of-truth number/behavior.
- A reviewer asks "did you check this against RediSearch?"

Do **not** use this to test valkey-search itself — that is the `valkey-search-contrib` skill (build + gtest + integration). This skill measures the *reference implementation*.

## Core workflow (quick Docker probe)

Use this for a one-off "what does RediSearch do here?" look. If the case can be expressed as a generator, prefer extending `integration/compatibility/` instead (see above) so the check is permanent.

0. **First consult `COMPATIBILITY.md` and the in-repo compatibility suite.**
   - **`COMPATIBILITY.md`** (repo root) is the authoritative definition of *what parity means* and decides whether an observed difference is even a bug before you spend effort measuring it:
     - **Expected-compatibility areas** (command/argument syntax, query language and semantics, reply shapes, index types) — an observable difference here **is a bug**. Measure it and treat the RediSearch behavior as the target.
     - **Non-goals** — differences here are **expected, not bugs**: binary/RDB/replication format, internal/source-level parity, performance, **exact error-message text** (only the *semantic* error condition must align), and the stricter Valkey Search security/ACL model.
     - The contract applies **only to features valkey-search actually implements**; an error for an unsupported RediSearch feature is intended behavior, not a defect. Valkey Search may also *exceed* RediSearch (see its Extensions section) and accept combinations RediSearch rejects.
   - **`integration/compatibility/known_differences.md` and `unsupported_tests.md`** — the record of divergences already found and (often deliberately) not matched. Your case may already be documented; check before re-measuring.

   So: classify the divergence against `COMPATIBILITY.md`, then check whether the in-repo suite already covers it. If it falls under a non-goal (e.g. you are only comparing error *wording*), it is out of scope and Docker measurement is unnecessary. If it is an expected-compatibility area not already captured, proceed to measure.

1. **Start RediSearch in Docker.** Use **`redis:latest`** (Redis 8, native RediSearch) to match the reference engine the in-repo harness records against; only pull in `redis/redis-stack-server` to explain a reference-vs-reference disagreement (see `known_differences.md §2`). The helper defaults to redis-stack for the interactive shell but takes any image via `RS_IMAGE`:

   ```bash
   RS_IMAGE=redis:latest scripts/redisearch-docker.sh up   # match the harness reference engine
   scripts/redisearch-docker.sh cli                        # drop into redis-cli against it
   scripts/redisearch-docker.sh down                       # stop and remove the container
   ```

2. **Pin the image, then record the version you measured.** These are two distinct steps. RediSearch behavior can change across releases, so a parity claim is only meaningful with a fixed reference. The default `RS_IMAGE=redis/redis-stack-server:latest` is a **mutable tag** — fine for a quick look, but for any recorded parity claim set `RS_IMAGE` to a fixed tag or digest so the run is repeatable:

   ```bash
   RS_IMAGE=redis/redis-stack-server:7.4.0-v1 scripts/redisearch-docker.sh up   # pin the image
   scripts/redisearch-docker.sh cli FT._LIST                  # sanity: module loaded
   scripts/redisearch-docker.sh cli MODULE LIST               # record the search module version
   ```

   Recording `MODULE LIST` documents which module build you tested; it does **not** by itself make the run reproducible — that requires pinning `RS_IMAGE`. `up` and `status` print the resolved image (repo digest when available) so the run is self-documenting.

3. **Reproduce the exact command shape under question.** Build the smallest index + dataset that exercises the behavior, then run the exact FT.* command the valkey-search code path parses. Use `scripts/redisearch-docker.sh cli <ARGS...>` for one-offs or pipe a script into `... cli` on stdin. See `references/probe-recipes.md` for ready-made probes (unused PARAMS, DIALECT, SORTBY/LIMIT, ADDSCORES, RESP3).

4. **Diff against valkey-search — and prefer the harness for anything permanent.** For a quick check, run the same command shape against a local `valkey-server` loading `libsearch.so` (see `valkey-search-contrib`), passing any module args via a temporary `.conf` file per `AGENTS.md` (never on the command line), and compare replies. Where they differ, that gap is the bug (or the intended divergence). If the behavior can be generated, capture it as a `generate_*.py` case in `integration/compatibility/`, regenerate the pickle, and run the suite through the devcontainer the way `AGENTS.md` mandates:

   ```bash
   .devcontainer/run_in_docker.sh ./build.sh --run-integration-tests --debug   # full suite
   TEST_PATTERN=compatibility integration/run.sh                               # filter to the compat replay
   ```

   A one-off manual diff protects nothing after you close the terminal; the generated case in `compatibility_test.py` is what guards against regression.

5. **Document the measurement.** Put the RediSearch version, the exact commands, and the observed reply in the commit body and PR description, and state which `COMPATIBILITY.md` category the divergence falls under (expected-compatibility bug vs. non-goal). A parity claim without the reproducing commands is not verifiable.

6. **Tear down.** `scripts/redisearch-docker.sh down` — the container is disposable; never leave it running.

## Key facts

- **Prefer the in-repo harness over ad-hoc probing.** `integration/compatibility/` captures reference answers into checked-in pickles that CI replays; a generator case is a permanent, regression-guarded parity check, whereas a manual Docker probe evaporates when you close the shell.
- **The harness reference engine is `redis:latest` (Redis 8), not `redis-stack-server`.** They disagree on some cases (`known_differences.md §2`); measuring against redis-stack when the suite records against redis:latest can produce a "divergence" that is really just the wrong reference.
- **Editing any generator source forces a regenerate.** Each pickle embeds a SHA256 of the directory's `.py` files; `compatibility_test.py` fails on a mismatch, so a generator/`data_sets.py`/`__init__.py` edit must be followed by `./integration/compatibility/regenerate.sh` and a commit of the updated `*.pickle.gz`.
- **`redis`/`redis-stack-server` is the RediSearch reference; `valkey-server` + `libsearch.so` is the code under test.** They are different binaries and must both be exercised to make a parity claim — measuring only RediSearch tells you the target, not whether valkey-search hits it.
- **RESP version matters for reply shape.** RediSearch replies differ between RESP2 and RESP3 (arrays vs maps). Probe both when the change touches reply format: `redis-cli -3` for RESP3, plain for RESP2.
- **A parity claim is version-scoped.** Always capture `MODULE LIST` output; "RediSearch tolerates X" is incomplete without the version that was tested.
- **Keep the dataset minimal and deterministic.** One or two documents are usually enough to demonstrate a behavior; large datasets only matter for retention/limit bounds (e.g. SORTBY MAX over 10000 docs).

## Common mistakes

- **Reinventing the harness.** Hand-probing a case that `integration/compatibility/` already covers, or could cover — write/extend a generator instead so the check persists.
- **Measuring against the wrong reference.** Using `redis-stack-server` for a case the suite records against `redis:latest`; the two disagree in the spots documented in `known_differences.md §2`.
- **Measuring before classifying.** Skipping `COMPATIBILITY.md` and burning time in Docker on a difference that is an explicit non-goal (most often **error-message wording** — only the semantic condition must match) or a feature valkey-search does not implement.
- Asserting RediSearch behavior from memory or docs instead of measuring it — the docs lag the implementation, and edge cases (unused params, dialect quirks) are exactly where they diverge.
- Recording a parity result without the RediSearch version — the claim can't be reproduced or trusted later.
- Testing only RESP2 when the change affects reply structure — RESP3 shape can differ.
- Leaving the container running after the check.
- Confusing this with `valkey-search-contrib`: that skill builds/tests valkey-search; this one measures the RediSearch reference.

## Quick reference

| Task | Command |
|------|---------|
| Pick the reference engine | read the version-mapping table in `COMPATIBILITY.md` (1.3 → Redis 8 = `redis:latest`) |
| Check the canonical suite first | read `integration/compatibility/known_differences.md`, `unsupported_tests.md`, and the `generate_*.py` for your area |
| Classify the divergence | read `COMPATIBILITY.md` at the valkey-search repo root |
| Regenerate reference pickles | `./integration/compatibility/regenerate.sh` (uses `redis:latest`; commit the updated `*.pickle.gz`) |
| Run the compat suite (per AGENTS.md) | `.devcontainer/run_in_docker.sh ./build.sh --run-integration-tests --debug` |
| Filter to the compat replay | `TEST_PATTERN=compatibility integration/run.sh` |
| Start RediSearch (harness reference) | `RS_IMAGE=redis:latest scripts/redisearch-docker.sh up` |
| Start redis-stack (reference disagreement only) | `RS_IMAGE=redis/redis-stack-server:7.4.0-v1 scripts/redisearch-docker.sh up` |
| One-off command | `scripts/redisearch-docker.sh cli FT.SEARCH idx '*'` |
| Interactive shell | `scripts/redisearch-docker.sh cli` |
| RESP3 reply shape | `scripts/redisearch-docker.sh cli -3 FT.SEARCH idx '*'` |
| Capture module version | `scripts/redisearch-docker.sh cli MODULE LIST` |
| Tear down | `scripts/redisearch-docker.sh down` |
