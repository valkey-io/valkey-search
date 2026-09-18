---
name: redisearch-compat-testing
description: Establish RediSearch's actual behavior as the parity reference for valkey-search work by running RediSearch in Docker and probing it with real FT.* commands. Use when a valkey-search change hinges on "what does RediSearch actually do here?" — unused/undefined PARAMS, DIALECT differences, SORTBY/LIMIT/MAX retention, ADDSCORES and score semantics, reply-format shape, FT.SEARCH vs FT.AGGREGATE differences, or any VALKEY_SEARCH_COMPATIBILITY_FIX decision — and you need a ground-truth measurement rather than an assumption.
---

# RediSearch Compatibility Testing

## Overview

`valkey-io/valkey-search` is a RediSearch-compatible module: its entire query surface (FT.SEARCH, FT.AGGREGATE, PARAMS, DIALECT, reply formats, scoring) is defined *relative to RediSearch*. Whenever a change turns on "what does RediSearch actually do in this case?", the answer must be **measured against real RediSearch**, not assumed — the repo even bakes RediSearch-measured constants into code via `VALKEY_SEARCH_COMPATIBILITY_FIX(...)`. This skill runs RediSearch (the `redis-stack-server` image) in a throwaway Docker container and probes it with the exact command shapes under question, so the parity target is a fact you can cite in the commit body and PR description.

## When to use

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

## Core workflow

0. **First consult the repo's `COMPATIBILITY.md`** (at the valkey-search repo root). It is the authoritative definition of *what parity means* and decides whether an observed difference is even a bug before you spend effort measuring it:
   - **Expected-compatibility areas** (command/argument syntax, query language and semantics, reply shapes, index types) — an observable difference here **is a bug**. Measure it and treat the RediSearch behavior as the target.
   - **Non-goals** — differences here are **expected, not bugs**: binary/RDB/replication format, internal/source-level parity, performance, **exact error-message text** (only the *semantic* error condition must align), and the stricter Valkey Search security/ACL model.
   - The contract applies **only to features valkey-search actually implements**; an error for an unsupported RediSearch feature is intended behavior, not a defect. Valkey Search may also *exceed* RediSearch (see its Extensions section) and accept combinations RediSearch rejects.

   So: classify the divergence against `COMPATIBILITY.md` first. If it falls under a non-goal (e.g. you are only comparing error *wording*), it is out of scope and Docker measurement is unnecessary. If it is an expected-compatibility area, proceed to measure.

1. **Start RediSearch in Docker.** Use `redis/redis-stack-server` (ships the RediSearch module). The helper script handles pull, run, readiness-wait, and port selection:

   ```bash
   scripts/redisearch-docker.sh up          # start; prints the container name + port
   scripts/redisearch-docker.sh cli         # drop into redis-cli against it
   scripts/redisearch-docker.sh down         # stop and remove the container
   ```

2. **Pin the version you measured.** RediSearch behavior can change across releases, so a parity claim is only meaningful with a version attached. The script pins a tag (default `redis/redis-stack-server:latest`; override with `RS_IMAGE`). Record the resolved module version:

   ```bash
   scripts/redisearch-docker.sh cli FT._LIST                  # sanity: module loaded
   scripts/redisearch-docker.sh cli MODULE LIST               # capture search module version
   ```

3. **Reproduce the exact command shape under question.** Build the smallest index + dataset that exercises the behavior, then run the exact FT.* command the valkey-search code path parses. Use `scripts/redisearch-docker.sh cli <ARGS...>` for one-offs or pipe a script into `... cli` on stdin. See `references/probe-recipes.md` for ready-made probes (unused PARAMS, DIALECT, SORTBY/LIMIT, ADDSCORES, RESP3).

4. **Diff against valkey-search.** Run the same command shape against a local `valkey-server` loading `libsearch.so` (see `valkey-search-contrib`) and compare replies byte-for-byte. Where they differ, that gap is the bug (or the intended divergence).

5. **Document the measurement.** Put the RediSearch version, the exact commands, and the observed reply in the commit body and PR description, and state which `COMPATIBILITY.md` category the divergence falls under (expected-compatibility bug vs. non-goal). A parity claim without the reproducing commands is not verifiable.

6. **Tear down.** `scripts/redisearch-docker.sh down` — the container is disposable; never leave it running.

## Key facts

- **`redis-stack-server` is the RediSearch reference; `valkey-server` + `libsearch.so` is the code under test.** They are different binaries and must both be exercised to make a parity claim — measuring only RediSearch tells you the target, not whether valkey-search hits it.
- **RESP version matters for reply shape.** RediSearch replies differ between RESP2 and RESP3 (arrays vs maps). Probe both when the change touches reply format: `redis-cli -3` for RESP3, plain for RESP2.
- **A parity claim is version-scoped.** Always capture `MODULE LIST` output; "RediSearch tolerates X" is incomplete without the version that was tested.
- **Keep the dataset minimal and deterministic.** One or two documents are usually enough to demonstrate a behavior; large datasets only matter for retention/limit bounds (e.g. SORTBY MAX over 10000 docs).

## Common mistakes

- **Measuring before classifying.** Skipping `COMPATIBILITY.md` and burning time in Docker on a difference that is an explicit non-goal (most often **error-message wording** — only the semantic condition must match) or a feature valkey-search does not implement.
- Asserting RediSearch behavior from memory or docs instead of measuring it — the docs lag the implementation, and edge cases (unused params, dialect quirks) are exactly where they diverge.
- Recording a parity result without the RediSearch version — the claim can't be reproduced or trusted later.
- Testing only RESP2 when the change affects reply structure — RESP3 shape can differ.
- Leaving the container running after the check.
- Confusing this with `valkey-search-contrib`: that skill builds/tests valkey-search; this one measures the RediSearch reference.

## Quick reference

| Task | Command |
|------|---------|
| Classify the divergence first | read `COMPATIBILITY.md` at the valkey-search repo root |
| Start RediSearch | `scripts/redisearch-docker.sh up` |
| Pin a specific version | `RS_IMAGE=redis/redis-stack-server:7.4.0-v1 scripts/redisearch-docker.sh up` |
| One-off command | `scripts/redisearch-docker.sh cli FT.SEARCH idx '*'` |
| Interactive shell | `scripts/redisearch-docker.sh cli` |
| RESP3 reply shape | `scripts/redisearch-docker.sh cli -3 FT.SEARCH idx '*'` |
| Capture module version | `scripts/redisearch-docker.sh cli MODULE LIST` |
| Tear down | `scripts/redisearch-docker.sh down` |
