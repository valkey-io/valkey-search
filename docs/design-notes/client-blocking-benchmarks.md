# Client blocking: before and after the PR

Measured on 2026-10-29 for PR #1640. **Before** is the PR parent `6d1eb26`. **After** is the final implementation. Intermediate builds and diagnostic experiments are in the appendices.

## What was tested

The benchmark runs `HSET`, `JSON.SET`, `DEL`, `UNLINK`, and `EXPIRE` with 1, 2, 5, and 10 index schemas. It covers tracked keys, untracked keys, and missing keys. `MULTI/EXEC` workloads update distinct keys, update the same key repeatedly, or finish with an ordinary `HSET`.

Single-client runs measure time until replies arrive. Pipelined runs measure throughput, including the final index drain. The primary comparison validates 19.6 million operations with no response or final-index errors.

## Overall results

For `MULTI`, one `HSET`, `EXEC`, then another `HSET`, average p50 reply latency changes by at most 2.0%. Average p99 changes between −3.2% and +10.9%.

Tracked `HSET` throughput changes between −0.6% and +1.4%. Deleting eight untracked STRING keys is 4.6 to 5.0 times faster. Other HASH workloads vary by schema count.

`JSON.SET` throughput falls 7.2% with five schemas and 3.6% with ten. The cause remains unresolved. The final implementation improves untracked deletion but does not improve every workload.

## Commands and measured units

The workload names in the tables refer to these exact command shapes. `<value>` is the text payload for that update. Setup creates indexes and seeds existing keys before timing.

| Workload | Commands in one measured operation | Key state |
|---|---|---|
| `hset_tracked` | `HSET <key> content <value>` | Existing indexed HASH |
| `hset_new` | `HSET <new-key> content <value>` | New HASH matching the index prefix |
| `hset_outside` | `HSET <key> content <value>` | HASH outside the index prefix |
| `json_tracked` | `JSON.SET <key> $ <json-document>` | Existing indexed JSON document with a `content` field |
| `expire_string` | `EXPIRE <key> <seconds>` | Untracked STRING under the index prefix. The test sets a TTL and does not wait for expiration. |
| `del_tracked_1` | `DEL <key>` | One indexed HASH |
| `del_tracked_8` | `DEL <key1> ... <key8>` | Eight indexed HASH keys |
| `del_string_8` | `DEL <key1> ... <key8>` | Eight untracked STRING keys under the index prefix |
| `del_missing_8` | `DEL <key1> ... <key8>` | Eight missing keys |
| `unlink_tracked_8` | `UNLINK <key1> ... <key8>` | Eight indexed HASH keys |
| `multi_distinct` | `MULTI` → four `HSET`s on four distinct keys → `EXEC` | Existing indexed HASH keys |
| `multi_duplicate` | `MULTI` → four `HSET`s on the same key → `EXEC` | Existing indexed HASH |
| `multi_then_normal` | `MULTI` → one `HSET` → `EXEC` → one ordinary `HSET` on the same key | Existing indexed HASH |

One `multi_then_normal` operation sends all four commands together and waits for all four replies:

```text
MULTI
HSET doc:example content <transaction-value>
EXEC
HSET doc:example content <ordinary-value>
```

The other transaction workloads send `MULTI`, four `HSET`s, and `EXEC` together. Each complete sequence counts as one operation. A deletion of eight keys also counts as one operation, not eight.

The two configurations measure different units:

- **Single-client:** one client sends one command or complete sequence, then waits for every reply. The latency timer covers that whole exchange.
- **Pipelined:** eight clients each send 16 commands or complete sequences at once, then wait for every reply. The latency timer covers a batch of 16 operations.

After the load finishes, the runner checks every schema with these commands:

```text
FT.INFO <index>
FT.SEARCH <index> * NOCONTENT LIMIT 0 0
FT.INFO <index>
```

The runner repeats the final `FT.INFO` until the mutation queue is empty and the schema is ready.

Reply latency excludes that final drain. Throughput includes the time to receive all load replies and finish the drain. Final value and search-marker checks run afterward, outside both timers.

## Averages and test configuration

Every throughput and latency table reports the arithmetic average of five independent runs per build. This revision uses averages instead of the earlier report's medians. The underlying runs are unchanged.

Each run produces a p50 and a p99 reply latency. The report averages those per-run values. It does not pool all latency samples. Within a run, p50 is the median reply latency and p99 is the 99th percentile.

Change is `(after / before - 1) × 100%`. Positive changes mean higher throughput or longer latency. Higher throughput is better. Lower latency is better. Latency is in microseconds, abbreviated µs.

The primary comparison has 680 timed trials, 68 workload-and-schema comparisons, and 19,578,880 logical operations. Build order alternates between runs.

Release builds use `-O3 -DNDEBUG`, four writer threads, and two reader threads. The ARM64 host has 32 logical CPUs. Servers use CPUs 0 through 5. The generator uses CPUs 6 through 13. Persistence is disabled. Module arguments are loaded through temporary configuration files.

Each trial starts after backfill completes and the mutation queues empty. Validation checks exact replies, final HASH and JSON values, deletion counts, document counts in each schema, searchable final-update markers, and empty final queues. Optional blocked-client INFO metrics were unavailable.

## MULTI/EXEC followed by HSET

This section measures `multi_then_normal`, the four-command sequence above. These values are not the latency of the final `HSET` alone.

Before and after the PR, an ordinary write that joins a deferred transaction record replaces the pending data without waiting for indexing. Both implementations retain `ProcessMultiQueue` as the query barrier.

Average reply latency for the whole sequence, in µs. Before is pre-PR. After is the final PR implementation.

| Schemas | Before p50 | After p50 | Change | Before p99 | After p99 | Change |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 26.13 | 26.65 | +2.0% | 94.38 | 104.64 | +10.9% |
| 2 | 30.02 | 29.74 | -0.9% | 160.97 | 155.84 | -3.2% |
| 5 | 40.10 | 40.26 | +0.4% | 347.92 | 349.82 | +0.5% |
| 10 | 58.31 | 58.87 | +1.0% | 686.38 | 697.64 | +1.6% |

Average complete sequences per second. Before is pre-PR. After is the final PR implementation.

| Schemas | Before | After | Change |
|---:|---:|---:|---:|
| 1 | 71,843 | 72,355 | +0.7% |
| 2 | 42,761 | 41,447 | -3.1% |
| 5 | 18,564 | 18,610 | +0.3% |
| 10 | 9,441 | 9,681 | +2.5% |

## JSON.SET throughput

The operation replaces the JSON document. Every schema indexes its `content` field.

Average commands per second. Before is pre-PR. After is the final PR implementation.

| Schemas | Before | After | Change |
|---:|---:|---:|---:|
| 1 | 52,001 | 53,099 | +2.1% |
| 2 | 39,349 | 38,763 | -1.5% |
| 5 | 27,846 | 25,836 | -7.2% |
| 10 | 18,065 | 17,413 | -3.6% |

## DEL of eight untracked STRING keys

The command is `DEL <key1> ... <key8>`. After the PR, ingestion skips keys with neither indexed nor pending tracked state.

Average commands per second. Before is pre-PR. After is the final PR implementation.

| Schemas | Before | After | Change |
|---:|---:|---:|---:|
| 1 | 21,770 | 107,788 | +395.1% |
| 2 | 16,487 | 78,273 | +374.8% |
| 5 | 9,452 | 43,326 | +358.4% |
| 10 | 5,446 | 25,822 | +374.2% |

## Verification and limits

The final renamed source passes 174 core tests, 392 relevant index tests, three SDK ownership tests, and 37 focused integrations. Formatting and changed-line clang-tidy pass. Four pre-existing distance tests are excluded. The same behavior passed 49 legacy integrations before the identifier-only rename.

The final full Python suite has not been rerun. The preceding snapshot had 984 passes, 11 skips, and one unresolved noeviction failure. That failure did not reproduce in 15 isolated runs.

These measurements use modest text documents on loopback. They exclude replication, persistence, remote-network delays, and large vector updates. The averages describe these runs, not a production performance guarantee.

## Full before-and-after timing tables

Before is the pre-PR parent `6d1eb26`. After is the final PR implementation.

### Pipelined throughput

Values are average commands per second, or complete sequences per second for the three `multi_` workloads. Timing includes the final index drain.

| Workload | Schemas | Before | After | Change |
|---|---:|---:|---:|---:|
| `del_missing_8` | 1 | 740,981 | 742,772 | +0.2% |
| `del_missing_8` | 2 | 741,803 | 742,591 | +0.1% |
| `del_missing_8` | 5 | 740,626 | 739,094 | -0.2% |
| `del_missing_8` | 10 | 732,673 | 731,609 | -0.1% |
| `del_string_8` | 1 | 21,770 | 107,788 | +395.1% |
| `del_string_8` | 2 | 16,487 | 78,273 | +374.8% |
| `del_string_8` | 5 | 9,452 | 43,326 | +358.4% |
| `del_string_8` | 10 | 5,446 | 25,822 | +374.2% |
| `del_tracked_1` | 1 | 71,496 | 72,924 | +2.0% |
| `del_tracked_1` | 2 | 61,158 | 61,214 | +0.1% |
| `del_tracked_1` | 5 | 48,972 | 49,573 | +1.2% |
| `del_tracked_1` | 10 | 35,543 | 36,500 | +2.7% |
| `del_tracked_8` | 1 | 19,096 | 19,210 | +0.6% |
| `del_tracked_8` | 2 | 15,137 | 16,095 | +6.3% |
| `del_tracked_8` | 5 | 10,345 | 10,902 | +5.4% |
| `del_tracked_8` | 10 | 6,331 | 6,482 | +2.4% |
| `expire_string` | 1 | 471,692 | 465,374 | -1.3% |
| `expire_string` | 2 | 385,744 | 386,217 | +0.1% |
| `expire_string` | 5 | 251,477 | 251,586 | 0.0% |
| `expire_string` | 10 | 164,555 | 163,645 | -0.6% |
| `hset_new` | 1 | 59,621 | 64,561 | +8.3% |
| `hset_new` | 2 | 53,087 | 53,993 | +1.7% |
| `hset_new` | 5 | 38,917 | 40,742 | +4.7% |
| `hset_new` | 10 | 20,496 | 20,397 | -0.5% |
| `hset_outside` | 1 | 629,410 | 629,065 | -0.1% |
| `hset_outside` | 2 | 629,322 | 628,383 | -0.1% |
| `hset_outside` | 5 | 625,260 | 626,469 | +0.2% |
| `hset_outside` | 10 | 621,946 | 620,938 | -0.2% |
| `hset_tracked` | 1 | 70,788 | 71,803 | +1.4% |
| `hset_tracked` | 2 | 64,189 | 65,028 | +1.3% |
| `hset_tracked` | 5 | 34,734 | 34,527 | -0.6% |
| `hset_tracked` | 10 | 17,078 | 17,302 | +1.3% |
| `json_tracked` | 1 | 52,001 | 53,099 | +2.1% |
| `json_tracked` | 2 | 39,349 | 38,763 | -1.5% |
| `json_tracked` | 5 | 27,846 | 25,836 | -7.2% |
| `json_tracked` | 10 | 18,065 | 17,413 | -3.6% |
| `multi_distinct` | 1 | 25,967 | 26,063 | +0.4% |
| `multi_distinct` | 2 | 14,095 | 14,281 | +1.3% |
| `multi_distinct` | 5 | 5,976 | 5,874 | -1.7% |
| `multi_distinct` | 10 | 2,953 | 2,977 | +0.8% |
| `multi_duplicate` | 1 | 53,894 | 52,827 | -2.0% |
| `multi_duplicate` | 2 | 32,506 | 32,922 | +1.3% |
| `multi_duplicate` | 5 | 14,937 | 15,047 | +0.7% |
| `multi_duplicate` | 10 | 7,970 | 7,891 | -1.0% |
| `multi_then_normal` | 1 | 71,843 | 72,355 | +0.7% |
| `multi_then_normal` | 2 | 42,761 | 41,447 | -3.1% |
| `multi_then_normal` | 5 | 18,564 | 18,610 | +0.3% |
| `multi_then_normal` | 10 | 9,441 | 9,681 | +2.5% |
| `unlink_tracked_8` | 1 | 18,984 | 19,290 | +1.6% |
| `unlink_tracked_8` | 2 | 15,430 | 15,928 | +3.2% |
| `unlink_tracked_8` | 5 | 10,520 | 11,119 | +5.7% |
| `unlink_tracked_8` | 10 | 6,393 | 6,634 | +3.8% |

### Pipelined reply latency

Values are average per-run p50 and p99 in µs. The timer covers all replies to 16 commands or complete sequences.

| Workload | Schemas | Before p50 | After p50 | Change | Before p99 | After p99 | Change |
|---|---:|---:|---:|---:|---:|---:|---:|
| `del_missing_8` | 1 | 155.05 | 154.57 | -0.3% | 270.75 | 269.26 | -0.5% |
| `del_missing_8` | 2 | 154.91 | 154.66 | -0.2% | 270.11 | 268.63 | -0.5% |
| `del_missing_8` | 5 | 154.27 | 154.72 | +0.3% | 267.97 | 268.69 | +0.3% |
| `del_missing_8` | 10 | 154.68 | 154.87 | +0.1% | 269.76 | 270.05 | +0.1% |
| `del_string_8` | 1 | 5804.31 | 982.44 | -83.1% | 6310.82 | 1981.27 | -68.6% |
| `del_string_8` | 2 | 7679.26 | 1338.12 | -82.6% | 8352.62 | 2581.10 | -69.1% |
| `del_string_8` | 5 | 13323.56 | 2482.36 | -81.4% | 14380.80 | 4924.33 | -65.8% |
| `del_string_8` | 10 | 23251.96 | 4140.51 | -82.2% | 24731.52 | 7639.87 | -69.1% |
| `del_tracked_1` | 1 | 1759.78 | 1724.36 | -2.0% | 2200.18 | 2201.93 | +0.1% |
| `del_tracked_1` | 2 | 2048.81 | 2049.12 | 0.0% | 2458.54 | 2458.60 | 0.0% |
| `del_tracked_1` | 5 | 2535.97 | 2509.25 | -1.1% | 3297.41 | 3253.70 | -1.3% |
| `del_tracked_1` | 10 | 3467.23 | 3380.97 | -2.5% | 5223.86 | 4463.51 | -14.6% |
| `del_tracked_8` | 1 | 6600.04 | 6543.22 | -0.9% | 7331.88 | 7369.08 | +0.5% |
| `del_tracked_8` | 2 | 8315.27 | 7801.93 | -6.2% | 9569.37 | 8950.33 | -6.5% |
| `del_tracked_8` | 5 | 12068.72 | 11378.92 | -5.7% | 14033.82 | 12966.28 | -7.6% |
| `del_tracked_8` | 10 | 19888.14 | 19386.28 | -2.5% | 21835.06 | 20961.14 | -4.0% |
| `expire_string` | 1 | 240.53 | 242.19 | +0.7% | 441.59 | 459.39 | +4.0% |
| `expire_string` | 2 | 294.86 | 294.19 | -0.2% | 547.69 | 545.81 | -0.3% |
| `expire_string` | 5 | 447.91 | 448.05 | 0.0% | 853.24 | 854.71 | +0.2% |
| `expire_string` | 10 | 681.00 | 684.45 | +0.5% | 1321.35 | 1329.99 | +0.7% |
| `hset_new` | 1 | 1974.00 | 1905.37 | -3.5% | 5805.71 | 3336.93 | -42.5% |
| `hset_new` | 2 | 2311.45 | 2280.35 | -1.3% | 4092.02 | 3887.96 | -5.0% |
| `hset_new` | 5 | 3126.68 | 2967.37 | -5.1% | 5344.68 | 5319.22 | -0.5% |
| `hset_new` | 10 | 6048.97 | 6075.64 | +0.4% | 9221.72 | 8893.50 | -3.6% |
| `hset_outside` | 1 | 181.21 | 181.27 | 0.0% | 332.84 | 331.05 | -0.5% |
| `hset_outside` | 2 | 181.06 | 181.67 | +0.3% | 329.52 | 329.06 | -0.1% |
| `hset_outside` | 5 | 181.74 | 181.42 | -0.2% | 329.44 | 327.68 | -0.5% |
| `hset_outside` | 10 | 181.47 | 181.72 | +0.1% | 327.16 | 329.00 | +0.6% |
| `hset_tracked` | 1 | 1767.06 | 1732.54 | -2.0% | 2561.78 | 2377.58 | -7.2% |
| `hset_tracked` | 2 | 1976.39 | 1946.29 | -1.5% | 2336.45 | 2330.90 | -0.2% |
| `hset_tracked` | 5 | 3648.99 | 3689.89 | +1.1% | 3969.37 | 3858.69 | -2.8% |
| `hset_tracked` | 10 | 7421.52 | 7340.68 | -1.1% | 8183.76 | 8033.74 | -1.8% |
| `json_tracked` | 1 | 2443.22 | 2393.07 | -2.1% | 2803.17 | 2739.30 | -2.3% |
| `json_tracked` | 2 | 3218.82 | 3273.84 | +1.7% | 3777.78 | 3775.69 | -0.1% |
| `json_tracked` | 5 | 4558.94 | 4920.94 | +7.9% | 5268.22 | 5651.97 | +7.3% |
| `json_tracked` | 10 | 7051.18 | 7318.63 | +3.8% | 7370.07 | 7638.85 | +3.6% |
| `multi_distinct` | 1 | 5506.02 | 5375.93 | -2.4% | 7174.87 | 6963.50 | -2.9% |
| `multi_distinct` | 2 | 10065.56 | 9955.64 | -1.1% | 12757.92 | 12765.90 | +0.1% |
| `multi_distinct` | 5 | 23135.76 | 23139.36 | 0.0% | 28922.46 | 29432.40 | +1.8% |
| `multi_distinct` | 10 | 44162.10 | 43815.42 | -0.8% | 55380.44 | 54885.20 | -0.9% |
| `multi_duplicate` | 1 | 2524.77 | 2601.64 | +3.0% | 3478.71 | 3484.16 | +0.2% |
| `multi_duplicate` | 2 | 4284.86 | 4179.73 | -2.5% | 5619.45 | 5328.65 | -5.2% |
| `multi_duplicate` | 5 | 9210.31 | 9280.89 | +0.8% | 12001.62 | 11726.02 | -2.3% |
| `multi_duplicate` | 10 | 16763.34 | 16967.38 | +1.2% | 20991.12 | 21484.16 | +2.3% |
| `multi_then_normal` | 1 | 1845.46 | 1734.52 | -6.0% | 2788.97 | 2738.12 | -1.8% |
| `multi_then_normal` | 2 | 3512.54 | 3494.87 | -0.5% | 4550.65 | 5174.69 | +13.7% |
| `multi_then_normal` | 5 | 7626.87 | 7767.83 | +1.8% | 11325.18 | 10910.02 | -3.7% |
| `multi_then_normal` | 10 | 14475.36 | 14461.54 | -0.1% | 24132.36 | 22341.46 | -7.4% |
| `unlink_tracked_8` | 1 | 6644.57 | 6512.63 | -2.0% | 7384.59 | 7460.42 | +1.0% |
| `unlink_tracked_8` | 2 | 8147.55 | 7884.36 | -3.2% | 9333.04 | 9149.29 | -2.0% |
| `unlink_tracked_8` | 5 | 11839.26 | 11210.32 | -5.3% | 13337.46 | 12605.22 | -5.5% |
| `unlink_tracked_8` | 10 | 19643.78 | 18959.36 | -3.5% | 21455.18 | 20519.44 | -4.4% |

### Single-client throughput

Values are average commands per second, or complete sequences per second for the three `multi_` workloads. Timing includes the final index drain.

| Workload | Schemas | Before | After | Change |
|---|---:|---:|---:|---:|
| `del_tracked_1` | 1 | 26,068 | 26,344 | +1.1% |
| `del_tracked_1` | 2 | 21,546 | 22,883 | +6.2% |
| `del_tracked_1` | 5 | 17,860 | 18,011 | +0.8% |
| `del_tracked_1` | 10 | 15,096 | 15,505 | +2.7% |
| `hset_tracked` | 1 | 18,831 | 18,803 | -0.1% |
| `hset_tracked` | 2 | 16,486 | 16,550 | +0.4% |
| `hset_tracked` | 5 | 12,300 | 12,320 | +0.2% |
| `hset_tracked` | 10 | 8,409 | 8,619 | +2.5% |
| `json_tracked` | 1 | 16,381 | 15,869 | -3.1% |
| `json_tracked` | 2 | 14,324 | 14,286 | -0.3% |
| `json_tracked` | 5 | 10,690 | 10,980 | +2.7% |
| `json_tracked` | 10 | 7,851 | 7,903 | +0.7% |
| `multi_then_normal` | 1 | 24,932 | 24,191 | -3.0% |
| `multi_then_normal` | 2 | 17,130 | 17,439 | +1.8% |
| `multi_then_normal` | 5 | 9,065 | 9,114 | +0.5% |
| `multi_then_normal` | 10 | 5,028 | 5,018 | -0.2% |

### Single-client reply latency

Values are average per-run p50 and p99 in µs. The timer covers all replies to one command or one complete sequence.

| Workload | Schemas | Before p50 | After p50 | Change | Before p99 | After p99 | Change |
|---|---:|---:|---:|---:|---:|---:|---:|
| `del_tracked_1` | 1 | 36.98 | 36.72 | -0.7% | 51.84 | 48.36 | -6.7% |
| `del_tracked_1` | 2 | 43.81 | 41.99 | -4.2% | 78.24 | 55.77 | -28.7% |
| `del_tracked_1` | 5 | 53.70 | 52.80 | -1.7% | 72.10 | 71.71 | -0.5% |
| `del_tracked_1` | 10 | 62.49 | 60.90 | -2.5% | 83.57 | 81.80 | -2.1% |
| `hset_tracked` | 1 | 51.86 | 51.96 | +0.2% | 63.59 | 62.18 | -2.2% |
| `hset_tracked` | 2 | 58.78 | 58.42 | -0.6% | 74.17 | 79.43 | +7.1% |
| `hset_tracked` | 5 | 78.64 | 78.51 | -0.2% | 97.01 | 97.06 | +0.1% |
| `hset_tracked` | 10 | 114.90 | 112.39 | -2.2% | 147.18 | 138.14 | -6.1% |
| `json_tracked` | 1 | 59.41 | 61.58 | +3.7% | 78.39 | 82.19 | +4.9% |
| `json_tracked` | 2 | 67.69 | 68.13 | +0.6% | 91.25 | 88.04 | -3.5% |
| `json_tracked` | 5 | 90.63 | 88.23 | -2.6% | 119.67 | 115.88 | -3.2% |
| `json_tracked` | 10 | 123.30 | 122.50 | -0.6% | 150.21 | 148.03 | -1.5% |
| `multi_then_normal` | 1 | 26.13 | 26.65 | +2.0% | 94.38 | 104.64 | +10.9% |
| `multi_then_normal` | 2 | 30.02 | 29.74 | -0.9% | 160.97 | 155.84 | -3.2% |
| `multi_then_normal` | 5 | 40.10 | 40.26 | +0.4% | 347.92 | 349.82 | +0.5% |
| `multi_then_normal` | 10 | 58.31 | 58.87 | +1.0% | 686.38 | 697.64 | +1.6% |

## Appendix A: before-sleep cleanup versus the final implementation

This earlier build starts workers when an ordinary write joins deferred transaction work. It releases command registration ownership at before-sleep. The final implementation preserves deferred work and releases registration ownership at execution-unit end.

Before is the earlier before-sleep build. After is the final PR implementation.

### Pipelined throughput

Values are average commands per second, or complete sequences per second for the three `multi_` workloads. Timing includes the final index drain.

| Workload | Schemas | Before | After | Change |
|---|---:|---:|---:|---:|
| `del_missing_8` | 1 | 740,795 | 743,180 | +0.3% |
| `del_missing_8` | 2 | 742,656 | 743,187 | +0.1% |
| `del_missing_8` | 5 | 731,487 | 737,796 | +0.9% |
| `del_missing_8` | 10 | 730,693 | 732,775 | +0.3% |
| `del_string_8` | 1 | 22,865 | 108,563 | +374.8% |
| `del_string_8` | 2 | 16,899 | 77,928 | +361.1% |
| `del_string_8` | 5 | 9,642 | 43,323 | +349.3% |
| `del_string_8` | 10 | 5,396 | 25,822 | +378.6% |
| `del_tracked_1` | 1 | 74,877 | 71,801 | -4.1% |
| `del_tracked_1` | 2 | 63,444 | 63,601 | +0.2% |
| `del_tracked_1` | 5 | 50,391 | 50,164 | -0.5% |
| `del_tracked_1` | 10 | 37,154 | 36,513 | -1.7% |
| `del_tracked_8` | 1 | 20,111 | 19,507 | -3.0% |
| `del_tracked_8` | 2 | 16,785 | 16,129 | -3.9% |
| `del_tracked_8` | 5 | 11,478 | 10,922 | -4.8% |
| `del_tracked_8` | 10 | 6,529 | 6,652 | +1.9% |
| `expire_string` | 1 | 482,735 | 474,447 | -1.7% |
| `expire_string` | 2 | 392,157 | 386,900 | -1.3% |
| `expire_string` | 5 | 252,929 | 251,052 | -0.7% |
| `expire_string` | 10 | 164,446 | 163,762 | -0.4% |
| `hset_new` | 1 | 66,173 | 64,866 | -2.0% |
| `hset_new` | 2 | 57,675 | 53,023 | -8.1% |
| `hset_new` | 5 | 40,357 | 40,692 | +0.8% |
| `hset_new` | 10 | 21,095 | 20,413 | -3.2% |
| `hset_outside` | 1 | 632,868 | 629,026 | -0.6% |
| `hset_outside` | 2 | 631,671 | 627,732 | -0.6% |
| `hset_outside` | 5 | 628,347 | 625,771 | -0.4% |
| `hset_outside` | 10 | 623,847 | 619,024 | -0.8% |
| `hset_tracked` | 1 | 79,808 | 73,932 | -7.4% |
| `hset_tracked` | 2 | 66,399 | 65,707 | -1.0% |
| `hset_tracked` | 5 | 34,402 | 34,128 | -0.8% |
| `hset_tracked` | 10 | 17,712 | 17,329 | -2.2% |
| `json_tracked` | 1 | 53,009 | 52,819 | -0.4% |
| `json_tracked` | 2 | 39,869 | 38,903 | -2.4% |
| `json_tracked` | 5 | 28,029 | 27,074 | -3.4% |
| `json_tracked` | 10 | 17,531 | 17,807 | +1.6% |
| `multi_distinct` | 1 | 25,782 | 25,937 | +0.6% |
| `multi_distinct` | 2 | 14,350 | 13,968 | -2.7% |
| `multi_distinct` | 5 | 5,862 | 5,948 | +1.5% |
| `multi_distinct` | 10 | 2,983 | 2,923 | -2.0% |
| `multi_duplicate` | 1 | 53,531 | 54,150 | +1.2% |
| `multi_duplicate` | 2 | 32,573 | 33,076 | +1.5% |
| `multi_duplicate` | 5 | 15,023 | 15,393 | +2.5% |
| `multi_duplicate` | 10 | 7,997 | 7,957 | -0.5% |
| `multi_then_normal` | 1 | 44,261 | 73,354 | +65.7% |
| `multi_then_normal` | 2 | 32,641 | 43,124 | +32.1% |
| `multi_then_normal` | 5 | 21,007 | 19,441 | -7.5% |
| `multi_then_normal` | 10 | 11,960 | 9,992 | -16.5% |
| `unlink_tracked_8` | 1 | 19,889 | 19,329 | -2.8% |
| `unlink_tracked_8` | 2 | 16,207 | 16,430 | +1.4% |
| `unlink_tracked_8` | 5 | 11,414 | 11,244 | -1.5% |
| `unlink_tracked_8` | 10 | 6,593 | 6,598 | +0.1% |

### Pipelined reply latency

Values are average per-run p50 and p99 in µs. The timer covers all replies to 16 commands or complete sequences.

| Workload | Schemas | Before p50 | After p50 | Change | Before p99 | After p99 | Change |
|---|---:|---:|---:|---:|---:|---:|---:|
| `del_missing_8` | 1 | 155.37 | 154.80 | -0.4% | 269.30 | 270.01 | +0.3% |
| `del_missing_8` | 2 | 154.72 | 154.65 | 0.0% | 268.44 | 267.34 | -0.4% |
| `del_missing_8` | 5 | 156.26 | 154.88 | -0.9% | 271.15 | 269.14 | -0.7% |
| `del_missing_8` | 10 | 155.00 | 154.69 | -0.2% | 269.77 | 268.44 | -0.5% |
| `del_string_8` | 1 | 5494.76 | 964.55 | -82.4% | 5935.99 | 1936.69 | -67.4% |
| `del_string_8` | 2 | 7409.42 | 1347.91 | -81.8% | 8133.91 | 2702.30 | -66.8% |
| `del_string_8` | 5 | 12985.02 | 2475.03 | -80.9% | 13962.16 | 4839.69 | -65.3% |
| `del_string_8` | 10 | 23229.16 | 4144.08 | -82.2% | 25016.98 | 7917.33 | -68.4% |
| `del_tracked_1` | 1 | 1668.95 | 1752.87 | +5.0% | 2067.46 | 2201.76 | +6.5% |
| `del_tracked_1` | 2 | 1972.83 | 1966.69 | -0.3% | 2369.89 | 2386.88 | +0.7% |
| `del_tracked_1` | 5 | 2448.69 | 2486.30 | +1.5% | 3098.88 | 3105.44 | +0.2% |
| `del_tracked_1` | 10 | 3310.68 | 3388.45 | +2.3% | 4012.93 | 4162.58 | +3.7% |
| `del_tracked_8` | 1 | 6235.01 | 6447.55 | +3.4% | 6871.35 | 7187.92 | +4.6% |
| `del_tracked_8` | 2 | 7446.34 | 7761.04 | +4.2% | 8544.44 | 9007.31 | +5.4% |
| `del_tracked_8` | 5 | 10784.26 | 11399.26 | +5.7% | 11999.60 | 13346.94 | +11.2% |
| `del_tracked_8` | 10 | 18918.38 | 18891.84 | -0.1% | 22821.68 | 20880.42 | -8.5% |
| `expire_string` | 1 | 236.19 | 240.55 | +1.8% | 433.24 | 441.14 | +1.8% |
| `expire_string` | 2 | 289.32 | 293.59 | +1.5% | 539.23 | 545.60 | +1.2% |
| `expire_string` | 5 | 444.67 | 448.50 | +0.9% | 850.49 | 855.92 | +0.6% |
| `expire_string` | 10 | 681.41 | 684.36 | +0.4% | 1324.55 | 1329.98 | +0.4% |
| `hset_new` | 1 | 1847.50 | 1890.95 | +2.4% | 3129.72 | 3549.70 | +13.4% |
| `hset_new` | 2 | 2099.60 | 2321.47 | +10.6% | 3972.77 | 3915.83 | -1.4% |
| `hset_new` | 5 | 3016.47 | 2973.31 | -1.4% | 5366.73 | 5395.07 | +0.5% |
| `hset_new` | 10 | 5868.80 | 6067.80 | +3.4% | 8532.60 | 9449.67 | +10.7% |
| `hset_outside` | 1 | 180.69 | 181.69 | +0.6% | 327.02 | 329.13 | +0.6% |
| `hset_outside` | 2 | 180.47 | 181.87 | +0.8% | 326.34 | 328.17 | +0.6% |
| `hset_outside` | 5 | 180.73 | 181.52 | +0.4% | 326.66 | 329.68 | +0.9% |
| `hset_outside` | 10 | 180.72 | 181.82 | +0.6% | 329.05 | 331.97 | +0.9% |
| `hset_tracked` | 1 | 1583.38 | 1716.37 | +8.4% | 1896.31 | 2038.19 | +7.5% |
| `hset_tracked` | 2 | 1904.91 | 1929.90 | +1.3% | 2375.15 | 2297.64 | -3.3% |
| `hset_tracked` | 5 | 3691.36 | 3698.36 | +0.2% | 3943.42 | 4078.83 | +3.4% |
| `hset_tracked` | 10 | 7191.47 | 7350.58 | +2.2% | 7330.91 | 7517.73 | +2.5% |
| `json_tracked` | 1 | 2390.16 | 2401.46 | +0.5% | 2698.66 | 2756.05 | +2.1% |
| `json_tracked` | 2 | 3167.17 | 3263.96 | +3.1% | 3586.66 | 3622.38 | +1.0% |
| `json_tracked` | 5 | 4500.10 | 4689.07 | +4.2% | 5212.52 | 5458.68 | +4.7% |
| `json_tracked` | 10 | 7267.28 | 7145.60 | -1.7% | 7531.25 | 7467.24 | -0.9% |
| `multi_distinct` | 1 | 5455.95 | 5504.53 | +0.9% | 7212.71 | 7115.38 | -1.3% |
| `multi_distinct` | 2 | 9858.71 | 9995.22 | +1.4% | 12558.22 | 12757.26 | +1.6% |
| `multi_distinct` | 5 | 23586.24 | 23762.42 | +0.7% | 30050.78 | 29646.60 | -1.3% |
| `multi_distinct` | 10 | 44309.14 | 45164.80 | +1.9% | 56173.72 | 55744.30 | -0.8% |
| `multi_duplicate` | 1 | 2557.88 | 2382.00 | -6.9% | 3491.42 | 3454.52 | -1.1% |
| `multi_duplicate` | 2 | 4351.36 | 4261.84 | -2.1% | 5718.04 | 5573.66 | -2.5% |
| `multi_duplicate` | 5 | 9418.79 | 9233.23 | -2.0% | 11921.48 | 11692.54 | -1.9% |
| `multi_duplicate` | 10 | 17524.94 | 17542.94 | +0.1% | 21556.84 | 22026.80 | +2.2% |
| `multi_then_normal` | 1 | 2854.16 | 1841.16 | -35.5% | 3912.29 | 2662.95 | -31.9% |
| `multi_then_normal` | 2 | 3881.45 | 3247.07 | -16.3% | 4507.79 | 4601.31 | +2.1% |
| `multi_then_normal` | 5 | 6012.40 | 7203.84 | +19.8% | 6637.77 | 9855.93 | +48.5% |
| `multi_then_normal` | 10 | 10616.80 | 14629.92 | +37.8% | 11170.08 | 18777.92 | +68.1% |
| `unlink_tracked_8` | 1 | 6318.21 | 6502.10 | +2.9% | 6860.83 | 7339.28 | +7.0% |
| `unlink_tracked_8` | 2 | 7729.60 | 7645.42 | -1.1% | 8897.58 | 8737.84 | -1.8% |
| `unlink_tracked_8` | 5 | 10852.98 | 11031.26 | +1.6% | 12285.44 | 12716.94 | +3.5% |
| `unlink_tracked_8` | 10 | 18957.74 | 19061.24 | +0.5% | 21941.46 | 20989.26 | -4.3% |

### Single-client throughput

Values are average commands per second, or complete sequences per second for the three `multi_` workloads. Timing includes the final index drain.

| Workload | Schemas | Before | After | Change |
|---|---:|---:|---:|---:|
| `del_tracked_1` | 1 | 26,481 | 26,453 | -0.1% |
| `del_tracked_1` | 2 | 22,264 | 22,654 | +1.8% |
| `del_tracked_1` | 5 | 17,917 | 18,130 | +1.2% |
| `del_tracked_1` | 10 | 15,306 | 15,634 | +2.1% |
| `hset_tracked` | 1 | 18,810 | 18,792 | -0.1% |
| `hset_tracked` | 2 | 16,495 | 16,740 | +1.5% |
| `hset_tracked` | 5 | 12,428 | 12,389 | -0.3% |
| `hset_tracked` | 10 | 8,726 | 8,737 | +0.1% |
| `json_tracked` | 1 | 16,452 | 16,254 | -1.2% |
| `json_tracked` | 2 | 14,306 | 14,291 | -0.1% |
| `json_tracked` | 5 | 10,965 | 10,888 | -0.7% |
| `json_tracked` | 10 | 7,939 | 7,663 | -3.5% |
| `multi_then_normal` | 1 | 15,835 | 24,674 | +55.8% |
| `multi_then_normal` | 2 | 13,304 | 17,357 | +30.5% |
| `multi_then_normal` | 5 | 8,836 | 9,085 | +2.8% |
| `multi_then_normal` | 10 | 5,853 | 5,068 | -13.4% |

### Single-client reply latency

Values are average per-run p50 and p99 in µs. The timer covers all replies to one command or one complete sequence.

| Workload | Schemas | Before p50 | After p50 | Change | Before p99 | After p99 | Change |
|---|---:|---:|---:|---:|---:|---:|---:|
| `del_tracked_1` | 1 | 36.62 | 36.65 | +0.1% | 46.46 | 47.27 | +1.7% |
| `del_tracked_1` | 2 | 43.75 | 42.52 | -2.8% | 57.49 | 53.68 | -6.6% |
| `del_tracked_1` | 5 | 53.29 | 52.82 | -0.9% | 73.31 | 70.46 | -3.9% |
| `del_tracked_1` | 10 | 61.82 | 60.45 | -2.2% | 77.37 | 77.19 | -0.2% |
| `hset_tracked` | 1 | 51.77 | 51.66 | -0.2% | 67.13 | 71.03 | +5.8% |
| `hset_tracked` | 2 | 58.49 | 58.00 | -0.8% | 78.18 | 72.25 | -7.6% |
| `hset_tracked` | 5 | 77.67 | 77.95 | +0.4% | 97.83 | 93.79 | -4.1% |
| `hset_tracked` | 10 | 111.18 | 111.43 | +0.2% | 130.14 | 128.13 | -1.5% |
| `json_tracked` | 1 | 59.56 | 60.23 | +1.1% | 73.16 | 74.13 | +1.3% |
| `json_tracked` | 2 | 68.33 | 68.48 | +0.2% | 87.01 | 83.30 | -4.3% |
| `json_tracked` | 5 | 89.12 | 89.16 | 0.0% | 107.81 | 113.40 | +5.2% |
| `json_tracked` | 10 | 122.13 | 125.79 | +3.0% | 146.05 | 151.98 | +4.1% |
| `multi_then_normal` | 1 | 61.04 | 26.32 | -56.9% | 81.63 | 96.68 | +18.4% |
| `multi_then_normal` | 2 | 69.69 | 29.92 | -57.1% | 96.60 | 159.68 | +65.3% |
| `multi_then_normal` | 5 | 96.59 | 40.29 | -58.3% | 172.49 | 349.68 | +102.7% |
| `multi_then_normal` | 10 | 139.25 | 58.53 | -58.0% | 284.55 | 666.45 | +134.2% |

## Appendix B: eager waiting versus the final implementation

This earlier build uses execution-unit cleanup but makes an ordinary write wait when it joins deferred transaction work. The final implementation removes that wait.

For the complete `MULTI`, `HSET`, `EXEC`, `HSET` sequence, removing the wait lowers p50 reply latency. It increases p99 reply latency and reduces pipelined throughput at five and ten schemas. These are comparisons with an intermediate implementation, not regressions against pre-PR.

Before is the earlier eager-wait build. After is the final PR implementation.

### Pipelined throughput

Values are average commands per second, or complete sequences per second for the three `multi_` workloads. Timing includes the final index drain.

| Workload | Schemas | Before | After | Change |
|---|---:|---:|---:|---:|
| `hset_tracked` | 1 | 73,791 | 72,906 | -1.2% |
| `multi_then_normal` | 1 | 44,357 | 73,793 | +66.4% |
| `multi_then_normal` | 2 | 31,411 | 42,945 | +36.7% |
| `multi_then_normal` | 5 | 19,935 | 19,201 | -3.7% |
| `multi_then_normal` | 10 | 11,661 | 9,997 | -14.3% |

### Pipelined reply latency

Values are average per-run p50 and p99 in µs. The timer covers all replies to 16 commands or complete sequences.

| Workload | Schemas | Before p50 | After p50 | Change | Before p99 | After p99 | Change |
|---|---:|---:|---:|---:|---:|---:|---:|
| `hset_tracked` | 1 | 1716.80 | 1739.70 | +1.3% | 2030.01 | 2054.95 | +1.2% |
| `multi_then_normal` | 1 | 2841.13 | 1940.40 | -31.7% | 3320.13 | 2697.45 | -18.8% |
| `multi_then_normal` | 2 | 4036.44 | 3428.12 | -15.1% | 4550.74 | 4477.60 | -1.6% |
| `multi_then_normal` | 5 | 6364.17 | 7751.05 | +21.8% | 6904.51 | 10049.66 | +45.6% |
| `multi_then_normal` | 10 | 10904.64 | 14578.98 | +33.7% | 11499.58 | 19326.10 | +68.1% |

### Single-client throughput

Values are average commands per second, or complete sequences per second for the three `multi_` workloads. Timing includes the final index drain.

| Workload | Schemas | Before | After | Change |
|---|---:|---:|---:|---:|
| `multi_then_normal` | 1 | 15,855 | 24,407 | +53.9% |
| `multi_then_normal` | 2 | 13,357 | 17,217 | +28.9% |
| `multi_then_normal` | 5 | 8,861 | 9,054 | +2.2% |
| `multi_then_normal` | 10 | 5,854 | 4,984 | -14.9% |

### Single-client reply latency

Values are average per-run p50 and p99 in µs. The timer covers all replies to one command or one complete sequence.

| Workload | Schemas | Before p50 | After p50 | Change | Before p99 | After p99 | Change |
|---|---:|---:|---:|---:|---:|---:|---:|
| `multi_then_normal` | 1 | 60.95 | 26.67 | -56.2% | 80.79 | 100.30 | +24.1% |
| `multi_then_normal` | 2 | 69.69 | 29.92 | -57.1% | 100.30 | 161.76 | +61.3% |
| `multi_then_normal` | 5 | 96.38 | 40.25 | -58.2% | 171.64 | 354.45 | +106.5% |
| `multi_then_normal` | 10 | 139.65 | 58.87 | -57.8% | 281.28 | 686.25 | +144.0% |

## Appendix C: diagnostic JSON.SET experiments with five schemas

These experiments use the same `JSON.SET` workload. Their averages are separate from the full sweep. Neither diagnostic implementation is part of the PR.

### Cleanup timing

The diagnostic build keeps the final mutation behavior but replaces execution-unit cleanup with before-sleep cleanup. Each build has five runs of 32,768 updates.

| Build | Average commands per second | Average main-thread CPU per update, µs |
|---|---:|---:|
| Pre-PR | 28,277 | 35.22 |
| Final PR | 27,527 | 36.07 |
| Diagnostic before-sleep cleanup | 27,242 | 36.74 |

The final build averages 2.7% less throughput than pre-PR in this follow-up. Before-sleep cleanup does not recover throughput.

### Cost of an otherwise empty post-notification job

Both diagnostic builds release ownership at before-sleep. One adds a post-notification job whose callback does nothing. Each build has five runs of 131,072 updates.

| Build | Average commands per second | Average main-thread CPU per update, µs |
|---|---:|---:|
| Before: no job | 28,203 | 35.40 |
| After: one empty job per notification | 27,231 | 36.68 |

Adding the empty job lowers average throughput by 3.4%. Job overhead is a possible contributor to the regression. Separate linking can change code layout. Both diagnostic builds also retain ownership longer than the final PR. The experiment does not account for the full production regression.

### CPU profiles

Three instruction profiles per build sample the main thread at 997 Hz. Each profile validates 65,536 updates. Profiled runs are excluded from throughput averages.

The main thread consumes approximately one full core. Pre-PR uses 62,655 main-thread cycles per update. The final build uses 63,546, an increase of 1.4%. JSON extraction, allocation, and atomic operations consume substantial CPU time.

Caller-stack unwinding failed, so these profiles do not assign all allocator or atomic costs to their callers. They do not establish that shared ownership or cleanup jobs explain the entire regression. The generator remains well below one core of CPU use.

The diagnostic experiment validates 1,802,240 timed updates with no response or final-index errors. The average results support further investigation but do not identify a complete cause.

## Appendix D: build provenance and raw results

The primary comparison measures final behavior before the identifier-only rename to `ClientBlockingContext` and `AcquireBlock`. The JSON follow-up and final tests use the renamed Release build. The rename changes no measured behavior.

The immutable builds are recorded below:

| Build | Artifact | SHA256 |
|---|---|---|
| Final behavior before identifier rename | `.audit/pr1640-preserve-deferred-libsearch.so` | `6fe877b0610028c9db4a0cc1653b1771ba3f3d80a64963d62ddf2baa8562e70c` |
| Final renamed Release | `.audit/pr1640-client-blocking-context-libsearch.so` | `a96f17ffb1d7e0f65c0fbce8e1e538a0f2127e5f66afab45db536142d407d30d` |

The pre-PR parent is `6d1eb2667d214c4841a8c5c59b249b02bbbdd7b8`. No builds or style checks ran during measurement.

The full sweep, including both intermediate comparisons, contains 1,450 trials, 145 workload-and-schema comparisons, and 39,731,200 validated operations with zero benchmark errors. The JSON diagnostic experiment is separate.

Raw files remain in `.audit/benchmarks/` in the development workspace. Each `preserve-deferred-<baseline>-<mode>.jsonl` file records one result per timed run. Baselines are `pre_pr`, `before_sleep`, and `postjob`. Modes are `macro` for pipelined runs and `micro` for single-client runs.

The measured command builder is `.audit/benchmarks/run_matrix.py`. The reply timer is `.audit/command_load.cc`. The following example regenerates average JSON throughput with five schemas from the retained raw results:

```bash
python3 - <<'PYRESULTS'
import json
import statistics
from pathlib import Path

rows = [json.loads(line) for line in Path(
	".audit/benchmarks/preserve-deferred-pre_pr-macro.jsonl"
).read_text().splitlines()]
values = []
for side in ("before", "after"):
	values.append(statistics.mean(
		r["completed_ops_per_second"] for r in rows
		if r["side"] == side
		and r["workload"] == "json_tracked" and r["schemas"] == 5
	))
print(*(round(value) for value in values))
print(round((values[1] / values[0] - 1) * 100, 1))
PYRESULTS
```

The output is `27846 25836`, followed by `-7.2`.
