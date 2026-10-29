# Client blocking: pre-PR and final benchmark results

Measured on 2026-10-29 for PR #1640. The primary comparison is the PR parent `6d1eb26` against the final implementation's behavior.

## What was tested

The benchmark compares pre-PR and final behavior with 1, 2, 5, and 10 index schemas. Workloads cover HASH and JSON writes, tracked and untracked keys, missing keys, expiration, and single-key and multi-key deletion. Transaction cases use distinct keys, duplicate keys, and an ordinary write after MULTI.

Single-client runs measure p50 and p99 latency. Pipelined runs measure throughput through completed indexing. The primary comparison validates 19.6 million operations with no response or final-index errors.

## Overall results

Transaction latency stays close to pre-PR behavior. HASH-write throughput varies by workload and schema count, with no consistent change across the tracked-write results. Deleting eight untracked STRING keys is 4.6 to 5.0 times faster.

JSON throughput falls 6.6% with five schemas and 5.2% with ten. The cause remains unresolved. The final implementation improves untracked deletion and preserves transaction acknowledgment behavior, but it is not performance-neutral across all workloads.

Intermediate implementations and diagnostic builds appear only in the appendices. Their results explain design choices but are not the baseline for judging the final PR.

## Builds and measurement method

The final implementation uses `ClientBlockingContext`, shared `BlockedClient` ownership, and execution-unit cleanup. Ordinary writes that join deferred transaction records coalesce without acquiring a block or scheduling a worker. Queued and running mutations retain waiting clients until indexing completes.

The full sweep measures this behavior before the identifier-only rename to `ClientBlockingContext` and `AcquireBlock`. The JSON follow-up and final verification use the renamed Release build. Appendix D records the build identifiers and verification history.

The pre-PR comparison covers 13 macro workloads and four single-client workloads at 1, 2, 5, and 10 schemas. Each comparison uses five fresh independent server pairs with alternating execution order. The primary comparison contains **680 timed trials, 68 comparison cells, and 19,578,880 logical operations, with zero benchmark errors**.

Release builds use `-O3 -DNDEBUG`, four writer threads, and two reader threads. The ARM64 host has 32 logical CPUs. Servers use CPUs 0 through 5, and the compiled generator uses CPUs 6 through 13. Persistence is disabled. Module arguments are loaded through temporary configuration files.

Macro runs use eight clients and a pipeline depth of 16. Micro runs use one client and a pipeline depth of one. Completed throughput includes replies and the final indexing drain. Ordinary workloads count commands per second. Transaction workloads count transaction groups per second.

Macro latency measures a batch of 16 operations. Micro latency measures one command or transaction group. Latency is in microseconds, abbreviated µs. Higher throughput and lower latency are better.

Every trial starts with ready schemas, complete backfill, and empty mutation queues. Validation checks exact RESP replies, final HASH and JSON values, deletion counts, document counts in every schema, searchable final-update markers, and empty final queues. Optional blocked-client INFO metrics were unavailable. The measurements do not establish that those counters reach zero.

## Transaction acknowledgment

The final implementation preserves the pre-PR behavior of ordinary writes that join deferred MULTI or Lua work. Both implementations retain `ProcessMultiQueue` as the query barrier.

| Schemas | Pre-PR groups/s | Final groups/s | Change |
|---:|---:|---:|---:|
| 1 | 72,686 | 72,639 | −0.1% |
| 2 | 42,915 | 42,742 | −0.4% |
| 5 | 18,594 | 18,824 | +1.2% |
| 10 | 9,343 | 9,859 | +5.5% |

The ranges overlap at every schema count. These medians do not establish a throughput improvement or regression.

| Schemas | Pre-PR p50 µs | Final p50 µs | Pre-PR p99 µs | Final p99 µs |
|---:|---:|---:|---:|---:|
| 1 | 26.11 | 26.03 | 94.57 | 92.63 |
| 2 | 29.92 | 29.71 | 157.06 | 157.13 |
| 5 | 40.15 | 40.48 | 349.79 | 344.22 |
| 10 | 58.47 | 58.62 | 677.66 | 683.98 |

## JSON throughput regression

| Schemas | Pre-PR commands/s, median [range] | Final commands/s, median [range] | Change |
|---:|---:|---:|---:|
| 5 | 27,783 [27,197–28,793] | 25,938 [24,359–26,837] | −6.6% |
| 10 | 18,191 [17,347–18,410] | 17,251 [16,694–18,107] | −5.2% |

Both JSON cases have lower median throughput. The five-schema ranges do not overlap. The ten-schema ranges overlap.

A later pre-PR comparison with the final renamed build measures a smaller JSON5 loss of 1.4%. Main-thread cycle profiles also measure about 1.4% more cycles per update. The main thread consumes approximately one full core. Job overhead is a plausible contributor, but the full regression is not causally isolated. Appendix C includes the follow-up comparison and diagnostic controls.

## Untracked deletion improvement

The final implementation skips ingestion for keys with neither indexed nor pending tracked state. This reduces the work required for an eight-key untracked STRING deletion.

| Schemas | Pre-PR commands/s | Final commands/s | Improvement |
|---:|---:|---:|---:|
| 1 | 21,770 | 108,392 | 5.0× |
| 2 | 17,016 | 78,149 | 4.6× |
| 5 | 9,481 | 43,270 | 4.6× |
| 10 | 5,450 | 25,810 | 4.7× |

Deletion counts and final index contents pass validation. The complete tables also retain smaller adverse results, including duplicate-key MULTI throughput at one schema, which falls 0.8%.

## Verification and limits

The final renamed source passes 174 core tests, 392 relevant index tests, three SDK ownership tests, and 37 focused integrations. Formatting and isolated changed-line clang-tidy checks pass. Four known pre-existing distance tests are excluded. Before the identifier-only rename, the same behavior also passed 49 legacy integrations.

The full Python suite has not been rerun on the final candidate. Appendix D records the earlier full-suite result and its unresolved failure.

These measurements use modest text documents on loopback. They exclude replication, persistence, remote-network delays, and large vector updates. Mixed pair directions and overlapping ranges limit conclusions. No universal performance-neutrality claim is supported.

## Complete pre-PR versus final timing tables

Every cell has five independent pairs. Values are median [minimum–maximum]. The tables retain all 68 primary comparison cells, including throughput and p50 and p99 latency.

### Macro throughput

| Workload | Schemas | Pre-PR completed/s | Final completed/s | Change | Pair direction |
|---|---:|---:|---:|---:|---|
| del_missing_8 | 1 | 742,356 [731,354–746,171] | 745,508 [730,740–748,660] | +0.4% | mixed across pairs |
| del_missing_8 | 2 | 742,158 [739,689–743,465] | 743,175 [739,602–744,916] | +0.1% | mixed across pairs |
| del_missing_8 | 5 | 741,291 [737,767–742,508] | 738,421 [737,417–742,451] | -0.4% | mixed across pairs |
| del_missing_8 | 10 | 731,722 [731,380–735,636] | 731,405 [729,182–733,622] | -0.0% | mixed across pairs |
| del_string_8 | 1 | 21,770 [21,384–22,098] | 108,392 [104,948–109,122] | +397.9% | better in all five pairs |
| del_string_8 | 2 | 17,016 [15,286–17,398] | 78,149 [77,866–78,846] | +359.3% | better in all five pairs |
| del_string_8 | 5 | 9,481 [9,277–9,615] | 43,270 [42,901–43,785] | +356.4% | better in all five pairs |
| del_string_8 | 10 | 5,450 [5,338–5,532] | 25,810 [25,586–26,036] | +373.6% | better in all five pairs |
| del_tracked_1 | 1 | 71,445 [71,263–71,774] | 73,025 [71,905–73,685] | +2.2% | better in all five pairs |
| del_tracked_1 | 2 | 60,709 [59,499–63,411] | 61,314 [59,818–62,271] | +1.0% | mixed across pairs |
| del_tracked_1 | 5 | 49,601 [47,110–50,268] | 49,874 [48,000–50,268] | +0.6% | mixed across pairs |
| del_tracked_1 | 10 | 35,476 [34,240–37,130] | 36,597 [34,834–37,577] | +3.2% | mixed across pairs |
| del_tracked_8 | 1 | 19,149 [18,775–19,317] | 19,215 [18,648–19,719] | +0.3% | mixed across pairs |
| del_tracked_8 | 2 | 15,152 [14,689–15,392] | 16,377 [15,193–16,449] | +8.1% | better in all five pairs |
| del_tracked_8 | 5 | 10,480 [10,037–10,582] | 10,962 [10,680–11,009] | +4.6% | better in all five pairs |
| del_tracked_8 | 10 | 6,304 [6,254–6,438] | 6,476 [6,267–6,652] | +2.7% | mixed across pairs |
| expire_string | 1 | 472,937 [462,210–475,443] | 467,421 [446,719–475,296] | -1.2% | mixed across pairs |
| expire_string | 2 | 386,794 [381,497–388,051] | 387,674 [381,859–388,763] | +0.2% | mixed across pairs |
| expire_string | 5 | 251,777 [250,347–252,314] | 252,140 [248,019–254,272] | +0.1% | mixed across pairs |
| expire_string | 10 | 164,644 [163,517–165,158] | 163,726 [162,211–164,640] | -0.6% | mixed across pairs |
| hset_new | 1 | 61,720 [52,841–63,825] | 64,679 [62,334–65,897] | +4.8% | better in all five pairs |
| hset_new | 2 | 52,939 [51,418–54,554] | 53,973 [53,628–54,453] | +2.0% | mixed across pairs |
| hset_new | 5 | 38,906 [38,435–39,412] | 40,716 [40,106–41,523] | +4.7% | better in all five pairs |
| hset_new | 10 | 20,546 [19,446–21,327] | 20,270 [19,475–21,320] | -1.3% | mixed across pairs |
| hset_outside | 1 | 630,082 [624,982–631,513] | 629,675 [623,318–634,496] | -0.1% | mixed across pairs |
| hset_outside | 2 | 628,405 [625,921–632,942] | 629,124 [624,874–631,287] | +0.1% | mixed across pairs |
| hset_outside | 5 | 624,647 [623,769–627,445] | 626,940 [625,039–627,834] | +0.4% | better in all five pairs |
| hset_outside | 10 | 622,720 [618,371–623,730] | 622,016 [615,577–624,172] | -0.1% | mixed across pairs |
| hset_tracked | 1 | 70,805 [69,031–72,332] | 72,502 [68,071–73,590] | +2.4% | mixed across pairs |
| hset_tracked | 2 | 64,636 [61,604–65,618] | 64,849 [64,610–65,655] | +0.3% | mixed across pairs |
| hset_tracked | 5 | 34,514 [33,851–35,904] | 34,305 [32,878–36,374] | -0.6% | mixed across pairs |
| hset_tracked | 10 | 17,128 [16,413–17,513] | 17,348 [16,549–17,872] | +1.3% | mixed across pairs |
| json_tracked | 1 | 52,044 [50,651–52,838] | 53,263 [50,971–55,791] | +2.3% | mixed across pairs |
| json_tracked | 2 | 39,374 [38,539–40,019] | 38,704 [38,122–39,661] | -1.7% | mixed across pairs |
| json_tracked | 5 | 27,783 [27,197–28,793] | 25,938 [24,359–26,837] | -6.6% | worse in all five pairs |
| json_tracked | 10 | 18,191 [17,347–18,410] | 17,251 [16,694–18,107] | -5.2% | worse in all five pairs |
| multi_distinct | 1 | 26,324 [25,184–26,386] | 25,850 [25,481–26,688] | -1.8% | mixed across pairs |
| multi_distinct | 2 | 14,069 [13,908–14,395] | 14,300 [14,031–14,545] | +1.6% | mixed across pairs |
| multi_distinct | 5 | 6,041 [5,757–6,116] | 5,819 [5,712–6,159] | -3.7% | mixed across pairs |
| multi_distinct | 10 | 2,952 [2,919–2,985] | 2,984 [2,932–3,016] | +1.1% | mixed across pairs |
| multi_duplicate | 1 | 53,668 [53,311–54,679] | 53,219 [51,551–53,470] | -0.8% | worse in all five pairs |
| multi_duplicate | 2 | 32,649 [31,986–32,876] | 32,919 [32,688–33,151] | +0.8% | mixed across pairs |
| multi_duplicate | 5 | 14,883 [14,741–15,258] | 15,080 [14,804–15,297] | +1.3% | mixed across pairs |
| multi_duplicate | 10 | 7,957 [7,872–8,126] | 7,791 [7,696–8,122] | -2.1% | mixed across pairs |
| multi_then_normal | 1 | 72,686 [69,218–73,355] | 72,639 [70,582–74,116] | -0.1% | mixed across pairs |
| multi_then_normal | 2 | 42,915 [41,553–43,659] | 42,742 [35,350–43,422] | -0.4% | mixed across pairs |
| multi_then_normal | 5 | 18,594 [18,033–19,419] | 18,824 [17,446–19,431] | +1.2% | mixed across pairs |
| multi_then_normal | 10 | 9,343 [9,297–9,811] | 9,859 [9,131–10,103] | +5.5% | mixed across pairs |
| unlink_tracked_8 | 1 | 18,965 [18,635–19,471] | 19,142 [18,891–19,947] | +0.9% | mixed across pairs |
| unlink_tracked_8 | 2 | 15,622 [14,856–15,984] | 15,909 [15,151–16,606] | +1.8% | mixed across pairs |
| unlink_tracked_8 | 5 | 10,535 [10,214–10,954] | 10,971 [10,646–11,630] | +4.1% | better in all five pairs |
| unlink_tracked_8 | 10 | 6,389 [6,209–6,583] | 6,648 [6,551–6,709] | +4.1% | better in all five pairs |

### Macro latency (µs)

| Workload | Schemas | Pre-PR p50 | Final p50 | Pre-PR p99 | Final p99 |
|---|---:|---:|---:|---:|---:|
| del_missing_8 | 1 | 155.21 [154.36–155.33] | 154.57 [153.80–155.34] | 268.78 [266.50–282.65] | 267.01 [266.19–275.47] |
| del_missing_8 | 2 | 154.96 [154.48–155.17] | 154.56 [154.42–155.18] | 267.12 [266.22–275.87] | 268.25 [265.66–272.68] |
| del_missing_8 | 5 | 154.28 [153.94–154.61] | 154.80 [154.13–155.11] | 267.97 [265.30–270.07] | 269.02 [265.01–271.18] |
| del_missing_8 | 10 | 154.82 [154.24–154.86] | 154.92 [154.14–155.52] | 270.46 [266.91–273.23] | 269.26 [268.46–273.93] |
| del_string_8 | 1 | 5,817.29 [5,731.69–5,885.47] | 967.65 [960.94–1,011.42] | 6,345.73 [6,169.32–6,418.93] | 2,004.86 [1,888.21–2,015.07] |
| del_string_8 | 2 | 7,462.15 [7,253.19–8,224.36] | 1,342.09 [1,320.89–1,343.47] | 7,983.78 [7,850.60–9,083.54] | 2,529.14 [2,463.17–2,733.96] |
| del_string_8 | 5 | 13,272.20 [13,095.60–13,544.60] | 2,450.84 [2,413.82–2,667.01] | 14,137.80 [13,933.70–15,260.40] | 5,048.47 [4,637.75–5,094.59] |
| del_string_8 | 10 | 23,243.50 [22,882.60–23,680.90] | 4,114.29 [4,054.78–4,315.31] | 24,740.50 [24,003.00–25,313.60] | 7,593.91 [7,580.00–7,810.99] |
| del_tracked_1 | 1 | 1,757.59 [1,752.37–1,770.52] | 1,726.17 [1,706.22–1,746.04] | 2,208.67 [2,116.53–2,282.76] | 2,179.34 [2,108.57–2,418.52] |
| del_tracked_1 | 2 | 2,056.40 [1,973.23–2,106.98] | 2,041.30 [2,014.46–2,103.75] | 2,443.22 [2,379.17–2,611.57] | 2,458.34 [2,427.81–2,481.62] |
| del_tracked_1 | 5 | 2,517.32 [2,480.62–2,618.24] | 2,489.84 [2,478.65–2,592.35] | 3,207.61 [3,136.55–3,568.49] | 3,210.88 [3,056.60–3,558.49] |
| del_tracked_1 | 10 | 3,470.65 [3,341.14–3,619.14] | 3,357.55 [3,299.28–3,530.69] | 4,134.26 [3,967.30–9,408.38] | 4,292.52 [3,941.95–5,780.00] |
| del_tracked_8 | 1 | 6,593.63 [6,490.23–6,691.34] | 6,513.62 [6,374.89–6,745.43] | 7,254.62 [7,220.16–7,602.03] | 7,393.87 [7,235.71–7,485.31] |
| del_tracked_8 | 2 | 8,270.33 [8,183.68–8,579.52] | 7,704.97 [7,654.62–8,220.15] | 9,601.35 [9,315.73–9,818.77] | 8,982.39 [8,388.49–9,711.26] |
| del_tracked_8 | 5 | 11,955.20 [11,820.00–12,345.60] | 11,301.20 [11,239.40–11,634.80] | 13,564.10 [13,285.90–15,439.50] | 12,883.90 [12,712.50–13,345.10] |
| del_tracked_8 | 10 | 19,940.90 [19,484.90–20,136.20] | 19,379.80 [18,724.20–20,089.40] | 21,842.60 [21,456.70–22,172.60] | 21,088.60 [20,317.10–21,297.50] |
| expire_string | 1 | 240.16 [239.71–241.58] | 243.14 [238.84–246.06] | 441.05 [438.46–445.74] | 446.43 [440.56–508.61] |
| expire_string | 2 | 294.13 [292.82–298.54] | 293.06 [292.10–298.03] | 546.35 [544.68–553.54] | 544.16 [543.33–551.31] |
| expire_string | 5 | 447.88 [446.73–449.23] | 446.50 [443.30–455.11] | 853.07 [850.90–856.10] | 853.63 [846.28–866.97] |
| expire_string | 10 | 680.83 [678.30–685.06] | 682.23 [680.50–690.47] | 1,320.78 [1,317.23–1,327.37] | 1,325.90 [1,323.21–1,342.44] |
| hset_new | 1 | 1,993.54 [1,929.65–2,007.76] | 1,896.41 [1,869.30–1,979.57] | 3,420.99 [3,203.26–9,649.02] | 3,196.73 [3,135.23–3,788.40] |
| hset_new | 2 | 2,317.21 [2,247.11–2,382.65] | 2,271.21 [2,263.64–2,300.97] | 4,114.06 [3,888.06–4,257.31] | 3,869.73 [3,799.02–4,022.91] |
| hset_new | 5 | 3,146.84 [3,059.28–3,177.69] | 2,984.16 [2,916.68–3,021.93] | 5,423.28 [5,104.26–5,591.35] | 5,264.47 [4,799.87–5,838.39] |
| hset_new | 10 | 6,014.59 [5,810.59–6,360.91] | 6,105.62 [5,789.85–6,368.67] | 8,649.89 [8,428.89–11,500.00] | 8,749.56 [8,566.00–9,602.54] |
| hset_outside | 1 | 181.12 [180.91–181.76] | 181.49 [179.96–181.80] | 331.62 [330.05–339.24] | 331.22 [327.53–333.62] |
| hset_outside | 2 | 180.97 [180.03–181.87] | 181.15 [180.83–182.93] | 329.74 [326.82–331.88] | 329.56 [327.12–329.80] |
| hset_outside | 5 | 181.90 [181.25–182.11] | 181.51 [180.85–181.93] | 330.25 [324.86–331.59] | 327.27 [325.38–330.20] |
| hset_outside | 10 | 181.24 [180.94–182.57] | 181.48 [180.87–183.28] | 328.20 [323.55–331.89] | 328.24 [325.54–333.02] |
| hset_tracked | 1 | 1,758.52 [1,727.15–1,815.21] | 1,738.37 [1,720.13–1,741.75] | 2,559.50 [2,439.95–2,688.14] | 2,399.35 [2,098.70–2,744.71] |
| hset_tracked | 2 | 1,963.45 [1,929.88–2,057.10] | 1,946.68 [1,930.22–1,958.25] | 2,321.67 [2,268.96–2,414.14] | 2,319.30 [2,273.62–2,390.26] |
| hset_tracked | 5 | 3,684.09 [3,539.70–3,708.07] | 3,709.85 [3,492.70–3,869.62] | 3,833.96 [3,751.37–4,295.92] | 3,791.27 [3,686.34–4,077.35] |
| hset_tracked | 10 | 7,425.19 [7,219.86–7,640.99] | 7,307.99 [7,069.40–7,700.76] | 7,876.95 [7,589.19–9,346.80] | 7,916.39 [7,557.83–8,593.29] |
| json_tracked | 1 | 2,444.55 [2,401.48–2,506.77] | 2,382.38 [2,274.12–2,493.20] | 2,802.59 [2,757.88–2,866.15] | 2,742.73 [2,607.78–2,877.97] |
| json_tracked | 2 | 3,212.83 [3,161.27–3,275.21] | 3,266.73 [3,201.10–3,334.04] | 3,686.61 [3,551.64–4,125.03] | 3,630.97 [3,533.74–4,345.60] |
| json_tracked | 5 | 4,566.76 [4,410.32–4,678.73] | 4,900.70 [4,730.46–5,216.63] | 5,188.27 [4,986.18–5,798.57] | 5,644.56 [5,417.65–5,879.58] |
| json_tracked | 10 | 6,998.95 [6,918.63–7,341.75] | 7,379.44 [7,031.60–7,637.71] | 7,325.03 [7,253.38–7,555.90] | 7,686.41 [7,448.81–7,891.39] |
| multi_distinct | 1 | 5,526.48 [5,303.71–5,764.96] | 5,408.40 [5,231.17–5,445.37] | 7,251.57 [6,828.74–7,317.54] | 6,945.67 [6,738.35–7,149.12] |
| multi_distinct | 2 | 10,053.10 [9,915.32–10,225.50] | 9,897.88 [9,649.95–10,353.70] | 12,703.30 [12,501.00–13,172.10] | 12,745.80 [12,208.90–13,450.90] |
| multi_distinct | 5 | 23,119.80 [21,787.50–24,592.50] | 23,002.20 [22,805.50–23,546.10] | 29,051.90 [27,985.60–29,610.90] | 29,181.50 [27,375.80–31,536.40] |
| multi_distinct | 10 | 44,089.70 [43,507.00–44,675.10] | 43,619.00 [42,951.50–44,847.90] | 55,133.20 [53,986.00–57,535.60] | 54,300.70 [54,049.30–56,792.90] |
| multi_duplicate | 1 | 2,541.56 [2,423.14–2,588.09] | 2,578.24 [2,558.43–2,650.18] | 3,488.80 [3,432.00–3,519.44] | 3,509.37 [3,371.92–3,570.30] |
| multi_duplicate | 2 | 4,289.63 [4,190.69–4,351.67] | 4,140.38 [4,059.10–4,333.12] | 5,592.14 [5,484.77–5,771.43] | 5,387.85 [4,845.60–5,650.36] |
| multi_duplicate | 5 | 9,191.10 [8,950.39–9,400.36] | 9,284.28 [9,098.60–9,422.58] | 11,833.40 [11,671.50–12,719.80] | 11,676.20 [11,275.20–12,239.20] |
| multi_duplicate | 10 | 16,532.30 [16,082.20–17,476.90] | 17,145.40 [15,861.30–17,433.00] | 21,585.20 [19,632.50–21,966.60] | 21,436.30 [19,704.30–23,820.70] |
| multi_then_normal | 1 | 1,968.12 [1,345.52–1,985.91] | 1,924.11 [1,378.21–1,971.93] | 2,816.08 [2,676.06–2,840.93] | 2,713.72 [2,626.79–2,859.27] |
| multi_then_normal | 2 | 3,530.39 [3,436.32–3,544.75] | 3,493.62 [3,470.65–3,538.69] | 4,564.37 [4,394.11–4,758.00] | 4,550.71 [4,447.57–7,737.56] |
| multi_then_normal | 5 | 7,652.38 [7,418.25–7,786.60] | 7,642.25 [7,488.25–8,434.97] | 10,077.30 [9,745.60–14,508.90] | 10,143.00 [9,812.17–13,898.20] |
| multi_then_normal | 10 | 14,576.00 [13,985.10–14,625.30] | 14,484.90 [14,048.90–14,801.80] | 21,178.10 [18,729.50–32,406.00] | 18,666.30 [18,180.20–30,842.50] |
| unlink_tracked_8 | 1 | 6,683.74 [6,463.81–6,779.72] | 6,543.76 [6,262.82–6,671.44] | 7,357.41 [7,298.71–7,494.41] | 7,325.45 [7,021.22–8,458.37] |
| unlink_tracked_8 | 2 | 8,010.91 [7,843.72–8,496.60] | 7,832.88 [7,557.14–8,287.46] | 9,323.29 [9,094.04–9,588.29] | 9,061.48 [8,806.65–9,535.64] |
| unlink_tracked_8 | 5 | 11,792.70 [11,379.00–12,234.50] | 11,382.80 [10,698.40–11,763.80] | 13,409.40 [12,419.20–13,897.90] | 12,817.30 [11,687.30–13,368.70] |
| unlink_tracked_8 | 10 | 19,672.30 [19,019.20–20,235.90] | 18,930.40 [18,687.70–19,206.40] | 21,438.90 [20,834.80–22,076.80] | 20,511.70 [19,967.30–21,477.10] |

### Micro throughput

| Workload | Schemas | Pre-PR completed/s | Final completed/s | Change | Pair direction |
|---|---:|---:|---:|---:|---|
| del_tracked_1 | 1 | 25,961 [25,582–26,665] | 26,176 [26,049–27,238] | +0.8% | mixed across pairs |
| del_tracked_1 | 2 | 21,902 [19,161–22,585] | 22,760 [22,272–23,373] | +3.9% | better in all five pairs |
| del_tracked_1 | 5 | 18,188 [16,219–18,536] | 17,917 [17,552–18,587] | -1.5% | mixed across pairs |
| del_tracked_1 | 10 | 15,038 [14,903–15,314] | 15,439 [15,148–15,883] | +2.7% | better in all five pairs |
| hset_tracked | 1 | 18,769 [18,526–19,335] | 18,743 [18,642–19,087] | -0.1% | mixed across pairs |
| hset_tracked | 2 | 16,455 [16,165–16,736] | 16,501 [16,389–16,840] | +0.3% | mixed across pairs |
| hset_tracked | 5 | 12,329 [11,965–12,517] | 12,311 [12,156–12,547] | -0.1% | mixed across pairs |
| hset_tracked | 10 | 8,376 [8,094–8,641] | 8,557 [8,386–8,868] | +2.2% | mixed across pairs |
| json_tracked | 1 | 16,174 [16,074–17,017] | 15,895 [15,371–16,204] | -1.7% | mixed across pairs |
| json_tracked | 2 | 14,498 [13,669–15,030] | 14,106 [13,827–14,762] | -2.7% | mixed across pairs |
| json_tracked | 5 | 10,641 [10,531–10,963] | 11,029 [10,533–11,303] | +3.6% | mixed across pairs |
| json_tracked | 10 | 7,909 [7,623–8,046] | 7,897 [7,768–8,078] | -0.1% | mixed across pairs |
| multi_then_normal | 1 | 24,919 [24,796–25,058] | 25,123 [20,960–25,247] | +0.8% | mixed across pairs |
| multi_then_normal | 2 | 17,122 [16,745–17,383] | 17,560 [16,986–17,706] | +2.6% | mixed across pairs |
| multi_then_normal | 5 | 9,052 [8,882–9,179] | 9,126 [8,927–9,240] | +0.8% | mixed across pairs |
| multi_then_normal | 10 | 5,061 [4,935–5,091] | 5,031 [4,875–5,120] | -0.6% | mixed across pairs |

### Micro latency (µs)

| Workload | Schemas | Pre-PR p50 | Final p50 | Pre-PR p99 | Final p99 |
|---|---:|---:|---:|---:|---:|
| del_tracked_1 | 1 | 37.03 [36.34–37.51] | 36.98 [35.66–37.02] | 49.91 [46.68–58.47] | 49.18 [42.80–51.57] |
| del_tracked_1 | 2 | 43.37 [42.56–46.11] | 41.94 [41.20–43.09] | 68.47 [58.98–115.32] | 52.27 [51.69–61.44] |
| del_tracked_1 | 5 | 53.04 [52.07–57.16] | 52.64 [51.73–53.99] | 64.05 [62.39–100.68] | 63.37 [63.24–85.48] |
| del_tracked_1 | 10 | 62.69 [61.92–62.88] | 61.40 [59.57–61.98] | 82.23 [78.35–90.71] | 77.46 [72.15–103.91] |
| hset_tracked | 1 | 52.02 [50.66–52.72] | 51.97 [51.39–52.48] | 62.99 [59.99–67.74] | 60.70 [59.80–66.55] |
| hset_tracked | 2 | 58.64 [58.05–59.85] | 58.52 [57.35–59.26] | 74.27 [69.25–79.51] | 76.21 [70.60–96.94] |
| hset_tracked | 5 | 78.36 [77.37–80.88] | 78.64 [76.87–79.52] | 98.06 [91.10–101.86] | 97.43 [92.59–98.98] |
| hset_tracked | 10 | 114.62 [112.80–118.27] | 113.72 [109.75–114.64] | 147.19 [128.88–169.85] | 134.51 [126.50–161.31] |
| json_tracked | 1 | 60.37 [56.63–60.75] | 61.47 [59.97–63.74] | 78.35 [73.20–84.48] | 82.17 [74.42–88.76] |
| json_tracked | 2 | 67.18 [64.92–70.11] | 68.88 [65.75–70.54] | 88.16 [79.05–109.21] | 89.86 [81.66–92.66] |
| json_tracked | 5 | 90.90 [87.72–92.37] | 88.04 [84.76–91.64] | 121.04 [114.37–125.84] | 113.44 [107.70–132.31] |
| json_tracked | 10 | 122.40 [120.80–126.16] | 122.41 [119.63–125.42] | 148.39 [138.95–165.32] | 147.46 [141.64–160.65] |
| multi_then_normal | 1 | 26.11 [25.94–26.42] | 26.03 [25.97–29.02] | 94.57 [90.64–99.19] | 92.63 [88.86–142.21] |
| multi_then_normal | 2 | 29.92 [29.79–30.43] | 29.71 [29.42–30.09] | 157.06 [152.11–175.14] | 157.13 [151.50–159.30] |
| multi_then_normal | 5 | 40.15 [39.75–40.31] | 40.48 [39.32–40.67] | 349.79 [339.28–357.17] | 344.22 [336.83–376.37] |
| multi_then_normal | 10 | 58.47 [57.88–58.80] | 58.62 [58.05–60.03] | 677.66 [669.74–707.30] | 683.98 [679.59–733.04] |

## Appendix A: earlier before-sleep implementation

This intermediate build promotes deferred transaction work and releases command registration ownership at before-sleep. It is a historical comparison, not the pre-PR baseline.

Against before-sleep cleanup, tracked HSET at one schema remains adverse: **79,506→73,819 commands/second, −7.2%, worse in all five pairs**. New-key HSET at two schemas is **57,198→52,569 commands/second, −8.1%, worse in all pairs**. Tracked single-key DEL1 is −3.7%, tracked DEL8 −2.7% at one schema, also all pairs adverse. These are additional costs relative to the preceding cleanup architecture, not effects that the direct transaction wait removal proves absent.

The before-sleep transaction comparison has the same tradeoff as the immediate-previous comparison: completed medians improve +62.3%/+31.5% with one/two schemas but fall −7.7%/−17.3% with five/ten, all pair directions consistent.

### Earlier before-sleep implementation

#### Macro throughput

| Workload | Schemas | Baseline completed/s | Candidate completed/s | Change | Pair direction |
|---|---:|---:|---:|---:|---|
| del_missing_8 | 1 | 741,376 [737,316–742,499] | 742,265 [739,467–747,884] | +0.1% | mixed across pairs |
| del_missing_8 | 2 | 742,742 [739,805–744,862] | 743,181 [741,626–744,981] | +0.1% | mixed across pairs |
| del_missing_8 | 5 | 734,771 [715,196–738,453] | 737,244 [735,707–740,224] | +0.3% | mixed across pairs |
| del_missing_8 | 10 | 731,041 [728,289–731,963] | 733,415 [729,206–734,850] | +0.3% | better in all five pairs |
| del_string_8 | 1 | 22,847 [22,485–23,536] | 108,398 [107,698–109,291] | +374.5% | better in all five pairs |
| del_string_8 | 2 | 17,019 [16,258–17,222] | 77,852 [77,554–78,325] | +357.4% | better in all five pairs |
| del_string_8 | 5 | 9,643 [9,464–9,803] | 43,233 [43,013–43,739] | +348.3% | better in all five pairs |
| del_string_8 | 10 | 5,471 [5,132–5,518] | 25,815 [25,677–25,942] | +371.8% | better in all five pairs |
| del_tracked_1 | 1 | 74,497 [73,301–76,616] | 71,715 [70,972–72,441] | -3.7% | worse in all five pairs |
| del_tracked_1 | 2 | 62,812 [61,922–66,034] | 63,606 [62,849–64,554] | +1.3% | mixed across pairs |
| del_tracked_1 | 5 | 51,143 [47,298–52,367] | 50,040 [49,552–50,784] | -2.2% | mixed across pairs |
| del_tracked_1 | 10 | 36,917 [36,114–38,005] | 36,451 [35,845–37,172] | -1.3% | mixed across pairs |
| del_tracked_8 | 1 | 20,005 [19,895–20,447] | 19,457 [19,284–19,781] | -2.7% | worse in all five pairs |
| del_tracked_8 | 2 | 16,828 [16,403–17,121] | 15,798 [15,646–16,835] | -6.1% | mixed across pairs |
| del_tracked_8 | 5 | 11,554 [11,060–11,845] | 11,063 [10,508–11,174] | -4.2% | mixed across pairs |
| del_tracked_8 | 10 | 6,576 [6,358–6,709] | 6,653 [6,533–6,829] | +1.2% | mixed across pairs |
| expire_string | 1 | 483,038 [480,831–485,248] | 475,039 [472,954–475,994] | -1.7% | worse in all five pairs |
| expire_string | 2 | 393,990 [386,845–394,357] | 387,433 [384,739–387,953] | -1.7% | mixed across pairs |
| expire_string | 5 | 252,556 [252,204–254,335] | 250,750 [249,655–253,115] | -0.7% | mixed across pairs |
| expire_string | 10 | 164,246 [163,930–165,697] | 163,922 [162,897–164,549] | -0.2% | mixed across pairs |
| hset_new | 1 | 65,605 [65,294–67,895] | 65,275 [63,536–65,890] | -0.5% | mixed across pairs |
| hset_new | 2 | 57,198 [57,022–59,603] | 52,569 [50,876–54,984] | -8.1% | worse in all five pairs |
| hset_new | 5 | 40,447 [39,203–41,365] | 40,740 [40,392–41,046] | +0.7% | mixed across pairs |
| hset_new | 10 | 21,111 [20,899–21,341] | 20,741 [19,511–20,935] | -1.8% | worse in all five pairs |
| hset_outside | 1 | 633,286 [631,059–634,385] | 629,140 [626,541–631,070] | -0.7% | mixed across pairs |
| hset_outside | 2 | 630,682 [629,482–634,444] | 626,752 [626,028–630,960] | -0.6% | worse in all five pairs |
| hset_outside | 5 | 628,788 [626,305–629,232] | 624,921 [624,135–628,342] | -0.6% | worse in all five pairs |
| hset_outside | 10 | 625,471 [619,572–626,273] | 619,663 [614,093–621,491] | -0.9% | mixed across pairs |
| hset_tracked | 1 | 79,506 [78,480–81,199] | 73,819 [72,960–74,884] | -7.2% | worse in all five pairs |
| hset_tracked | 2 | 66,344 [64,598–67,723] | 65,438 [64,914–66,665] | -1.4% | mixed across pairs |
| hset_tracked | 5 | 34,356 [32,218–36,222] | 33,616 [33,310–36,352] | -2.2% | mixed across pairs |
| hset_tracked | 10 | 17,714 [17,289–18,125] | 17,455 [16,977–17,572] | -1.5% | mixed across pairs |
| json_tracked | 1 | 53,104 [50,943–54,461] | 52,125 [50,991–54,541] | -1.8% | mixed across pairs |
| json_tracked | 2 | 39,849 [39,604–40,043] | 38,507 [38,345–40,234] | -3.4% | mixed across pairs |
| json_tracked | 5 | 27,918 [26,956–29,281] | 27,354 [25,805–27,469] | -2.0% | mixed across pairs |
| json_tracked | 10 | 17,684 [16,803–18,040] | 17,770 [17,244–18,483] | +0.5% | mixed across pairs |
| multi_distinct | 1 | 25,983 [24,573–26,520] | 26,032 [25,027–26,357] | +0.2% | mixed across pairs |
| multi_distinct | 2 | 14,393 [13,988–14,574] | 13,985 [13,839–14,173] | -2.8% | mixed across pairs |
| multi_distinct | 5 | 5,890 [5,589–6,058] | 6,008 [5,782–6,097] | +2.0% | mixed across pairs |
| multi_distinct | 10 | 2,947 [2,928–3,056] | 2,931 [2,843–2,982] | -0.6% | mixed across pairs |
| multi_duplicate | 1 | 53,552 [51,789–54,844] | 54,157 [52,876–54,867] | +1.1% | mixed across pairs |
| multi_duplicate | 2 | 32,727 [32,015–33,199] | 33,282 [32,207–33,587] | +1.7% | mixed across pairs |
| multi_duplicate | 5 | 15,138 [14,709–15,254] | 15,433 [15,137–15,592] | +2.0% | mixed across pairs |
| multi_duplicate | 10 | 8,028 [7,822–8,093] | 7,955 [7,895–8,048] | -0.9% | mixed across pairs |
| multi_then_normal | 1 | 45,193 [42,096–45,792] | 73,347 [72,082–74,623] | +62.3% | better in all five pairs |
| multi_then_normal | 2 | 32,641 [31,722–33,386] | 42,921 [42,343–43,829] | +31.5% | better in all five pairs |
| multi_then_normal | 5 | 21,195 [20,262–21,366] | 19,555 [19,142–19,741] | -7.7% | worse in all five pairs |
| multi_then_normal | 10 | 12,000 [11,720–12,142] | 9,928 [9,741–10,292] | -17.3% | worse in all five pairs |
| unlink_tracked_8 | 1 | 19,871 [19,394–20,571] | 19,409 [19,004–19,579] | -2.3% | mixed across pairs |
| unlink_tracked_8 | 2 | 16,285 [15,585–16,768] | 16,431 [15,970–16,827] | +0.9% | mixed across pairs |
| unlink_tracked_8 | 5 | 11,489 [10,712–11,779] | 11,283 [11,155–11,337] | -1.8% | mixed across pairs |
| unlink_tracked_8 | 10 | 6,607 [6,494–6,663] | 6,618 [6,434–6,867] | +0.2% | mixed across pairs |

#### Macro latency (µs)

| Workload | Schemas | Baseline p50 | Candidate p50 | Baseline p99 | Candidate p99 |
|---|---:|---:|---:|---:|---:|
| del_missing_8 | 1 | 155.15 [155.14–156.17] | 155.06 [154.10–155.34] | 269.64 [266.86–272.15] | 268.13 [265.14–277.30] |
| del_missing_8 | 2 | 154.70 [154.24–155.26] | 154.66 [154.19–155.02] | 268.69 [266.40–270.00] | 267.28 [266.56–268.63] |
| del_missing_8 | 5 | 155.51 [154.73–159.66] | 154.80 [154.39–155.35] | 268.88 [267.17–279.05] | 269.19 [266.91–271.83] |
| del_missing_8 | 10 | 154.97 [154.67–155.46] | 154.64 [154.40–155.30] | 269.14 [267.34–272.59] | 268.31 [266.96–270.55] |
| del_string_8 | 1 | 5,491.89 [5,330.05–5,622.79] | 963.16 [954.11–976.63] | 5,919.14 [5,845.11–6,051.48] | 1,945.45 [1,860.94–2,003.00] |
| del_string_8 | 2 | 7,371.48 [7,205.30–7,724.96] | 1,347.65 [1,342.51–1,356.36] | 8,127.64 [7,923.98–8,393.75] | 2,724.62 [2,631.03–2,757.72] |
| del_string_8 | 5 | 12,920.40 [12,860.90–13,290.10] | 2,442.14 [2,400.29–2,670.39] | 14,011.00 [13,649.60–14,166.70] | 4,856.63 [4,693.23–4,925.07] |
| del_string_8 | 10 | 22,776.30 [22,638.60–24,577.70] | 4,104.64 [4,085.68–4,332.11] | 24,668.00 [24,416.70–26,169.40] | 7,863.62 [7,652.35–8,187.86] |
| del_tracked_1 | 1 | 1,671.35 [1,635.72–1,704.97] | 1,754.93 [1,736.22–1,773.35] | 2,080.07 [2,001.51–2,106.23] | 2,204.49 [2,148.14–2,270.41] |
| del_tracked_1 | 2 | 1,990.30 [1,899.43–2,012.88] | 1,970.29 [1,938.06–1,993.65] | 2,287.59 [2,276.75–2,623.11] | 2,392.77 [2,291.25–2,477.36] |
| del_tracked_1 | 5 | 2,410.45 [2,352.33–2,611.84] | 2,493.20 [2,458.81–2,518.81] | 3,121.42 [2,953.99–3,274.24] | 3,067.19 [3,022.69–3,319.64] |
| del_tracked_1 | 10 | 3,330.09 [3,220.39–3,392.35] | 3,376.85 [3,343.94–3,467.03] | 3,958.18 [3,859.92–4,349.99] | 4,141.43 [3,998.94–4,374.02] |
| del_tracked_8 | 1 | 6,228.98 [6,179.45–6,328.90] | 6,462.53 [6,371.42–6,532.81] | 6,829.75 [6,781.54–7,007.97] | 7,180.01 [6,983.68–7,358.24] |
| del_tracked_8 | 2 | 7,458.31 [7,294.42–7,591.85] | 7,908.10 [7,371.28–8,041.34] | 8,348.01 [8,228.33–9,404.48] | 9,138.97 [8,567.86–9,238.21] |
| del_tracked_8 | 5 | 10,724.30 [10,401.60–11,207.70] | 11,286.10 [11,130.80–11,836.70] | 12,092.00 [11,785.20–12,178.60] | 12,712.70 [12,125.80–16,500.30] |
| del_tracked_8 | 10 | 19,025.70 [17,757.40–19,670.80] | 18,897.20 [18,406.80–19,333.50] | 22,829.20 [21,511.10–24,022.90] | 20,893.90 [20,236.70–21,581.50] |
| expire_string | 1 | 235.90 [234.78–237.28] | 240.30 [239.58–241.54] | 434.30 [430.46–435.18] | 441.60 [439.13–442.70] |
| expire_string | 2 | 287.83 [287.74–293.59] | 293.45 [292.22–295.51] | 538.04 [535.10–547.10] | 544.56 [542.58–550.98] |
| expire_string | 5 | 445.43 [441.60–446.11] | 448.28 [445.31–451.20] | 850.98 [842.85–856.00] | 854.42 [848.80–863.72] |
| expire_string | 10 | 682.71 [676.00–683.93] | 683.79 [682.38–686.60] | 1,327.05 [1,315.45–1,329.31] | 1,329.50 [1,324.61–1,335.57] |
| hset_new | 1 | 1,853.19 [1,801.22–1,869.53] | 1,886.97 [1,864.50–1,913.52] | 3,087.80 [3,070.70–3,234.59] | 3,404.62 [3,076.57–4,560.77] |
| hset_new | 2 | 2,121.12 [2,016.90–2,124.60] | 2,341.01 [2,235.04–2,407.99] | 4,013.51 [3,831.77–4,106.51] | 3,872.96 [3,842.09–4,023.67] |
| hset_new | 5 | 3,034.41 [2,916.53–3,080.93] | 2,961.31 [2,953.95–3,020.47] | 5,209.16 [4,956.43–6,035.08] | 5,286.21 [5,163.39–5,639.84] |
| hset_new | 10 | 5,875.59 [5,811.42–5,914.24] | 5,981.88 [5,929.23–6,292.88] | 8,549.24 [8,402.95–8,596.69] | 8,840.34 [8,579.05–12,056.40] |
| hset_outside | 1 | 180.53 [180.47–181.03] | 181.83 [181.23–181.89] | 326.65 [322.17–331.01] | 328.72 [326.46–332.60] |
| hset_outside | 2 | 180.14 [179.82–181.35] | 182.10 [181.00–182.30] | 324.19 [321.46–332.22] | 328.22 [324.48–332.55] |
| hset_outside | 5 | 180.56 [180.40–181.23] | 181.56 [181.00–182.06] | 327.03 [322.29–330.07] | 329.45 [327.74–331.61] |
| hset_outside | 10 | 180.18 [180.13–181.92] | 181.81 [181.38–182.13] | 328.77 [325.48–331.70] | 330.70 [325.91–343.06] |
| hset_tracked | 1 | 1,589.17 [1,556.77–1,609.66] | 1,715.61 [1,696.25–1,740.00] | 1,892.51 [1,858.79–1,954.26] | 2,035.10 [2,016.08–2,060.41] |
| hset_tracked | 2 | 1,914.45 [1,872.94–1,928.56] | 1,939.26 [1,899.83–1,948.94] | 2,272.68 [2,183.23–2,909.63] | 2,265.26 [2,239.67–2,368.38] |
| hset_tracked | 5 | 3,702.27 [3,508.66–3,907.47] | 3,756.87 [3,481.87–3,783.05] | 3,822.25 [3,705.70–4,280.91] | 3,941.91 [3,670.71–4,724.21] |
| hset_tracked | 10 | 7,171.79 [7,026.11–7,370.14] | 7,299.84 [7,238.13–7,504.47] | 7,412.02 [7,115.88–7,546.10] | 7,543.67 [7,386.47–7,671.56] |
| json_tracked | 1 | 2,379.91 [2,326.92–2,477.85] | 2,428.90 [2,327.43–2,487.54] | 2,693.40 [2,593.64–2,780.65] | 2,777.37 [2,666.97–2,866.03] |
| json_tracked | 2 | 3,157.85 [3,148.60–3,186.72] | 3,298.47 [3,145.98–3,316.82] | 3,554.40 [3,502.54–3,750.77] | 3,620.30 [3,534.47–3,710.09] |
| json_tracked | 5 | 4,540.79 [4,301.89–4,665.45] | 4,628.10 [4,619.04–4,921.88] | 5,292.61 [4,843.77–5,423.76] | 5,303.40 [5,209.96–5,801.84] |
| json_tracked | 10 | 7,200.20 [7,055.63–7,586.46] | 7,153.06 [6,884.03–7,390.04] | 7,514.39 [7,355.19–7,803.57] | 7,526.44 [7,295.09–7,598.18] |
| multi_distinct | 1 | 5,489.18 [5,196.30–5,628.09] | 5,458.11 [5,388.24–5,716.11] | 7,222.04 [6,909.25–7,508.84] | 7,171.29 [6,728.89–7,482.24] |
| multi_distinct | 2 | 9,860.12 [9,582.37–10,072.20] | 10,125.00 [9,644.76–10,247.50] | 12,520.00 [11,943.20–13,121.10] | 12,873.40 [12,264.70–13,374.50] |
| multi_distinct | 5 | 23,399.20 [22,808.10–25,174.90] | 23,559.20 [23,278.00–24,652.30] | 29,874.90 [29,360.40–31,031.40] | 29,500.50 [29,234.30–30,458.60] |
| multi_distinct | 10 | 44,006.40 [42,529.10–47,122.20] | 45,231.50 [43,978.00–46,274.90] | 55,267.30 [52,974.80–59,832.90] | 55,847.80 [54,604.60–56,896.90] |
| multi_duplicate | 1 | 2,609.64 [2,450.86–2,655.45] | 2,479.49 [1,944.94–2,579.47] | 3,476.00 [3,389.74–3,636.95] | 3,433.74 [3,412.70–3,549.69] |
| multi_duplicate | 2 | 4,317.64 [4,272.48–4,471.89] | 4,226.28 [4,201.98–4,368.36] | 5,798.53 [5,475.01–5,846.68] | 5,634.93 [5,385.91–5,777.87] |
| multi_duplicate | 5 | 9,382.06 [9,262.66–9,681.67] | 9,291.51 [9,053.09–9,373.86] | 11,907.10 [11,548.30–12,385.30] | 11,629.20 [11,567.50–11,945.70] |
| multi_duplicate | 10 | 17,466.00 [17,450.00–17,645.60] | 17,437.80 [17,291.90–17,913.10] | 21,665.10 [20,998.30–22,085.00] | 21,974.00 [21,444.80–22,647.30] |
| multi_then_normal | 1 | 2,797.70 [2,756.52–2,986.02] | 1,932.17 [1,391.43–2,018.95] | 3,388.51 [3,277.65–6,084.96] | 2,665.26 [2,596.11–2,746.35] |
| multi_then_normal | 2 | 3,888.82 [3,784.39–3,992.57] | 3,472.21 [2,333.11–3,536.45] | 4,483.39 [4,228.74–4,787.99] | 4,633.32 [4,392.52–4,753.37] |
| multi_then_normal | 5 | 5,966.32 [5,921.84–6,209.04] | 7,666.42 [5,282.01–7,727.35] | 6,442.53 [6,390.15–7,155.40] | 9,830.84 [9,559.05–10,208.50] |
| multi_then_normal | 10 | 10,595.50 [10,458.80–10,797.90] | 14,608.50 [14,358.50–14,888.10] | 10,991.30 [10,768.80–12,145.90] | 19,008.70 [18,116.60–19,235.70] |
| unlink_tracked_8 | 1 | 6,326.24 [6,115.28–6,484.77] | 6,489.90 [6,448.67–6,585.32] | 6,857.81 [6,704.86–7,025.89] | 7,274.85 [7,228.66–7,514.69] |
| unlink_tracked_8 | 2 | 7,647.51 [7,532.92–7,960.33] | 7,688.38 [7,419.06–7,863.04] | 8,847.25 [8,348.20–9,478.76] | 8,754.68 [8,516.14–8,888.57] |
| unlink_tracked_8 | 5 | 10,806.40 [10,443.80–11,481.20] | 11,029.20 [10,935.80–11,124.40] | 12,014.30 [11,648.40–13,781.10] | 12,801.30 [12,227.30–13,333.30] |
| unlink_tracked_8 | 10 | 18,913.80 [18,600.40–19,389.60] | 18,954.40 [18,243.00–19,649.20] | 22,166.00 [20,497.40–23,181.90] | 20,972.00 [19,841.20–21,921.00] |

#### Micro throughput

| Workload | Schemas | Baseline completed/s | Candidate completed/s | Change | Pair direction |
|---|---:|---:|---:|---:|---|
| del_tracked_1 | 1 | 26,359 [26,222–26,790] | 26,530 [25,820–26,996] | +0.6% | mixed across pairs |
| del_tracked_1 | 2 | 22,295 [21,730–22,816] | 22,528 [22,199–23,254] | +1.0% | mixed across pairs |
| del_tracked_1 | 5 | 17,859 [17,359–18,417] | 18,260 [17,218–18,688] | +2.2% | mixed across pairs |
| del_tracked_1 | 10 | 15,571 [14,732–15,686] | 15,719 [15,394–15,889] | +0.9% | mixed across pairs |
| hset_tracked | 1 | 18,788 [18,589–19,138] | 18,839 [18,411–19,027] | +0.3% | mixed across pairs |
| hset_tracked | 2 | 16,730 [15,363–16,976] | 16,681 [16,572–16,942] | -0.3% | mixed across pairs |
| hset_tracked | 5 | 12,501 [12,213–12,516] | 12,420 [12,297–12,456] | -0.6% | mixed across pairs |
| hset_tracked | 10 | 8,758 [8,439–8,905] | 8,763 [8,581–8,885] | +0.1% | mixed across pairs |
| json_tracked | 1 | 16,340 [15,867–17,259] | 16,519 [15,582–16,911] | +1.1% | mixed across pairs |
| json_tracked | 2 | 14,270 [13,732–15,143] | 14,066 [13,814–14,807] | -1.4% | mixed across pairs |
| json_tracked | 5 | 11,034 [10,162–11,457] | 10,943 [10,517–11,201] | -0.8% | mixed across pairs |
| json_tracked | 10 | 7,869 [7,768–8,249] | 7,645 [7,627–7,728] | -2.8% | worse in all five pairs |
| multi_then_normal | 1 | 15,780 [15,654–16,048] | 24,849 [23,731–25,266] | +57.5% | better in all five pairs |
| multi_then_normal | 2 | 13,291 [13,278–13,346] | 17,475 [16,988–17,559] | +31.5% | better in all five pairs |
| multi_then_normal | 5 | 8,840 [8,707–8,971] | 9,029 [8,978–9,239] | +2.1% | better in all five pairs |
| multi_then_normal | 10 | 5,859 [5,729–5,953] | 5,062 [5,037–5,094] | -13.6% | worse in all five pairs |

#### Micro latency (µs)

| Workload | Schemas | Baseline p50 | Candidate p50 | Baseline p99 | Candidate p99 |
|---|---:|---:|---:|---:|---:|
| del_tracked_1 | 1 | 36.67 [36.27–36.93] | 36.60 [36.12–37.04] | 45.03 [42.59–50.64] | 45.14 [42.82–58.89] |
| del_tracked_1 | 2 | 43.86 [42.27–44.62] | 42.56 [41.50–43.83] | 55.56 [51.67–65.15] | 53.79 [50.27–56.73] |
| del_tracked_1 | 5 | 53.15 [51.84–54.48] | 52.92 [51.66–54.44] | 68.07 [65.29–84.75] | 67.18 [63.35–90.04] |
| del_tracked_1 | 10 | 60.60 [60.44–64.28] | 60.24 [59.54–61.25] | 76.93 [73.89–81.13] | 77.93 [73.55–79.35] |
| hset_tracked | 1 | 51.84 [51.03–52.22] | 51.60 [51.28–52.17] | 66.47 [62.97–71.94] | 68.67 [67.40–79.57] |
| hset_tracked | 2 | 58.05 [57.14–60.89] | 58.09 [57.39–58.74] | 71.83 [69.45–104.99] | 71.34 [69.61–77.14] |
| hset_tracked | 5 | 77.44 [76.72–79.36] | 77.83 [77.24–78.67] | 95.54 [89.76–115.86] | 91.77 [91.18–98.97] |
| hset_tracked | 10 | 110.09 [109.38–114.84] | 111.36 [109.73–113.68] | 129.93 [125.52–133.46] | 125.92 [124.30–132.63] |
| json_tracked | 1 | 59.88 [56.63–61.75] | 59.27 [57.62–62.87] | 72.42 [70.51–75.71] | 73.56 [72.17–76.79] |
| json_tracked | 2 | 68.68 [64.47–71.22] | 69.53 [65.84–70.92] | 80.53 [79.81–107.04] | 83.32 [81.03–85.69] |
| json_tracked | 5 | 88.63 [84.16–97.33] | 87.24 [86.93–92.88] | 104.75 [104.00–115.45] | 110.09 [101.78–142.40] |
| json_tracked | 10 | 123.24 [117.46–125.08] | 125.76 [124.67–126.89] | 146.63 [137.19–153.16] | 151.06 [148.63–155.68] |
| multi_then_normal | 1 | 61.14 [60.32–61.91] | 26.10 [25.97–26.73] | 80.20 [74.70–92.41] | 96.35 [88.36–107.86] |
| multi_then_normal | 2 | 69.62 [69.58–69.88] | 29.95 [29.52–30.27] | 95.94 [93.98–100.24] | 153.66 [151.41–185.68] |
| multi_then_normal | 5 | 97.00 [94.25–97.86] | 40.28 [39.82–40.72] | 172.14 [169.69–177.38] | 350.61 [337.65–358.75] |
| multi_then_normal | 10 | 138.91 [136.77–143.10] | 58.44 [58.28–58.93] | 285.31 [279.11–288.11] | 663.17 [661.44–680.22] |

## Appendix B: earlier eager post-notification implementation

This intermediate build uses execution-unit cleanup but eagerly waits when an ordinary write joins deferred transaction work. The final implementation removes that wait.

### Eager waiting versus final deferred behavior

Completed MULTI→ordinary throughput, **transaction groups/second; higher is better**:

| Index schemas | Eager post-notification | Preserve deferred | Change |
| ---: | ---: | ---: | --- |
| 1 | 44,323 | 73,928 | +66.8%, better in all five pairs |
| 2 | 31,614 | 43,412 | +37.3%, better in all five pairs |
| 5 | 19,954 | 19,115 | −4.2%, worse in all five pairs |
| 10 | 11,676 | 9,906 | −15.2%, worse in all five pairs |

Single-client transaction-group latency, **µs; lower is better**:

| Index schemas | Previous p50 → new | p50 direction | Previous p99 → new | p99 direction |
| ---: | ---: | --- | ---: | --- |
| 1 | 60.87→26.35 | Lower/better | 79.12→90.50 | Higher/worse |
| 2 | 69.28→29.95 | Lower/better | 94.55→159.88 | Higher/worse |
| 5 | 96.58→40.18 | Lower/better | 172.22→355.62 | Higher/worse |
| 10 | 138.96→58.48 | Lower/better | 279.85→679.10 | Higher/worse |

The p99 ranges are:

| Index schemas | Previous p99 µs [min–max] | New p99 µs [min–max] |
| ---: | ---: | ---: |
| 1 | 74.70–91.36 | 86.43–142.96 |
| 2 | 92.32–123.50 | 155.11–176.55 |
| 5 | 167.29–174.19 | 347.48–359.35 |
| 10 | 275.49–292.37 | 664.24–723.03 |

At five/ten schemas, macro batch-of-16 p99 also rises: **6,751.8→10,052.7 µs** and **11,505.6→19,483.7 µs**. Returning to batching removes the usual per-write wait but concentrates occasional work into synchronization batches. The p50 gain must not hide those tails.

Single-client completed throughput improves +56.8%, +29.2% and +2.7% at one/two/five schemas, but falls **14.6% at ten**, all consistent across the five pairs. Direct ordinary HSET at one schema is approximately unchanged, **73,583→73,159 commands/second**, −0.6% with mixed pair directions.

### Immediately preceding eager post-notification implementation

#### Macro throughput

| Workload | Schemas | Baseline completed/s | Candidate completed/s | Change | Pair direction |
|---|---:|---:|---:|---:|---|
| hset_tracked | 1 | 73,583 [71,908–75,200] | 73,159 [71,828–73,272] | -0.6% | mixed across pairs |
| multi_then_normal | 1 | 44,323 [43,121–45,199] | 73,928 [72,473–74,715] | +66.8% | better in all five pairs |
| multi_then_normal | 2 | 31,614 [30,773–31,799] | 43,412 [40,974–43,498] | +37.3% | better in all five pairs |
| multi_then_normal | 5 | 19,954 [19,427–20,668] | 19,115 [18,888–19,725] | -4.2% | worse in all five pairs |
| multi_then_normal | 10 | 11,676 [11,525–11,817] | 9,906 [9,771–10,317] | -15.2% | worse in all five pairs |

#### Macro latency (µs)

| Workload | Schemas | Baseline p50 | Candidate p50 | Baseline p99 | Candidate p99 |
|---|---:|---:|---:|---:|---:|
| hset_tracked | 1 | 1,717.70 [1,685.56–1,757.43] | 1,733.56 [1,730.89–1,764.04] | 2,033.02 [1,935.68–2,113.18] | 2,051.26 [2,025.26–2,092.03] |
| multi_then_normal | 1 | 2,837.46 [2,792.31–2,929.67] | 1,929.34 [1,890.59–2,002.19] | 3,308.30 [3,190.09–3,476.10] | 2,717.35 [2,563.33–2,800.15] |
| multi_then_normal | 2 | 4,024.00 [3,996.52–4,096.65] | 3,432.45 [3,363.65–3,487.30] | 4,578.26 [4,331.12–4,834.37] | 4,464.61 [4,397.75–4,636.70] |
| multi_then_normal | 5 | 6,377.90 [6,148.34–6,501.93] | 7,790.40 [7,305.19–8,113.14] | 6,751.80 [6,559.21–7,470.08] | 10,052.70 [9,733.52–10,247.60] |
| multi_then_normal | 10 | 10,881.80 [10,775.10–11,042.10] | 14,171.90 [13,886.30–15,739.70] | 11,505.60 [11,173.00–11,861.30] | 19,483.70 [18,316.70–20,155.30] |

#### Micro throughput

| Workload | Schemas | Baseline completed/s | Candidate completed/s | Change | Pair direction |
|---|---:|---:|---:|---:|---|
| multi_then_normal | 1 | 15,916 [15,671–15,927] | 24,957 [21,562–25,506] | +56.8% | better in all five pairs |
| multi_then_normal | 2 | 13,379 [12,853–13,647] | 17,292 [16,726–17,490] | +29.2% | better in all five pairs |
| multi_then_normal | 5 | 8,819 [8,739–9,002] | 9,055 [8,999–9,101] | +2.7% | better in all five pairs |
| multi_then_normal | 10 | 5,855 [5,674–5,958] | 4,999 [4,915–5,062] | -14.6% | worse in all five pairs |

#### Micro latency (µs)

| Workload | Schemas | Baseline p50 | Candidate p50 | Baseline p99 | Candidate p99 |
|---|---:|---:|---:|---:|---:|
| multi_then_normal | 1 | 60.87 [60.51–61.31] | 26.35 [26.18–28.11] | 79.12 [74.69–91.36] | 90.50 [86.43–142.96] |
| multi_then_normal | 2 | 69.28 [68.36–72.30] | 29.95 [29.68–30.13] | 94.55 [92.32–123.50] | 159.88 [155.11–176.55] |
| multi_then_normal | 5 | 96.58 [94.41–98.47] | 40.18 [40.10–40.45] | 172.22 [167.29–174.19] | 355.62 [347.48–359.35] |
| multi_then_normal | 10 | 138.96 [137.25–144.19] | 58.48 [58.14–59.75] | 279.85 [275.49–292.37] | 679.10 [664.24–723.02] |

## Appendix C: diagnostic JSON5 experiments


The post-notification job is a plausible contributor, but these experiments do not establish that it explains the entire earlier 6.6% regression. Fresh end-to-end comparisons show a smaller gap; cleanup timing alone does not recover throughput. The investigation changed only copied diagnostic sources; neither diagnostic implementation is part of the production change.

### Decision and controls

Question: does command-end job machinery account for the JSON5 loss, or does the loss require other ownership/scheduling changes? A consistent recovery across five paired runs with a gap exceeding the observed spread would support changing that mechanism. Overlapping spread and inconsistent pair directions limit attribution.

Same JSON.SET workload as the existing sweep: five TEXT index schemas, content field with eight repetitions of a short text payload; eight clients, pipeline 16, four writer threads and two reader threads. Release builds, same clean Valkey server executable and JSON module, persistence disabled, loopback. Server affinity CPUs 0–5, generator CPUs 6–13. Initial host load average 0.68 with 32 CPUs. Module arguments were loaded through temporary configuration files. Every trial checks responses, document values, per-index counts/search markers, and drained indexing queues. Completed throughput includes final indexing drain; setup/seed/validation are outside the timed generator region.

### Experiment 1: change cleanup timing only

Five independent fresh-server cohorts; order reversed on alternate cohorts; 32,768 logical updates per trial. The isolated before_sleep binary retains current shared ownership and dispatch-state behavior, removes post-notification job registration, and releases command ownership in the host before-sleep callback. This deliberately changes release timing and is diagnostic, not a proposed production fix.

| Build | Median completed commands/s | Range | Median main-thread CPU/update |
|---|---:|---:|---:|
| pre_pr | 28,132 | 27,477–29,537 | 35.71 µs |
| current | 27,748 | 26,951–27,979 | 35.71 µs |
| before_sleep | 27,680 | 25,405–28,171 | 36.01 µs |

Current/pre-PR ratio of medians: -1.36%. Current was slower in all five corresponding cohorts, but ranges overlap. Before-sleep/current ratio of medians: -0.25%; before-sleep was slower in three pairs and faster in two. Removing jobs while delaying release did not yield a consistent completed-throughput gain.

### Experiment 2: hold ownership timing fixed, add a no-op host job

Five fresh-server pairs, alternating execution order, 131,072 updates per trial. Both builds release command ownership at before-sleep. The no-op variant registers one post-notification job per keyspace notification, independent of schema count; its callback does nothing. For this ordinary JSON.SET workload this exercises host job allocation, list processing, context preparation and cleanup while preserving the manager's reference-release timing. It does not measure the real manager cleanup callback or general multi-notification command behavior.

| Build | Median completed commands/s | Range | Median main-thread CPU/update |
|---|---:|---:|---:|
| no_job | 28,337 | 26,761–29,172 | 35.25 µs |
| noop_job | 27,008 | 26,474–28,884 | 37.00 µs |

No-op/no-job ratio of medians: -4.69%. Paired changes: -9.25%, +2.37%, -0.22%, -5.33%, -4.40%. Four of five pairs were slower, one faster; ranges overlap. Median main-thread CPU/update increased by 1.75 µs. This supports job overhead as a candidate, but variation prevents assigning the entire original regression to it. Separate linking can also alter code layout; this is not a same-binary runtime toggle.

### CPU profiles (excluded from throughput comparisons)

Three user-space main-thread cycle profiles per build, 65,536 updates/profile, sampled at 997 Hz. No samples were reported lost. All three builds consumed approximately one full main core. Generator CPU use in timed no-op trials stayed below 0.155 cores, so the generator was not saturated.

Weighted main-thread cycles/update: pre-PR 62,655; current 63,546 (+1.42%); before-sleep 60,958. Profile overhead makes profiled throughput unsuitable for comparison with the unprofiled sweep. Cycle profiles identify the main thread as the limiting resource, not a specific full causal stack.

Current self samples include JsonUnquote 10.75%, allocator memory-size accounting zmalloc_used_memory 4.94%, and allocator/atomic operations. JsonUnquote was already 10.80% in pre-PR: a large existing cost is not proof of a new regression. Named job/context/cleanup functions individually occupy small self-sample fractions, but their called allocation and atomic work is counted elsewhere. Refcount/atomic helpers cannot be attributed to shared_ptr alone.

User-space DWARF stack capture was attempted without altering perf_event_paranoid or system settings. Capture succeeded, but offline unwinding did not resolve caller chains reliably (ENOENT despite mapped binaries and copied container libraries). Consequently this report uses symbolized instruction samples only and does not claim inclusive job/refcount costs. Raw stack captures are retained for follow-up.

### Verification and limits

25 unprofiled trials completed 1,802,240 checked updates, with 0 response errors and no final-state/index validation failures. Nine instruction-profile and three stack-profile runs each validated another 65,536 updates. This is a narrow causal experiment, not a full regression suite or a substitute for the completed index-count sweep. The diagnostic builds deliberately restore longer ownership holds, so benchmark success does not make them suitable production changes.

Earlier sweep results remain valid observations: JSON5 -6.6%, JSON10 -5.2%, all five paired results adverse. This new smaller gap demonstrates environmental/process-placement sensitivity; it does not erase those observations. No definitive attribution to shared_ptr, cleanup jobs, or record-lock contention is established. Keep the current correct lifecycle; do not introduce production complexity based on these uncertain measurements.

Sources and artifacts: run_experiment.py/results.jsonl; run_noop.py/noop-results.jsonl; profile-summary.json and symbolized reports; copied diagnostic sources and build scripts; binary-sha256.json. Original production sources remain unchanged by this investigation. git diff --check passes.

## Appendix D: provenance and earlier verification

Across the primary comparison and the two intermediate comparisons, the sweep contains 1,450 trials, 145 comparison cells, and 39,731,200 logical operations, with zero benchmark errors. The primary comparison contributes 680 trials and 19,578,880 operations. The before-sleep comparison contributes another 680 trials and 19,578,880 operations. The eager post-notification comparison contributes 90 trials and 573,440 operations.

The separate JSON investigation adds 25 timed trials and 1,802,240 validated updates. These totals are separate from the primary comparison.

### Build identifiers

- The pre-PR parent is `6d1eb2667d214c4841a8c5c59b249b02bbbdd7b8`, with the old SDK reference tracker. This is a pinned baseline, not a freshly fetched main revision.
- The full sweep uses `.audit/pr1640-preserve-deferred-libsearch.so`, with SHA256 `6fe877b0610028c9db4a0cc1653b1771ba3f3d80a64963d62ddf2baa8562e70c`.
- The final renamed Release build is `.audit/pr1640-client-blocking-context-libsearch.so`, with SHA256 `a96f17ffb1d7e0f65c0fbce8e1e538a0f2127e5f66afab45db536142d407d30d`.
- The earlier eager post-notification artifact has a SHA256 beginning `24c43eb6`.

No builds or style checks ran during the full sweep. The measured module remained unchanged. The subsequent rename changes identifiers but not the measured behavior.

### Earlier full-suite result

The preceding post-notification snapshot had 984 Python test passes, 11 skips, and one unresolved noeviction failure. The failure did not reproduce in 15 isolated runs. That result is not a green full-suite result for the final candidate.

### Earlier CPU and wait-channel profiles

Separate `/proc` CPU/wait-channel profiles are excluded from timed medians. The generator uses approximately **0.03–0.41 cores** across its eight available CPUs, so it is not the measured limiter.

- **Actual pre-PR JSON5:** a separate pair measures 28,450→27,910 commands/second, main CPU/update **34.79→36.01 µs**, with approximately one saturated main-thread core. This supports some main-thread cost but does not attribute it to a particular function or account precisely for the main sweep's −6.6%.
- **Actual pre-PR JSON10:** a separate pair reverses direction, 16,588→17,913 commands/second, CPU/update 294.80→277.10 µs. This exposes process/scheduling variation; it does not erase the five adverse timed pairs.
- **Before-sleep JSON5:** 28,533→25,535 commands/second, main CPU/update **35.40→39.06 µs**, approximately one main-thread core. Exact function-level cause remains unresolved.
- **Actual pre-PR HSET1:** 71,876→73,580 commands/second with roughly one main-thread core and equal rounded main CPU/command, 13.43 µs. The actual baseline comparison is close here.
- **Before-sleep HSET1:** 78,703→69,912 commands/second in the separate pair, approximately main-thread limited; short-run CPU-counter granularity does not isolate the source of the cost. The five-pair main sweep confirms the loss.
- **Transactions:** against before-sleep, two-schema main CPU/group drops **28.08→9.77 µs** and throughput rises 32,750→41,660 groups/second. At five schemas, CPU/group drops **178.22→155.03 µs** but throughput falls 21,162→18,801; main-thread futex samples rise **50→263** at the same operation count. Batching performs less CPU work but spends more time waiting and uses less available concurrency.

No extra deferred-record eligibility lookup exists in the measured implementation: eligibility, merge and acquisition use the existing tracking mutex/map lookup. A proposed queue-empty shortcut based on an assumed second lookup was withdrawn after inspecting the current method. The profiles provide resource-level evidence, not a syscall count or function-level attribution.

### Raw results

The development workspace retains `preserve-deferred-{pre_pr,before_sleep,postjob}-{macro,micro}.jsonl`, the corresponding summary JSON files, `preserve-deferred-profile-results.json`, and `preserve-deferred-metadata.json`. The tables in this note summarize those runs.

The following command reproduces the primary trial, operation, and error counts from the retained raw data:

```bash
python3 - <<'PYCOUNT'
import json
from pathlib import Path

rows = [
	json.loads(line)
	for mode in ("macro", "micro")
	for line in Path(
		f".audit/benchmarks/preserve-deferred-pre_pr-{mode}.jsonl"
	).read_text().splitlines()
]
print(len(rows), sum(r["completed_operations"] for r in rows),
	  sum(r["errors"] for r in rows))
PYCOUNT
```

The output is `680 19578880 0`.
