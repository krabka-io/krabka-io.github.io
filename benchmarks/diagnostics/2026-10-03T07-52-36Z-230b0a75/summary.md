# Full curve collection: 71 passed, 1 failed

Run: 2026-10-03T07-52-36Z-230b0a75. All 72 planned trials were attempted.

The run failed delivery validation and was not published to the website benchmark history. All 36 latency/load trials and all 27 memory-budget trials passed. Kafka and Redpanda passed all three recovery trials; Krabka passed two and failed one.

Krabka 0.6.1 recovery repetition 1 delivered all 1,300,000 unique records with zero producer errors, but observed 59 duplicate sequence numbers despite producer idempotence being enabled. The chart export explicitly marks that trial as failed.

The producer blocking deadline was corrected to 30 seconds for every vendor, allowing backpressure during the ten-second pause with the same 32 MiB buffer. Earlier attempts and the separate recovery precheck remain local under .benchmarks; the precheck also observed a Krabka delivery-timeout failure.

Numbers below are medians of passing trials only. The passed column makes missing valid repetitions explicit. These are shared-host, buffered-write measurements with 1 KiB random LZ4 records. Recovery tests pause/resume. Summary throughput uses intervals classified by their start, so boundary intervals may include warm-up catch-up and exceed the offered rate. P99 includes drain for records scheduled after warm-up. Recovery time confirms three consecutive intervals meeting throughput and lag thresholds.

| Case | System | Passed | Ack records/s | End-to-end p99 ms | End offered backlog | Recovery confirmation seconds |
|---|---|---:|---:|---:|---:|---:|
| latency-20000 | krabka | 3/3 | 19981.89 | 45.85 | 0.00 | — |
| latency-20000 | kafka | 3/3 | 19977.97 | 5.97 | 0.00 | — |
| latency-20000 | redpanda | 3/3 | 19982.43 | 3.31 | 0.00 | — |
| latency-100000 | krabka | 3/3 | 99665.17 | 393.98 | 0.00 | — |
| latency-100000 | kafka | 3/3 | 99665.76 | 649.22 | 4.00 | — |
| latency-100000 | redpanda | 3/3 | 99903.46 | 2.63 | 2.00 | — |
| latency-250000 | krabka | 3/3 | 255349.38 | 622.08 | 0.00 | — |
| latency-250000 | kafka | 3/3 | 249159.93 | 361.21 | 10.00 | — |
| latency-250000 | redpanda | 3/3 | 249677.01 | 4.98 | 6.00 | — |
| latency-500000 | krabka | 3/3 | 379985.31 | 9068.54 | 4489518.00 | — |
| latency-500000 | kafka | 3/3 | 506663.00 | 3731.45 | 584835.00 | — |
| latency-500000 | redpanda | 3/3 | 498776.88 | 927.23 | 331902.00 | — |
| memory-2g | krabka | 3/3 | 367809.43 | 621.05 | 0.00 | — |
| memory-2g | kafka | 3/3 | 519994.11 | 367.62 | 0.00 | — |
| memory-2g | redpanda | 3/3 | 529679.17 | 83.58 | 0.00 | — |
| memory-4g | krabka | 3/3 | 388282.14 | 499.20 | 0.00 | — |
| memory-4g | kafka | 3/3 | 523679.26 | 544.25 | 0.00 | — |
| memory-4g | redpanda | 3/3 | 581622.21 | 70.40 | 0.00 | — |
| memory-8g | krabka | 3/3 | 357957.02 | 662.53 | 0.00 | — |
| memory-8g | kafka | 3/3 | 491290.14 | 613.38 | 0.00 | — |
| memory-8g | redpanda | 3/3 | 521530.01 | 78.91 | 6251.00 | — |
| recovery | krabka | 2/3 | 19952.96 | 9535.49 | 0.00 | 4.31 |
| recovery | kafka | 3/3 | 19976.10 | 9560.06 | 0.00 | 3.79 |
| recovery | redpanda | 3/3 | 19986.59 | 31.97 | 0.00 | 3.56 |

collection.json.gz retains successful trial data and failure metadata. charts.json.gz retains all 72 outcomes. The recovery-failure directory retains the failed trial telemetry, container inspections, and logs; provenance.json contains image digests, source hashes, host resources, allocations, and failed-run status. The failed resource samples have elapsed times but lack the resource-window UTC start; their workload/fault UTC timestamps are available.

Verified all 72 case/vendor/repetition combinations, 11282 broker-resource samples, and 2804 workload intervals. Passing summaries match raw captures; source hashes and enforced Docker CPU/memory limits match provenance.

Gzip files contain the original JSON/JSONL bytes and can be read with `gzip -dc FILE`. SHA256SUMS verifies the archived files. Successful per-trial resource and workload timelines are embedded in collection.json.gz. The failed point, workload timeline, events, and raw resource samples are included in charts.json.gz. These diagnostic files are separate from benchmarks/results and do not update a latest report.
