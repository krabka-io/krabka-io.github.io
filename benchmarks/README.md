# Local broker comparison

`npm run benchmark` compares published Krabka, Kafka **4.3.1**, and Redpanda containers on the local host. It runs six producer/consumer cases at RF1 and RF3, with three independent repetitions and a rotating vendor order. A complete run writes dated machine-readable results and a Markdown report under `results/`, then replaces `latest.md`. Results are local measurements, not production qualification.

The checked-in local collections are from Krabka **0.7.0** and have not been re-run for 1.0.0. They passed all 108 [throughput trials](latest.md) and all 72 [latency, memory, and recovery trials](latest-curves.md). Both reports link to dated provenance and per-trial time series; the curve collection also includes `charts.json` for later website rendering.

```sh
# Check prerequisites and resolve immutable image references without starting containers
npm run benchmark -- --dry-run

# Exercise all three vendors, both topologies, and every case with small counts
# Smoke runs never update checked-in results
npm run benchmark -- --smoke

# Run the full 108-trial matrix and update repository results
npm run benchmark

# Capture latency/load, throughput/memory, and recovery curves
npm run benchmark -- --suite curves --dry-run
npm run benchmark -- --suite curves --smoke
npm run benchmark -- --suite curves

# Compare a different published Krabka or Redpanda image (tag or registry digest)
npm run benchmark -- --krabka-image ghcr.io/krabka-io/krabka-broker:v1.0.1 \
  --redpanda-image docker.redpanda.com/redpandadata/redpanda:v26.2.2

# Focused measurement and publication checks, without Docker
npm run test:benchmark
```

## OpenMessaging on Google Cloud

The manual **OpenMessaging broker benchmarks** Actions workflow uses the organization's
[Cyclenerd Google Cloud runners](https://github.com/Cyclenerd/google-cloud-github-runner).
Its default label, `gcp-ubuntu-24-04-16core`, requests an ephemeral Linux/amd64 VM
with 16 vCPUs, 64 GB RAM and a 600 GB SSD under the upstream default template.
`gcp-ubuntu-24-04-32core` is also selectable. Actual resources are checked and recorded;
customized organization templates must satisfy the prerequisites below.

```sh
gh workflow run openmessaging.yml -f mode=full
# Shorten all 13 workloads to one minute at 5,000 messages/s, without backlog
gh workflow run openmessaging.yml -f mode=smoke
# Run a narrower workload or repeat the comparison three times
gh workflow run openmessaging.yml -f mode=full \
  -f workloads=1-topic-16-partitions-1kb -f repetitions=3

# The same runner works on a prepared Linux host; no npm install is needed
npm run benchmark -- --suite openmessaging --dry-run
npm run benchmark -- --suite openmessaging --smoke --workloads simple-workload
npm run benchmark -- --suite openmessaging --workloads 1-topic-16-partitions-1kb
```

Defaults are all three vendors, RF1/RF3, and one round; select `replication_factors`
and `repetitions` in Actions or `--replication-factors` and `--repetitions` locally.
Only `workflow_dispatch` triggers this workflow. It splits the run into shards,
one per workload and replication factor, and spreads them over four lanes (two
with the 32-core template). Each lane is one runner VM that runs its shards one
after another, with each 100 GB backlog shard on a different lane. Cyclenerd
creates a VM for each queued job and does not retry one it could not create, and
the Google Cloud project has room for about four 16-core VMs, so a job per shard
left most shards without a runner. All three vendors for a shard, and every round
of them, run on one VM, so each comparison stays on one machine; shards never split
vendors. Every trial gets fresh broker storage, and vendors run sequentially on the
same VM with the same CPU affinity, 4 CPU quotas, 10 GiB memory limits, and a
separate 4 GiB client. Multiple rounds rotate vendor order. Each shard has a
24-hour deadline; individual workloads
have their upstream duration plus 30 minutes for startup and sustainable-rate probing,
or 90 extra minutes for a full backlog fill/drain (five extra minutes in smoke).
Deadlines remain failures. Smoke is a wiring check, not a performance
measurement of the original workload.

During backlog fill, upstream OMB blocks the consumer's message callback. Filling
100 GiB at 100,000 messages/s takes about 17½ minutes, exceeding the Kafka client's
default five-minute poll interval and causing it to leave the group. Full backlog
trials set `max.poll.interval.ms` to the workload's complete timeout, identically
for all vendors, to allow that intentional pause. The effective driver YAML and
`timeout_ms` are retained in artifacts; client errors still fail validation. Backlog
size, offered rate, warm-up and post-drain duration are preserved.

Set `krabka_comparison_image` to a control image and `krabka_image` to a candidate
to compare them on the same worker in control/candidate/candidate/control order.
Each invocation runs all three vendors with fresh storage and the selected workload,
RF and repetition settings. With `repetitions=1`, each Krabka image gets two rounds;
the four invocations retain separate reports and provenance. The series stops after
a failed invocation, and its diagnostics remain available. For example:

```sh
gh workflow run openmessaging.yml -f mode=full \
  -f workloads=max-rate-1-topic-16-partitions-1kb -f replication_factors=3 \
  -f repetitions=1 -f krabka_comparison_image="$CONTROL_IMAGE" \
  -f krabka_image="$CANDIDATE_IMAGE"
```

The runner builds [OpenMessaging commit 5b1fa709](https://github.com/openmessaging/benchmark/tree/5b1fa70951a323da26bd587174b58bb2c65b0b5c)
with an immutable Maven/JDK 17 image. It runs the upstream Kafka driver and its
**Kafka 3.6.1 client against every broker**, including the Kafka **4.3.1 server**.
One driver change is applied, [`omb-kafka-coalesce-commits.patch`](omb-kafka-coalesce-commits.patch),
identically for every broker. Upstream's consumer sends an async offset commit after
every poll without waiting for the previous one. A fast drain then queues thousands
of commits per second in the client until they expire with "Failed to send request
after 30000 ms", which failed RF3 backlog and maximum-rate trials for Kafka and Krabka
alike ([openmessaging/benchmark#270](https://github.com/openmessaging/benchmark/issues/270)
reports the same). The patch keeps at most one commit in flight and folds offsets
polled meanwhile into the next one, as Redpanda's fork does
([redpanda-data/openmessaging-benchmark#37](https://github.com/redpanda-data/openmessaging-benchmark/pull/37)).
A commit that still fails is logged as an error and fails the trial. The patch's
SHA-256 is in provenance. The common configuration follows upstream
`kafka-exactly-once.yaml`: idempotence, `acks=all`, one in-flight request, 1 MiB
batches and 1 ms linger, with no compression. This means idempotent production,
not transactional exactly-once application processing. Topic retention is unlimited,
except that a backlog workload's topic keeps 1.2x its backlog (`retention.bytes`
split evenly across its partitions, so 120 GiB per topic for the 100 GB cases).
OMB keeps producing while the backlog drains, so unlimited retention would store
every byte published three times on the one RF3 disk; the backlog only shrinks
once it is filled, so the limit deletes only records the consumer has read.
Kafka/Krabka use minISR1/minISR2, while Redpanda uses Raft majorities and
`write.caching=true`. This measures buffered writes, without a claim of identical
crash durability. OMB creates topics and consumers; the existing Kafka 4.3.1 admin
client checks cluster readiness before each trial.
`OpenMessagingMain.java` calls upstream `main` and exits when it returns, because
upstream's Kafka topic creator leaves a non-daemon scheduler alive. It does not
change workload execution or treat missing result files as success.

The default catalog is the 13 workloads listed in the linked
[OpenMessaging documentation](https://openmessaging.cloud/docs/benchmarks/):

| Workload name for `workloads` / `--workloads` |
|---|
| `simple-workload` |
| `1-topic-1-partition-1kb` |
| `1-topic-1-partition-100b` |
| `1-topic-16-partitions-1kb` |
| `backlog-1-topic-1-partition-1kb` |
| `backlog-1-topic-16-partitions-1kb` |
| `max-rate-1-topic-1-partition-1p-1c-1kb` |
| `max-rate-1-topic-1-partition-1p-1c-100b` |
| `1-topic-3-partition-100b-3producers` |
| `max-rate-1-topic-16-partitions-1kb` |
| `max-rate-1-topic-16-partitions-100b` |
| `max-rate-1-topic-100-partitions-1kb` |
| `max-rate-1-topic-100-partitions-100b` |

The pinned upstream `1m-10-topics-1-partition-100b` workload is also available
when selected explicitly. It uses ten topics with one partition each, randomized
100-byte payloads, a fixed offered rate of 1,000,000 messages/s and a 15-minute
measurement. Select it with `-f workloads=1m-10-topics-1-partition-100b` in Actions
or `--workloads 1m-10-topics-1-partition-100b` locally. It supports the same RF,
image comparison and repetition settings.

Two single-partition maximum-rate filenames now include `1p-1c` upstream. Full
runs retain upstream payloads, rates, partition counts, backlog sizes, and durations;
only the payload path is adapted to the container mount. Full runs warm up for the
upstream default one minute. OMB may additionally probe sustainable rates when
`producerRate=0`. The default matrix is 78 trials and takes many hours. The
100 GB backlog cases need **540 GiB free disk** in both the checkout filesystem and
Docker storage; full runs without backlog need 150 GiB, and smoke needs 4 GiB.
At RF3 that covers three replicas of the retained 120 GiB, the up to five minutes
of writes a broker accumulates between retention checks, segment granularity, record
overhead and the 20 GiB abort reserve below.
Maximum-rate cases have no record ceiling and can outgrow a fixed disk. The runner
monitors both filesystems and aborts with diagnostics before free space falls below
20 GiB (1 GiB in smoke). Choose a larger worker or narrower matrix if this occurs.

Each lane uploads its shards in one `openmessaging-<run>-<attempt>-lane-<N>` artifact.
A final `merge` job (`scripts/benchmark-openmessaging-shards.mjs merge`) checks that
the shards ran identical images, mode, rounds, contract and OMB build, and that every
workload × RF × vendor × round appears exactly once. It then writes one run in the
single-runner layout to the `openmessaging-<run>-<attempt>` artifact. That run's
`provenance.json` keeps each shard's host, runner and CPU sets under `shards`; the
top-level host fields are the first shard's. A failed, cancelled or incomplete shard
fails the merge. A `krabka_comparison_image` series keeps its four reports per shard
and is not merged.

Actions retains `provenance.json`, effective/upstream YAML, immutable image and
source references, runtime jar hashes, raw OMB JSON, broker inspections/logs,
250 ms CPU/memory time series, and failure diagnostics for 30 days. A complete
matrix also produces `summary.md` and a job summary with publish/consume rates,
publish/end-to-end p99 latency, CPU seconds and peak RSS. Resource windows include
startup of the benchmark client, warm-up, probing, measurement and shutdown.
OMB does **not** check exact delivery counts or duplicate sequences. Missing or
truncated result series, zero publish/consume traffic, publish errors, OOMs,
logged client/consumer errors and missing resource counters fail the run,
including when upstream exits zero after
catching a workload exception. Failed trials are recorded and the remaining matrix
is attempted; failed matrices do not produce a successful summary. Artifacts stay
under `.benchmarks/<run-id>/`; the runner never rewrites `latest.md`, commits or pushes.

To publish a complete full-mode run on the website, download its `openmessaging-<run>-<attempt>`
artifact and run `node scripts/publish-openmessaging.mjs <artifact>/<run-id>`. The script
rejects smoke, failed and incomplete matrices, re-validates every trial, and writes
`benchmarks/openmessaging/<run-id>/` (provenance, summary and a compact `trials.json` without raw
time series) and `latest-openmessaging.md`, which the `/benchmarks` page follows. Review
and commit those files; the raw artifact stays in Actions for 30 days.

## Local runner prerequisites

- Node >=22.12 and JDK >=17 (`java` and `javac`). No Bazel build, sibling checkout, npm dependencies, or Docker Compose is needed.
- Native Linux/amd64 with a local Docker daemon, cgroup v2, readable `/proc` and cgroup resource counters, CPU affinity/quota and memory/swap limits enabled. Remote Docker, Docker Desktop, and emulated images are rejected.
- At least **14 available logical CPUs** and **34 GiB available RAM** (three 10 GiB brokers plus a 4 GiB client). Each broker receives 4 logical CPUs, a 4 CPU quota, 10 GiB RAM, zero swap, and a 131,072 open-file limit. RF3 runs three brokers; the client receives the remaining CPUs and a separate 4 GiB memory limit. SMT siblings are grouped where possible and the topology is recorded.
- The **curves suite** requires the same CPUs but only **20 GiB available RAM**: at most 12 GiB of brokers plus the 4 GiB client and 4 GiB host headroom. It requires 150 GiB free disk for full runs or 24 GiB for smoke. This fits the current 16-logical-CPU host with about 61 GiB total RAM. Only one vendor/cluster runs at a time; each case gets fresh storage, which is removed before the next case. Other containers are left running.
- At least **150 GiB free disk** for a full run, or **4 GiB** for smoke, both at the checkout and Docker storage. Data uses Docker volumes and is deleted after each cluster repetition; measurements and logs remain.
- At least **24,728 available Linux AIO slots** (`fs.aio-max-nr - fs.aio-nr`). Redpanda networking AIO is explicitly limited to 1,024 control blocks per shard so three nodes fit the usual 65,536-slot host limit consistently. This setting and the host limit are recorded; no sysctl is changed.
- Registry access to `ghcr.io`, Docker Hub, and `docker.redpanda.com`; access to `raw.githubusercontent.com` for the pinned Java workload. Tags are pulled and resolved to registry digests once before running. The defaults are Krabka v1.0.1, Kafka 4.3.1, and Redpanda v26.2.2; versions never silently advance.

The runner owns only its uniquely named and labeled containers, volumes, and networks. It does not change host settings, stop other containers, commit, push, or deploy. Avoid other heavy work while measuring. Concurrent invocations in the same checkout fail; a hard-killed process can leave `.benchmarks/runner.lock`, which contains its PID and run ID. Inspect that process and its labeled Docker resources before manually removing a stale lock.

## Contract

Every topic has 12 partitions, `retention.ms=-1`, and `retention.bytes=-1`. RF1 uses one combined broker/controller; RF3 uses three. Kafka/Krabka enforce minISR1 and minISR2 respectively. Redpanda uses Raft majorities of one and two, and does not expose Kafka's minimum ISR as an enforced topic config. The same Kafka 4.3.1 admin client waits for all replicas in ISR before each workload. The producer uses `acks=all`, idempotence, 65,536-byte batches, and 5 ms linger; the consumer checks every sequence and exact acknowledged counts. Random payloads use the upstream workload's seed 42 and 4 MiB pool.

| Case | Bytes | Payload | Compression | Records | Rate |
|---|---:|---|---|---:|---:|
| 1k-random-lz4 | 1,024 | random | LZ4 | 10,000,000 | unlimited |
| 100b-random-lz4 | 100 | random | LZ4 | 10,000,000 | unlimited |
| 1k-zeros-lz4 | 1,024 | zeros | LZ4 | 10,000,000 | unlimited |
| 1k-random-none | 1,024 | random | none | 5,000,000 | unlimited |
| 100k-random-lz4 | 102,400 | random | LZ4 | 60,000 | unlimited |
| 1k-random-20k | 1,024 | random | LZ4 | 600,000 | 20,000/s |

Each fresh cluster warms up with 3 million 1 KiB random LZ4 records for RF1, or 1 million for RF3. Warm-up is excluded. Cases run in table order; their topics and disk cache remain until the cluster repetition finishes. Smoke uses one repetition, 2,000 warm-up records, 2,000 records per case except 200 for the 100 KiB case, with the same resource limits and replication settings.

Kafka uses `-Xms1g -Xmx1g`. Redpanda uses four shards, 8 GiB application memory, and 1 GiB reserved memory, with explicit configuration and `--overprovisioned` on the shared host. Its user topics set [`write.caching=true`](https://docs.redpanda.com/streaming/current/develop/manage-topics/config-topics/); the runner avoids `dev-container` mode and `unsafe-bypass-fsync`. The comparison is buffered throughput: Kafka/Krabka use in-sync replication acknowledgments, while Redpanda uses Raft majority acknowledgments. This does not claim identical failure behavior or durable disk writes before acknowledgment.

The producer/consumer source is downloaded unmodified from [krabka-broker commit c27ed4f7](https://github.com/krabka-io/krabka-broker/blob/c27ed4f7e2c9ba4bf48376ed674bc26b166353f4/packaging/performance/BrokerPerformanceWorkload.java) (Apache-2.0) and verified against its SHA-256 before compilation. Both Java classes compile against the exact Kafka 4.3.1 image jars, which are used for every vendor. Source, runner, and jar hashes are retained in provenance.

## Curves suite

`--suite curves` runs 72 trials (eight cases × three vendors × three repetitions) with rotating vendor order. Full cases use five seconds of warm-up and 30 seconds of measurement, except recovery, which measures 60 seconds. Expect roughly an hour or more including cluster startup and drain; readiness, client execution, drain, and replica recovery all have deadlines. Smoke runs all 24 vendor/case combinations once with two seconds of warm-up and three seconds of measurement (16 seconds for recovery), and never publishes.

Curve failures retain a `failure.json` beside raw telemetry and logs, then the runner cleans up and attempts the remaining cases. Any failed case makes the overall run fail and prevents publication; `collection.json` retains successful trials and the failure list under `.benchmarks/`. Interruptions and cleanup failures still stop immediately. Producer errors include the first ten exception traces and final counters.

| Chart | Cases | RF | Memory per broker |
|---|---|---|---|
| p99 latency versus offered throughput | 20k, 100k, 250k, 500k scheduled records/s | 1 | 4 GiB |
| Throughput versus memory budget | Unlimited offered load at 2, 4, 8 GiB | 1 | 2/4/8 GiB |
| Throughput/lag during recovery | 20k records/s; pause the broker leading partition 0, then resume it | 3 | 4 GiB |

All cases use 1 KiB seeded random payloads, LZ4, 12 partitions, `acks=all`, idempotence, 64 KiB batches, and 5 ms linger. Brokers have four logical CPUs each; SMT placement and client affinity are recorded. The client has a 4 GiB cgroup limit and 2 GiB heap. Kafka's heap is one quarter of its cgroup budget, capped at 1 GiB. Redpanda's application allocation is three quarters of its budget, with one eighth reserved and one eighth left as headroom, at four shards. Exact allocations and Docker inspections are retained. These settings define this comparison, not minimum vendor requirements or an exhaustive tuning search.

The local `BenchmarkTimeline.java` uses the same Kafka 4.3.1 jars as the original driver, including the bundled HDR Histogram jar. It uses bounded histograms (three significant digits, microseconds), a bounded sequence bitmap, a 32 MiB producer buffer, and a record ceiling of 64 million (16 million in smoke). Producer `max.block.ms=30000` allows backpressure during the ten-second recovery pause without requiring a larger buffer; request and delivery timeouts are 10 and 45 seconds. Reaching the ceiling fails the run rather than publishing a shortened case. The disk check allows for the maximum RF1 dataset and overhead; RF3 recovery is rate limited. No unbounded per-record latency array is used.

Rate-limited records carry their **scheduled send timestamp**, so acknowledgment and end-to-end latency include producer pacing/backpressure delay. The driver stops scheduling after the configured duration and allows up to 60 seconds to drain submitted records. It reports intended offered rate separately from actual submitted, acknowledged, and consumed counts. Offered backlog includes scheduled records that never reached the producer before the duration expired; it can remain nonzero after submitted records drain. Latency describes submitted records, so inspect that backlog before treating a point as sustainable throughput. Unlimited memory cases timestamp actual generation and make no fixed-rate claim.

`workload_time_series` retains a UTC start and one-second intervals with actual elapsed times, interval duration, warm-up/measurement/drain phase, achieved acknowledgment and consumption rates, cumulative counters, p99 acknowledgment and end-to-end latency, latency sample count, client CPU cores, cumulative client CPU seconds, and heap bytes. Empty latency intervals are `null`. `consumer_lag_records` is `max(0, acknowledged - consumed)`, a workload backlog count, not committed consumer-group offset lag. Summary throughput averages actual measurement intervals; intervals are classified by their start time, so boundary intervals can include part of a neighboring phase. Summary p99 uses all records scheduled after warm-up, including their drain latency; interval p99 values are never averaged. Broker resource samples have their own start timestamp and include startup, warm-up, measurement, and drain.

Recovery pauses the actual leader of partition 0 after 15 measured seconds for at least ten seconds, then resumes the same container and storage. Smoke pauses after four measured seconds for at least five seconds. `events` records actual pause/unpause completion times, UTC timestamps, container/broker identity, and affected leader partitions on the workload clock. Resource sampling continues while the process is paused; counters remain comparable across resume. The run requires exact final acknowledged/consumed counts, no duplicates/errors, and all replicas back in ISR afterward. Recovery time is the first three consecutive post-resume intervals with at least 90% of offered acknowledgment throughput and at most 100 ms worth of acknowledged backlog; it is `null` when recovery under that definition is not observed. This tests a process stall on a shared host, not a machine failure, restart, or disk durability.

Complete full runs retain all trial JSON and a `charts.json` containing `latency_vs_offered_throughput`, `throughput_vs_memory_budget`, and recovery timelines/events, ready for later website rendering. The new summary is `latest-curves.md`; the existing `latest.md` remains the original throughput suite. Publication rejects incomplete matrices, altered budgets, missing telemetry/fault events, counter inconsistencies, delivery errors, and missing replica recovery. Diagnostics from failed/smoke runs stay under `.benchmarks/`.

## Measurements and publication

Per-trial results contain records/s, logical MiB/s, p50/p95/p99 latency, aggregate broker CPU seconds and CPU µs/acknowledged record, and observed peak RSS, anonymous memory, and working set. CPU and memory cover client startup through shutdown, while workload throughput and latency exclude client initialization. The client and admin processes run outside broker cgroups. Working set is `memory.current - inactive_file`. Sampling targets 250 ms; actual gaps are reported. RF3 memory peaks use simultaneous cluster sums, not the sum of each broker's individual maximum.

Successful reports show medians and min–max ranges across three repetitions. They identify image references and IDs, host resources and SMT topology, CPU sets, source/client hashes, topic configurations, resource budgets, case order, warm-up, and measurement windows. TLS, authentication, tiered storage, compaction, forced per-append fsync, restarts, and failure injection are outside the workload. Shared-host noise and client bottlenecks can affect results.

Each per-trial JSON also retains a versioned `time_series` for future website charts:

- `started_at`: UTC start of the resource measurement window. Add each sample's `elapsed_ms` to this timestamp for its wall-clock time; elapsed times use a monotonic clock.
- `sampling_interval_ms`: the 250 ms target. Every actual sample time is retained, including longer gaps and the final partial interval; samples are not resampled or interpolated.
- `samples[].brokers`: broker IDs and raw CPU usage in microseconds, RSS, anonymous memory, cgroup current memory, inactive file cache (all in bytes), and OOM kill counters.
- `samples[].cluster`: simultaneous sums of CPU usage, RSS, anonymous memory, and working set, plus CPU seconds since the first sample and average CPU cores used during the preceding actual interval. The first sample's `cpu_cores` is `null` because it has no preceding interval. Four cores fully used read as `4`, not `100` percent.

The summary and timeline use the same samples, and publication rejects missing timelines or inconsistent summaries. Trial vendor, RF, repetition, case, and topic configuration remain alongside the series; run-level host, image, and workload provenance remain in `provenance.json`. Warm-up series are retained locally and excluded from published measured trials. Throughput and latency are whole-workload summaries, not interval measurements. Older checked-in runs without `time_series` remain summary-only; their samples cannot be reconstructed from peaks.

Raw configurations, container inspections, commands, logs, workload JSON, and streaming resource samples live in gitignored `.benchmarks/<UTC-run-id>/`. Checked-in history contains provenance, one JSON per measured trial (including its resource timeline), and a summary. Only a complete full matrix can publish. Smoke, failed, interrupted, missing-counter, OOM, or delivery-invalid runs retain diagnostics and preserve the previous latest report. Ctrl-C triggers cleanup; the workload subprocess has a 16-minute outer deadline, with shorter deadlines for setup and readiness.
