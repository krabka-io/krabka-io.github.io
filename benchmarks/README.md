# Local broker comparison

`npm run benchmark` compares published Krabka, Kafka **4.3.1**, and Redpanda containers on the local host. It runs six producer/consumer cases at RF1 and RF3, with three independent repetitions and a rotating vendor order. A complete run writes dated machine-readable results and a Markdown report under `results/`, then replaces `latest.md`. Results are local measurements, not production qualification.

```sh
# Check prerequisites and resolve immutable image references without starting containers
npm run benchmark -- --dry-run

# Exercise all three vendors, both topologies, and every case with small counts
# Smoke runs never update checked-in results
npm run benchmark -- --smoke

# Run the full 108-trial matrix and update repository results
npm run benchmark

# Compare a different published Krabka or Redpanda image (tag or registry digest)
npm run benchmark -- --krabka-image ghcr.io/krabka-io/krabka-broker:v0.6.1 \
  --redpanda-image docker.redpanda.com/redpandadata/redpanda:v26.2.2

# Focused measurement and publication checks, without Docker
npm run test:benchmark
```

## Prerequisites

- Node >=22.12 and JDK >=17 (`java` and `javac`). No Bazel build, sibling checkout, npm dependencies, or Docker Compose is needed.
- Native Linux/amd64 with a local Docker daemon, cgroup v2, readable `/proc` and cgroup resource counters, CPU affinity/quota and memory/swap limits enabled. Remote Docker, Docker Desktop, and emulated images are rejected.
- At least **14 available logical CPUs** and **34 GiB available RAM** (three 10 GiB brokers plus a 4 GiB client). Each broker receives 4 logical CPUs, a 4 CPU quota, 10 GiB RAM, zero swap, and a 131,072 open-file limit. RF3 runs three brokers; the client receives the remaining CPUs and a separate 4 GiB memory limit. SMT siblings are grouped where possible and the topology is recorded.
- At least **150 GiB free disk** for a full run, or **4 GiB** for smoke, both at the checkout and Docker storage. Data uses Docker volumes and is deleted after each cluster repetition; measurements and logs remain.
- At least **24,728 available Linux AIO slots** (`fs.aio-max-nr - fs.aio-nr`). Redpanda networking AIO is explicitly limited to 1,024 control blocks per shard so three nodes fit the usual 65,536-slot host limit consistently. This setting and the host limit are recorded; no sysctl is changed.
- Registry access to `ghcr.io`, Docker Hub, and `docker.redpanda.com`; access to `raw.githubusercontent.com` for the pinned Java workload. Tags are pulled and resolved to registry digests once before running. The defaults are Krabka v0.6.1, Kafka 4.3.1, and Redpanda v26.2.2; versions never silently advance.

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

## Measurements and publication

Per-trial results contain records/s, logical MiB/s, p50/p95/p99 latency, aggregate broker CPU seconds and CPU µs/acknowledged record, and observed peak RSS, anonymous memory, and working set. CPU and memory cover client startup through shutdown, while workload throughput and latency exclude client initialization. The client and admin processes run outside broker cgroups. Working set is `memory.current - inactive_file`. Sampling targets 250 ms; actual gaps are reported. RF3 memory peaks use simultaneous cluster sums, not the sum of each broker's individual maximum.

Successful reports show medians and min–max ranges across three repetitions. They identify image references and IDs, host resources and SMT topology, CPU sets, source/client hashes, topic configurations, resource budgets, case order, warm-up, and measurement windows. TLS, authentication, tiered storage, compaction, forced per-append fsync, restarts, and failure injection are outside the workload. Shared-host noise and client bottlenecks can affect results.

Raw configurations, container inspections, commands, logs, workload JSON, and resource time series live in gitignored `.benchmarks/<UTC-run-id>/`. Checked-in history is compact: provenance, one JSON per measured trial, and a summary. Only a complete full matrix can publish. Smoke, failed, interrupted, missing-counter, OOM, or delivery-invalid runs retain diagnostics and preserve the previous latest report. Ctrl-C triggers cleanup; the workload subprocess has a 16-minute outer deadline, with shorter deadlines for setup and readiness.
