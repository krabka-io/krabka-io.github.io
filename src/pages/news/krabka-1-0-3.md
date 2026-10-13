---
layout: ../../layouts/ProseLayout.astro
title: krabka 1.0.3
description: "krabka-broker 1.0.3: faster cold backlog reads, controller admission and storage safety fixes, and OpenMessaging results against 1.0.1, Kafka 4.3.1 and Redpanda 26.2.2. Also covers 1.0.2."
lead: "krabka-broker 1.0.3 was tagged on 2026-10-12. It changes no on-disk format, so it upgrades from any 1.x release in place."
---

## Upgrading

1.0.3 changes no on-disk format and adds no setting. It upgrades from 1.0.2 or 1.0.1 in place and rolls back to 1.0.2 the same way. Before you roll back to 1.0.1, remove `log_tail_cache_size` and `--log-tail-cache-size` from any configuration or service invocation that sets them, because 1.0.1 refuses a `[runtime]` key or argument it does not know.

The image is `ghcr.io/krabka-io/krabka-broker:v1.0.3`, a signed multi-arch index (`sha256:ee5b77a5…`) for linux/amd64 and linux/arm64. The [quickstarts](/docs/quickstart) now deploy it.

## What's new in 1.0.2 and 1.0.3

There was no announcement for 1.0.2, so this one covers both releases. The [CHANGELOG](https://github.com/krabka-io/krabka-broker/blob/v1.0.3/CHANGELOG.md) has the full list.

**Opt-in direct I/O (1.0.2).** The topic setting `preallocate` and the broker default `log.preallocate` reserve `segment.bytes` of disk blocks when an active segment opens. On Linux 6.1 or later, where the filesystem reports direct-I/O alignment, those segments are also written with `O_DIRECT`, bypassing the page cache. The newest data stays in memory to serve reads; `log_tail_cache_size` sets how much, and the default is 1 MiB. In 1.0.3, queued Produce batches that fit in the active segment share one aligned write of up to 1 MiB, which cuts synchronous writes and block padding under load. Direct I/O is off by default. Whether it should become the default is an open question ([krabka-broker#1333](https://github.com/krabka-io/krabka-broker/issues/1333)).

**Topic settings that now work (1.0.2).** `compression.gzip.level`, `compression.lz4.level`, `compression.zstd.level`, `segment.jitter.ms`, `file.delete.delay.ms`, `flush.messages` and `flush.ms` were accepted before but did nothing. They now control the log, and topic overrides and broker defaults take effect through live configuration updates.

**Cold backlog reads (1.0.3).** In 1.0.2, a plaintext (`sendfile`) fetch found the boundaries of a run of large batches by reading 16 KiB at each one while holding the partition's log mutex. When the backlog was no longer in the page cache, each of those reads went to disk. In our benchmark runs that made 100 GB backlog drains much slower than in 1.0.1, and it held up appends to the same partition. 1.0.3 reads only the 61-byte batch header for a large batch and keeps buffered scans for runs of small batches ([krabka-broker#1359](https://github.com/krabka-io/krabka-broker/issues/1359)). Drain times are back to 1.0.1's or better.

**Controller admission (1.0.3).** CreatePartitions, legacy AlterConfigs, AlterPartition and leader elections go through the controller's authoritative planning and guarded submission, so a stale request can no longer overwrite newer assignments, leadership, ISR or configuration. A topic UUID delete or incremental config update meant for a deleted topic no longer affects a topic recreated with the same name.

**Storage and fetch safety (1.0.3)**, including:

- Request byte limits reserve capacity before a frame body is allocated and read.
- A fetch above the log end returns `OFFSET_OUT_OF_RANGE`; a fetch at the log end stays valid and empty.
- Scheduled retention keeps pending records in both tiers, and timestamp lookups after a restart use the durable deletion floor.
- Share groups fence a shard after a timed-out or cancelled state write until replay restores it, and ShareFetch reads retained diskless WAL and archived history.
- Remote snapshot, restore and backup reads check sizes and counts before they allocate. Concurrent backup captures no longer overwrite each other.
- OPA authorization rejects HTTP error responses and uses the Kafka plugin's v1.5.1 action schema. Resource-type ACLs follow Kafka's pattern dominance rules.

**Kafka 4.3.1 compatibility (1.0.2).** Fetch and ShareFetch byte limits, replication-quota progress, KIP-835 metadata health metrics, and RE2J regex syntax and size limits now behave as Kafka's do.

## Benchmarks

Every OpenMessaging Benchmark trial completed for krabka 1.0.3, Apache Kafka 4.3.1 and Redpanda 26.2.2 on the dedicated benchmark host. The workloads, client and resource limits are the same as for the 1.0.1 run. The results come from four runs on that host. The first run stopped part-way through replication factor 3, when a Redpanda maximum-rate trial reached the disk reserve. The remaining workloads ran again after we made space. One trial ran again because other work ran on the host during it. The provenance of the published run records the source run of each trial and the reason it was run again.

Compared with 1.0.1, krabka 1.0.3 published faster in most maximum-rate workloads, and most of all in the 1 KB ones. Its end-to-end p99 latency was lower or the same in every fixed-rate workload, and it drained both backlogs at both replication factors in the same time as 1.0.1 or less.

At replication factor 1, krabka had the highest maximum rate in both single-partition workloads, and its p99 latency was the same as Redpanda's in every fixed-rate workload. Redpanda had the highest rate in most multi-partition workloads. At replication factor 3, krabka's p99 latency in the two 1 KB fixed-rate workloads is still far above Redpanda's: it is higher than Kafka's in one of them and lower in the other.

These runs leave `preallocate` off, so krabka writes through the page cache. On the benchmark host, page-cache writers stall on ext4 journal commits under sustained load, and that causes most of the latency gap to Redpanda. The [methodology](https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/README.md) explains the write paths and lists the full contract and caveats.

## Links

- [GitHub release v1.0.3](https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.3)
- [GitHub release v1.0.2](https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.2)
- [CHANGELOG, 1.0.3 section](https://github.com/krabka-io/krabka-broker/blob/v1.0.3/CHANGELOG.md)
- [Benchmarks](/benchmarks#openmessaging) and [methodology](https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/README.md)
- [krabka 1.0.0 announcement](/news/krabka-1-0-0)
- [Release status of every krabka repository](/versions)
