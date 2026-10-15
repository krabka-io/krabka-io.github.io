---
layout: ../../layouts/ProseLayout.astro
title: krabka 1.0.5
description: "krabka-broker 1.0.5 reads a cold backlog in one read per fetch. 1.0.4 made direct-I/O writes asynchronous. Also OpenMessaging results for 1.0.5 against Kafka 4.3.1 and Redpanda 26.2.2."
lead: "krabka-broker 1.0.5 was tagged on 2026-10-14 and 1.0.4 on 2026-10-13. Neither changes an on-disk format, so both upgrade from any 1.x release in place."
---

## Upgrading

1.0.4 and 1.0.5 change no on-disk format and add no setting. They upgrade from any 1.x release in place, and roll back to 1.0.3 the same way. Before you roll back to 1.0.1, remove `log_tail_cache_size` and `--log-tail-cache-size` from any configuration or service invocation that sets them, as the [1.0.3 announcement](/news/krabka-1-0-3#upgrading) explains.

Two changes in 1.0.4 apply only to partitions with `preallocate=true`, which write with `O_DIRECT`:

- The default `log_tail_cache_size` is 32 MiB, up from 1 MiB. Each such partition can hold up to 32 MiB of its newest data in memory. Set `log_tail_cache_size` to keep the old value.
- After a crash, the active segment of such a partition can hold zeros between batches that were not yet synced, as page-cache writeback can leave. The tail recovery of every 1.x release cuts the file at the first bytes that do not decode as a batch, so a rollback reads these segments too.

The image is `ghcr.io/krabka-io/krabka-broker:v1.0.5`, a signed multi-arch index (`sha256:2a81d1ba…`) for linux/amd64 and linux/arm64. The [quickstarts](/docs/quickstart) now deploy it.

## What's new in 1.0.4 and 1.0.5

There was no announcement for 1.0.4, so this one covers both releases. The [CHANGELOG](https://github.com/krabka-io/krabka-broker/blob/v1.0.5/CHANGELOG.md) has the full list.

**Cold backlog reads (1.0.5).** A consumer that catches up on a backlog that is no longer in the page cache now reads it in one large read per fetch. Before, a `sendfile` fetch walked the batch headers of its window with one small synchronous read each. On uncached bytes, each read waited for the disk while the partition's log lock was held. This slowed the drain and held up the producer's appends to the same partition. A fetch now checks, without waiting, whether the page cache holds the start and the end of its window. If it does not, the fetch reads the window with one read. Cached fetches keep the zero-copy `sendfile` path.

**Asynchronous direct-I/O writes (1.0.4).** A partition with `preallocate=true` acknowledges a produce request when its `O_DIRECT` write is queued, not when the write is on the disk. A partition without `preallocate` acknowledges when its bytes are in the page cache, so the two now acknowledge at the same point. Shared write threads put up to four writes of a segment on the disk at the same time, and unwritten bytes stay in memory for reads. A write that fails fails every later append and sync of the segment. Before, the append waited for the write under the partition's log lock, so one slow device write stalled the producer, the followers and the consumers of the partition. At replication factor 3, this gave a long end-to-end latency tail ([krabka-broker#1371](https://github.com/krabka-io/krabka-broker/issues/1371)).

**Direct-I/O fetches (1.0.4).** A fetch from a segment that was written with `O_DIRECT` no longer takes the `sendfile` path, because those bytes are not in the page cache. The fetch takes the newest bytes from memory without a copy and older bytes in one large read. With the larger tail cache, a consumer that falls behind the producer of a `preallocate=true` partition catches up ([krabka-broker#1370](https://github.com/krabka-io/krabka-broker/issues/1370)).

Direct I/O stays off by default. It holds more memory per partition and reserves the disk blocks of each active segment. Whether it should become the default is an open question ([krabka-broker#1333](https://github.com/krabka-io/krabka-broker/issues/1333)).

## Benchmarks

OpenMessaging Benchmark ran krabka 1.0.5, Apache Kafka 4.3.1 and Redpanda 26.2.2 on the dedicated benchmark host, with the same workloads, client and resource limits as the 1.0.3 run. The runner now trims the SSDs before every trial. The drives were not trimmed during the 1.0.3 run, and all three brokers published faster on the trimmed drives. For this reason, compare a broker with the others in the same run, not with its own result in the 1.0.3 run.

77 of the 78 trials are published. Redpanda's replication factor 3 trial of `1-topic-3-partition-100b-3producers` filled the disk to the runner's reserve before its 15 minutes ended, so it is shown as not measured, with the reason.

In the maximum-rate workloads, krabka published faster than Kafka in nine of 14, and published more messages per CPU-second than Kafka in ten of 14. Redpanda had the highest rate in most multi-partition maximum-rate workloads. At replication factor 3, krabka's rate as a share of Kafka's in the same run went up in six of seven maximum-rate workloads, compared with the 1.0.3 run. In the single-partition 1 KB workload, it went from below Kafka's rate to level with it.

In the fixed-rate workloads at replication factor 1, all three brokers had the same end-to-end p99 latency. At replication factor 3, krabka's p99 in the two 1 KB fixed-rate workloads was lower than Kafka's, but it is still far above Redpanda's. These runs leave `preallocate` off, so krabka writes through the page cache, and page-cache writers on the benchmark host stall on ext4 journal commits under sustained load.

The backlog workloads report end-to-end latency, which includes the wait while the backlog fills. They do not report how long the drain took, so this post does not compare drain times.

The [benchmarks page](/benchmarks#openmessaging) has every result, and the [methodology](https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/README.md) gives the full contract and caveats.

## Links

- [GitHub release v1.0.5](https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.5)
- [GitHub release v1.0.4](https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.4)
- [CHANGELOG, 1.0.5 section](https://github.com/krabka-io/krabka-broker/blob/v1.0.5/CHANGELOG.md)
- [Benchmarks](/benchmarks#openmessaging) and [methodology](https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/README.md)
- [krabka 1.0.3 announcement](/news/krabka-1-0-3)
- [Release status of every krabka repository](/versions)
