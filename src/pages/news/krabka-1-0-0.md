---
layout: ../../layouts/ProseLayout.astro
title: Krabka 1.0.0
description: "krabka-broker 1.0.0: on-disk formats stay readable across 1.x, coordinator records match Kafka 4.3.1, and the krabka crates are on crates.io."
lead: "krabka-broker 1.0.0 was tagged on 2026-10-07. It is the first release with a compatibility promise for the data a broker writes."
---

## What 1.0 promises

From 1.0.0, **every 1.x broker reads every on-disk artifact that an earlier 1.x broker wrote**, so a cluster moves from 1.x to 1.y with a rolling upgrade and no reformat. The promise covers partition logs and their sidecars, metadata log segments and snapshots, `quorum-state`, `meta.properties`, the bootstrap files, the internal-topic record formats, and the object-store formats: tiered segments, WORM manifests, diskless WAL objects and backup captures.

A format change after 1.0.0 is gated the way Kafka gates a metadata change on `metadata.version`. A broker that supports a new format keeps writing the old one until the operator finalizes the feature level that introduces it, so you can roll back to an earlier 1.x build until you finalize. `krabka.version` is the feature krabka owns for its own format changes. Nodes advertise it at `[0, 1]`, and both levels mean the 1.0.0 formats. It starts at 0 because a cluster can mix in Kafka nodes, which support only level 0. Once every node is krabka, `kafka-features upgrade --feature krabka.version=1` finalizes level 1.

Every krabka-owned on-disk artifact now carries a version marker, and every reader refuses a version it does not know, or a layout without one, instead of skipping it, regenerating it or treating it as absent. A controller stops on a committed metadata record it cannot decode, as Kafka's fatal fault handler does; before, such records were logged at debug level and skipped.

The constraint on all of this is the Kafka wire protocol. Requests and responses still follow Kafka's version negotiation and the KIPs krabka implements; the on-disk promise does not change them. The promise also does not cover the Rust API (no semver promise for crate interfaces), configuration and command-line flags, which follow the changelog, or caches. [Persisted formats](https://github.com/krabka-io/krabka-broker/blob/v1.0.0/docs/persisted_formats.md) lists every artifact, its version marker, the rules for changing it and the gaps that remain.

## Upgrading from 0.x

**A data directory that a 0.x broker wrote is not covered.** Format it again with `krabka-format` before you start 1.0.0 on it, and restore the topic data. Several 1.0.0 changes would refuse a 0.x directory anyway:

- `krabka-format` writes Kafka's `meta.properties` in place of `meta.properties.json`, so the Kafka tools read a krabka directory. The broker refuses a directory that has only `meta.properties.json`.
- The metadata log moved to `__cluster_metadata-0` under the metadata log directory, as Kafka's is.
- The metadata layer refuses a krabka-private metadata record written before 0.6.0.
- `MetadataHash` in the group metadata records is now Kafka's hash, so a hash a 0.x broker wrote does not match.

## What's new since 0.7.0

The [CHANGELOG](https://github.com/krabka-io/krabka-broker/blob/v1.0.0/CHANGELOG.md) has the full list. Highlights:

**Coordinator record parity with Kafka 4.3.1.** The records the group coordinators write to `__consumer_offsets` match Kafka 4.3.1 field for field, and they are written where Kafka 4.3.1's `GroupMetadataManager` writes them, in the same batches. Replay follows Kafka's `GroupMetadataManager.replay` record by record. The group, share and transaction coordinators load their topics as Kafka does: unknown record types are logged and skipped, and other bad records fail the load.

**Coordinators answer only after commit.** The group, share and transaction coordinators answer a request only once its records are committed, and `WriteTxnMarkers` waits for the high watermark to cover each marker. Before, they answered at the local append, so a failover could lose acknowledged state.

**Replication and log fixes found by Kafka's own system tests**, including:

- A new leader answers a retry of a batch it replicated as a follower as a duplicate. In Kafka's `ReplicationTest`, the consumer read six duplicates before this fix.
- A fetch stopped by `partition_max_bytes` inside a sealed segment no longer skips into the next segment and loses the offsets between.
- KIP-853 dynamic quorums grown from a `--standalone` controller work as Kafka's do, including across hosts.
- Share groups read records that only the remote tier holds, and tiered partitions that stop taking writes move their last records to the remote tier.

**Added:**

- `aspect kafka-system-tests` runs Apache Kafka's ducktape system tests against krabka, with Kafka's clients, tools and checks.
- Kafka's `metadata.log.dir` and the `metadata.log.*` / `metadata.max.*` retention and idle-interval settings.
- KIP-1331 topology descriptions for streams groups, with Kafka's built-in in-memory plugin.
- Streams-group session timeout and heartbeat interval bounds.
- `krabka-format --controller-listener-name`.

**Operational changes:** controller-only nodes open no client listener, the broker raises its open-file soft limit to the hard limit at startup as the JVM does, and the raft node id is the broker id, as Kafka's `node.id` is.

## Crates on crates.io

The krabka crates are published to crates.io under the `krabka-*` names. The old `crabka-*` crates are retired.

| Release | Crates |
|---|---|
| krabka-broker 1.0.0 | `krabka-macros`, `krabka-log`, `krabka-verified` |
| krabka-protocol 0.6.0 | `krabka-units`, `krabka-ids`, `krabka-voters`, `krabka-hlc`, `krabka-trace-context`, `krabka-compression`, `krabka-security`, `krabka-protocol`, `krabka-metadata` |
| krabka-client-rs 0.6.0 | `krabka-client-core`, `krabka-client-admin`, `krabka-client-consumer`, `krabka-client-producer` |
| krabka-schema-registry 0.4.2 | `krabka-schema-serde` |
| krabka-sspi 0.23.0 | `krabka-sspi` |

`krabka-sspi` is a temporary fork of `sspi` with MIT Kerberos fixes. `krabka-security` now takes Kerberos from it in place of a git fork. It will be yanked once upstream `sspi` releases those fixes. The broker itself and its other crates are not published; pin the repository by git revision to use them.

## Benchmarks

OpenMessaging Benchmark results for 1.0.0 against Apache Kafka 4.3.1 and Redpanda 26.2.2, on a Google Cloud runner, have not been measured yet. They will be added here and on the [benchmarks page](/benchmarks) after a complete run.

The local throughput and latency/memory/recovery results on the [benchmarks page](/benchmarks) are from krabka 0.7.0 on the dedicated benchmark host. They have not been re-run for 1.0.0. They measure buffered writes on one shared host with specific resource limits. They do not establish equal crash durability or production readiness, and they apply only to those images, host settings and workloads. The [methodology](https://github.com/krabka-io/krabka-io.github.io/blob/main/benchmarks/README.md) lists the full contract and caveats.

## Links

- [GitHub release v1.0.0](https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.0)
- [CHANGELOG, 1.0.0 section](https://github.com/krabka-io/krabka-broker/blob/v1.0.0/CHANGELOG.md)
- [Persisted formats](https://github.com/krabka-io/krabka-broker/blob/v1.0.0/docs/persisted_formats.md)
- [Broker documentation](/docs/broker)
- [Release status of every krabka repository](/versions)
