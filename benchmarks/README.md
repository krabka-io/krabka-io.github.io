# OpenMessaging broker benchmarks

The website uses only published full runs of the upstream OpenMessaging Benchmark (OMB). `npm run benchmark` runs OMB by default against Krabka, Apache Kafka, and Redpanda. These are project-run measurements using OMB; they are not results certified by the OpenMessaging project.

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
A retriable commit failure, such as one caused by a leader or coordinator move, is
counted in the trial's `retriable_commit_failures` and does not fail it, because the
consumer puts the offsets back and the next commit carries them; Redpanda's fork logs
these as warnings. A non-retriable commit failure, like every other client error,
still fails the trial. The patch's
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

"Same OMB build" means the same OMB commit, patch hashes, build image and
dependency jars under `/m2`. Each runner builds OMB itself, and Maven stamps the
jars it builds from `/src` with the build time, so those hashes differ between
builds of the same source and are recorded but not compared.

`split --rf N [--workloads A,B] --reason TEXT --out DIR RUN_DIR` turns one
replication factor of a run that did not finish into a complete shard. Every
vendor, round and selected workload at that RF must have a result and no
failure; `--workloads` leaves out a workload with a failed trial so it can be
rerun, for all three brokers, as its own shard. The shard's provenance, and the
merged run's `shards` entry for it, record the source run, its status and the
reason under `derived_from`.

`--omit VENDOR:WORKLOAD,... --omit-reason TEXT` leaves out a trial that did not
complete and cannot be run on this host, for example because the broker fills
the disk before the trial ends. A trial with a result cannot be omitted, and at
least two brokers stay in every comparison. The shard and the merged run list
each omitted trial and its reason under `omitted`. The summary, the charts and
the tables show it as not measured.

Actions retains `provenance.json`, effective/upstream YAML, immutable image and
source references, runtime jar hashes, raw OMB JSON, broker inspections/logs,
250 ms CPU/memory time series, and failure diagnostics for 30 days. A complete
matrix also produces `summary.md` and a job summary with publish/consume rates,
publish/end-to-end p99 latency, CPU seconds and peak RSS. Resource windows include
startup of the benchmark client, warm-up, probing, measurement and shutdown.

Each trial also samples `/proc/diskstats` with the 250 ms resource samples, in
`devices.jsonl`, for the block devices under the run directory and Docker
storage. `provenance.host.storage` records those mounts, their filesystem and
mount options, the RAID level and each member device's model and firmware.
`trial.json` has `metrics.measurement_window`, which covers only the OMB
measurement: its start and end from the client log, broker CPU seconds,
published messages (the mean publish rate times the window), messages per CPU
second, and each device's utilization and bytes read and written. The totals
in `metrics` still cover the full capture.
OMB does **not** check exact delivery counts or duplicate sequences. Missing or
truncated result series, zero publish/consume traffic, publish errors, OOMs,
logged client/consumer errors and missing resource counters fail the run,
including when upstream exits zero after
catching a workload exception. Failed trials are recorded and the remaining matrix
is attempted; failed matrices do not produce a successful summary. Artifacts stay
under `.benchmarks/<run-id>/`; the runner never publishes, commits or pushes.

To publish a complete full-mode run on the website, download its `openmessaging-<run>-<attempt>`
artifact and run `node scripts/publish-openmessaging.mjs <artifact>/<run-id>`. The script
rejects smoke, failed and incomplete matrices, re-validates every trial, and writes
`benchmarks/openmessaging/<run-id>/` (provenance, summary, compact `trials.json`,
`intervals.json`, and `omb-results/*.json.gz`) and `latest-openmessaging.md`, which the `/benchmarks` page follows. Review
and commit those files; the full diagnostic artifact stays in Actions for 30 days.

New publications retain the complete upstream OMB result object for every trial as
`omb-results/rf<RF>-<vendor>-<round>-<workload>.json.gz`. `intervals.json` indexes
these files by workload, RF, vendor and repetition, with sample counts and SHA-256
checksums of the compressed files. Decompress with `gzip -dc FILE.json.gz`.
Publish/consume/error rates, backlog, latency interval arrays, histograms and all
other upstream fields keep their original values, sample order and units. No
samples are averaged, resampled or assigned invented wall-clock timestamps.
The compact figures still come from the same validated capture. Broker resource
telemetry and logs remain in the diagnostic artifact. Older published runs without
an interval index continue to supply aggregate charts. The original raw results
for `omb-local-2026-10-11` are preserved in the [Git LFS archive](archive/README.md),
including OMB’s 10-second intervals. Other timelines require the original captures
and cannot be reconstructed from averages.

## Write path and latency

The brokers do not write to disk the same way, and on the benchmark host that
difference sets most of the latency gap. Redpanda opens its log segments with
`O_DIRECT` and keeps its own cache, so its writes bypass the Linux page cache.
Krabka and Kafka write segments through the page cache. On 2026-10-10 this was
checked on the host itself: Redpanda's open segment descriptors carry `O_DIRECT`,
and krabka's writer threads were sampled blocked in `ext4_buffered_write_iter`
and the ext4 journal.

The host's data volume is ext4 on an md RAID1 of two NVMe drives. A buffered
write that updates a file's timestamps waits for the running ext4 journal
commit, and commits wait for the md write-intent bitmap. Under sustained load,
krabka's writers therefore stall for hundreds of milliseconds every few seconds.
Those stalls set most of krabka's p99 and p99.9 latency in these results, and
Kafka's latency tails match krabka's in the same cases. Redpanda's direct writes
avoid them. Throughput and median latency are much less affected. On a different
filesystem or storage layout the gap could be smaller or larger.

Since 1.0.2, krabka can write active segments with `O_DIRECT` when a topic sets
`preallocate=true`, on Linux 6.1 or later where the filesystem reports direct-I/O
alignment. It is off by default and the benchmark does not set `preallocate`, so krabka
writes through the page cache here. Making direct I/O the default is an
open question ([krabka-broker#1333](https://github.com/krabka-io/krabka-broker/issues/1333)).
These results compare the brokers as they ship.


## SSD state

An SSD that is not told which blocks are free slows down as deleted data
fills it. The benchmark host mounts its data volume without `discard` and
trims it once a week, and each trial writes and deletes tens to hundreds of
gigabytes. On 2026-10-13, after two days of trials, direct writes on the host
were several times slower than after a manual `fstrim`
([krabka-broker#1370](https://github.com/krabka-io/krabka-broker/issues/1370)
has the measurements). Trials that ran hours apart therefore did not start
from the same disk, and the absolute numbers of runs before that date depend
on how long the drives had gone untrimmed. The runner runs every broker of a workload back to back,
which keeps that drift small within a workload but does not remove it.

From 2026-10-14 the runner trims the filesystems that hold the checkout and
Docker's data before every trial, with `sudo -n fstrim`. The contract's
`disk_trim_before_trial` is true only when every mount was trimmed before
every trial, so the shard merge does not combine trimmed and untrimmed runs.
`disk_trim` in the provenance records each mount, the trials trimmed and the
bytes released, and the merged provenance keeps it for every shard. A mount
that refuses the trim, for example on a host without passwordless sudo for
`fstrim`, is recorded and not asked again, and the other mounts are still
trimmed. `--no-fstrim` turns the trim off and needs no trim tools.

## Local runner prerequisites

- Node >=22.12 and JDK >=17 (`java` and `javac`). No Bazel build, sibling checkout, npm dependencies, or Docker Compose is needed.
- Native Linux/amd64 with a local Docker daemon, cgroup v2, readable `/proc` and cgroup resource counters, CPU affinity/quota and memory/swap limits enabled. Remote Docker, Docker Desktop, and emulated images are rejected.
- At least **14 available logical CPUs** and **34 GiB available RAM** (three 10 GiB brokers plus a 4 GiB client). Each broker receives 4 logical CPUs, a 4 CPU quota, 10 GiB RAM, zero swap, and a 131,072 open-file limit. RF3 runs three brokers; the client receives the remaining CPUs and a separate 4 GiB memory limit. SMT siblings are grouped where possible and the topology is recorded.
- At least **150 GiB free disk** for a full run, or **4 GiB** for smoke, both at the checkout and Docker storage. Data uses Docker volumes and is deleted after each cluster repetition; measurements and logs remain.
- At least **24,728 available Linux AIO slots** (`fs.aio-max-nr - fs.aio-nr`). Redpanda networking AIO is explicitly limited to 1,024 control blocks per shard so three nodes fit the usual 65,536-slot host limit consistently. This setting and the host limit are recorded; no sysctl is changed.
- Passwordless `sudo` for `fstrim` (see [SSD state](#ssd-state)); without it, trials run untrimmed.
- Registry access to `ghcr.io`, Docker Hub, and `docker.redpanda.com`; access to GitHub for the pinned OMB source and Maven repositories for dependencies. Tags are pulled and resolved to registry digests once before running. The defaults are Krabka v1.0.5, Kafka 4.3.1, and Redpanda v26.2.2; versions never silently advance.

The runner owns only its uniquely named and labeled containers, volumes, and networks. It does not change host settings, stop other containers, commit, push, or deploy. Avoid other heavy work while measuring. Concurrent invocations in the same checkout fail; a hard-killed process can leave `.benchmarks/runner.lock`, which contains its PID and run ID. Inspect that process and its labeled Docker resources before manually removing a stale lock.

## Direct I/O release qualification plan

Status: proposed release gate. The current release pipeline does not run this
qualification. The two rounds reported in [the direct I/O investigation](https://github.com/krabka-io/krabka-broker/issues/1370#issuecomment-6157203260)
are evidence for one workload, not release qualification. Redpanda had lower
end-to-end p99 and disk utilization in those rounds. They do not establish a
CPU efficiency advantage for krabka.

### Test matrix and controls

For each release candidate, pin immutable images for the candidate, the previous
release, Kafka and Redpanda. Pin the OMB commit, patches and client jars. Run the
full upstream workload catalog at RF1 and RF3 in both krabka modes: default
page-cache writes and `preallocate=true`. Compare each mode with the same mode
of the previous release. Keep ext4 and XFS results separate. Record the kernel,
mount options, device layout, firmware, CPU limits and memory limits.

Run at least five measured rounds per configuration. Rotate broker order across
rounds; keep paired comparisons close in time. Trim the SSD before every trial,
verify the trim succeeded, and record free space. Start a fresh cluster with
fresh data for each trial. Keep all failed and incomplete attempts in the
artifact. Do not silently replace them with successful retries.

Verify `O_DIRECT` on each active segment descriptor. Confirm the topic setting,
filesystem alignment and supported kernel. A fallback to buffered writes fails
direct-mode qualification. A successful command alone does not prove the mode.

With `--krabka-preallocate`, the runner does this check 30 seconds into each
krabka trial's OMB measurement. A sidecar container shares each broker's PID
namespace and user, and lists its open segment files with their `fdinfo`
flags. Every benchmark partition replica must hold its newest segment open
with `O_DIRECT`, and the replicas found must equal the topic's partitions
times the replication factor. Otherwise the trial fails. The listings are in
`direct-io-broker-N.txt` and the result is in `direct-io.json` and in
`trial.json` under `direct_io`.

### Measurements and acceptance

Retain raw OMB results, exact measurement timestamps, broker resource samples,
device telemetry, client logs and provenance for every round. Show each round,
median and range for publish and consume rates, publish p99 and p99.9, and
end-to-end p99 and p99.9. Show backlog separately. Do not average interval
percentiles or count intervals as independent trials.

Measure broker CPU and device utilization over the OMB measurement window.
Report full-capture totals separately. Calculate CPU cost from the measured CPU
seconds and the measured message count in the same window. Do not compare JVM
heap or Redpanda memory reservations with krabka RSS as an efficiency claim.

Set workload budgets before the run. Proposed review triggers, relative to the
previous release in the same mode and on the same storage stack, are:

- a median publish-rate drop greater than 5%;
- a median end-to-end p99 increase greater than 10%;
- a median measurement-window CPU cost increase greater than 10%.

These thresholds trigger investigation; they are not confidence intervals or
proof of statistical significance. Repeat flagged pairs and review every round
and interval. Review higher percentiles, backlog and device utilization even
when no threshold triggers. Block qualification for an unresolved regression,
a missing required trial, a mode-verification failure or a correctness failure.
Record the decision and its evidence in the release artifact.

### Correctness and recovery

Check acknowledged record IDs and duplicates across process termination,
leader failover, restart and disk-full errors. Include segment roll, retention,
compaction and catch-up reads. Check write-error propagation and bounded queue
behavior. Test host power loss separately before a power-loss durability claim.
OMB rates alone cannot establish delivery correctness or crash durability.

### Publication work before adoption

The publisher currently refuses `krabka_preallocate` overrides in the default
suite. Add a separate direct-mode publication track before adopting this gate;
do not relax the default-mode guard. Preserve the mode, image digests, disk
state, repetition ID, failed attempts and telemetry coverage in its schema.
Teach the viewer to group by mode and storage stack before it compares releases.

Attach the qualification matrix and decision to each release. Publish all raw
artifacts, including failures, and state any qualification gap in release notes.
A release with incomplete qualification must not claim direct-I/O parity across
the full suite. The maintainers must approve the budgets and implement this gate
before the site describes direct I/O as release-qualified.
