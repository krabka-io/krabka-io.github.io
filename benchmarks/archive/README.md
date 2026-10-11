# Raw benchmark results

These are the complete measured results behind the published krabka 1.0.1 OpenMessaging benchmark, kept with Git LFS. The published report keeps summaries only. The archive is a gzipped tarball that extracts to a directory named after the run.

| Archive | Suite | Trials | Published summary |
|---|---|---:|---|
| `omb-local-2026-10-11.tar.gz` | OpenMessaging | 78 | [`../openmessaging/omb-local-2026-10-11/`](../openmessaging/omb-local-2026-10-11/summary.md) |

It ran on the dedicated benchmark host (AMD EPYC 4344P, 16 logical CPUs) with `krabka-broker:v1.0.1` (`d0a383b1`), `apache/kafka:4.3.1` and `redpanda:v26.2.2`. [`../README.md`](../README.md) describes the method.

## What the archive contains

Results only:

- **The run:** `provenance.json` and `summary.md`.
- **Per trial:**
  - OMB's own `result.json`, with every 10-second rate and latency sample and the aggregated quantiles;
  - the runner's `trial.json`;
  - the 250 ms broker CPU and memory samples, in `resources.jsonl` and its metadata;
  - the client's log, in `workload.stdout` and `workload.stderr`.

Left out: the test setup and build. That means the Java sources and compiled classes, the OpenMessaging checkout, the Maven cache and jars, driver and workload YAML, image pull and compile logs, container inspections, broker logs, and the readiness, format and topic-creation probes. `provenance.json` still records the image digests, the OMB commit, the patch hash and every jar hash.

## Checksum

```
88d0b8ec39f9be788df20eb0862a7c7d79e038c874b067bbefd35944760a510f  omb-local-2026-10-11.tar.gz
```

## Using it

```sh
git lfs pull --include "benchmarks/archive/*.tar.gz"
cd benchmarks/archive && sha256sum -c <<'SUMS'
88d0b8ec39f9be788df20eb0862a7c7d79e038c874b067bbefd35944760a510f  omb-local-2026-10-11.tar.gz
SUMS
tar -xzf omb-local-2026-10-11.tar.gz
```

The tarball is deterministic: entries are sorted, with fixed ownership and modification times.
