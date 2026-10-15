# Raw benchmark results

These are the complete measured results behind the published krabka 1.0.1 and 1.0.5 OpenMessaging benchmarks, kept with Git LFS. The published reports keep summaries and OMB intervals only. Each archive is a gzipped tarball that extracts to a directory named after the run.

| Archive | Suite | Trials | Published summary |
|---|---|---:|---|
| `omb-local-2026-10-11.tar.gz` | OpenMessaging | 78 | [`../openmessaging/omb-local-2026-10-11/`](../openmessaging/omb-local-2026-10-11/summary.md) |
| `omb-local-2026-10-14.tar.gz` | OpenMessaging | 77 | [`../openmessaging/omb-local-2026-10-14/`](../openmessaging/omb-local-2026-10-14/summary.md) |

Both ran on the dedicated benchmark host (AMD EPYC 4344P, 16 logical CPUs) with `apache/kafka:4.3.1` and `redpanda:v26.2.2`. The 1.0.1 run used `krabka-broker:v1.0.1` (`d0a383b1`) on SSDs that were not trimmed. The 1.0.5 run used `krabka-broker:v1.0.5` (`2a81d1ba`), and the runner trimmed the SSDs before every trial. It merges three invocations of the runner. Its 78th trial, Redpanda's replication factor 3 `1-topic-3-partition-100b-3producers`, is omitted: `provenance.json` lists it under `omitted` with the reason. [`../README.md`](../README.md) describes the method.

## What the archive contains

Results only:

- **The run:** `provenance.json` and `summary.md`.
- **Per trial:**
  - OMB's own `result.json`, with every 10-second rate and latency sample and the aggregated quantiles;
  - the runner's `trial.json`;
  - the 250 ms broker CPU and memory samples, in `resources.jsonl` and its metadata;
  - the client's log, in `workload.stdout` and `workload.stderr`.

Left out: the test setup and build. That means the Java sources and compiled classes, the OpenMessaging checkout, the Maven cache and jars, driver and workload YAML, image pull and compile logs, container inspections, broker logs, and the readiness, format and topic-creation probes. `provenance.json` still records the image digests, the OMB commit, the patch hash and every jar hash.

## Interactive viewer

The website's [benchmark data explorer](https://krabka.io/benchmarks/explorer/) combines these archives with the published interval captures of other runs. It opens the newest run. Its release matrix compares throughput, p99 latency and total CPU time with Kafka, Redpanda or the previous krabka release. Missing comparisons remain visible. Cross-release results do not isolate software changes because disk preparation changed.

Select a workload, replication factor and round to compare OMB rate and latency series and backlog. The archived runs also supply broker CPU and memory timelines and client logs. Other published runs retain OMB intervals and resource totals, but do not supply resource timelines or client logs. The viewer disables unavailable controls. It does not reconstruct samples from totals.

Resource views support cluster totals and individual brokers. Zoom the time window, inspect exact samples, export a CSV, and compare aggregate latency quantiles and upstream cumulative distributions. A trial's interval samples are not independent repeated trials.

The trial catalog supports search and sorting. Trial drilldown exposes every original file listed above, including the complete runner trial, resource metadata, client stdout/stderr, run provenance and summary. JSON arrays expand in pages; logs retain their original line numbers. Original files can be downloaded individually. The view's URL preserves workload, broker selection, chart metric, scope, scale and window.

OMB intervals use nominal 10-second interval ends relative to measurement; the source result has no absolute timestamps. Resources use their recorded `elapsed_ms` and the metadata's clock anchor, covering startup, warm-up, probing, measurement and shutdown. The two timelines are intentionally separate. CPU rates use the actual gap between samples, and memory is shown in MiB. Drawing retains extrema; tables, exports and source files keep all original samples.

To develop locally, pull the LFS archive using the command below, then run `npm run start` or `npm run build:site`. Both run `npm run prepare:benchmark-archive`. This step verifies the archive checksums and the interval checksums of the other runs, then stages lossless gzip files and a compact index. Generated files are ignored by Git. The browser fetches selected trials on demand and verifies each file's size and SHA-256 when Web Crypto is available. Run `npm run test:benchmark` for transformation checks. After building, run `npm run check:benchmark-viewer` for browser coverage of all available runs and file views.

## Checksum

```
88d0b8ec39f9be788df20eb0862a7c7d79e038c874b067bbefd35944760a510f  omb-local-2026-10-11.tar.gz
57220747f13ace6b8501cea0b03dfe2e931eb287d1db3482cd2f7e9fec79eb69  omb-local-2026-10-14.tar.gz
```

## Using it

```sh
git lfs pull --include "benchmarks/archive/*.tar.gz"
cd benchmarks/archive && sha256sum -c <<'SUMS'
88d0b8ec39f9be788df20eb0862a7c7d79e038c874b067bbefd35944760a510f  omb-local-2026-10-11.tar.gz
57220747f13ace6b8501cea0b03dfe2e931eb287d1db3482cd2f7e9fec79eb69  omb-local-2026-10-14.tar.gz
SUMS
tar -xzf omb-local-2026-10-11.tar.gz
tar -xzf omb-local-2026-10-14.tar.gz
```

The tarballs are deterministic: entries are sorted, with fixed ownership and modification times.
