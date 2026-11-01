# OpenMessaging broker comparison

Run: gh-41260722312-1. Mode: full. Status: complete.

Upstream: https://github.com/openmessaging/benchmark.git at 5b1fa70951a323da26bd587174b58bb2c65b0b5c; Kafka client 3.6.1 for every broker.

Shared host, sequential fresh clusters; 4 logical CPUs and 10 GiB per broker. Buffered acknowledgments; Redpanda uses Raft majorities.
OMB reports latency and rates, not exact delivery/duplicate verification. Resource windows include client startup, warm-up, probing, measurement and shutdown.

- krabka: ghcr.io/krabka-io/krabka-broker@sha256:577a686c1b24a5a655ec8a2a82c877e119e3dd3f07b5fa617e91d05851714e1e
- kafka: apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
- redpanda: docker.redpanda.com/redpandadata/redpanda@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96

| Workload | RF | Broker | Round | Publish msg/s | Consume msg/s | Publish p99 ms | End-to-end p99 ms | CPU seconds | Peak RSS MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| backlog-1-topic-1-partition-1kb | 1 | krabka | 1 | 100017 | 99887 | 2.16 | 1038249.98 | 696.27 | 178.43 |
| backlog-1-topic-1-partition-1kb | 1 | kafka | 1 | 100013 | 99918 | 19.50 | 1038692.35 | 1188.34 | 1145.17 |
| backlog-1-topic-1-partition-1kb | 1 | redpanda | 1 | 100011 | 99984 | 2.69 | 1039249.41 | 1931.77 | 2669.71 |
| simple-workload | 1 | krabka | 1 | 10006 | 10006 | 1.74 | 2.00 | 134.23 | 39.50 |
| simple-workload | 1 | kafka | 1 | 10009 | 10009 | 2.04 | 2.00 | 289.98 | 967.92 |
| simple-workload | 1 | redpanda | 1 | 10008 | 10008 | 2.06 | 2.00 | 432.36 | 4992.80 |
| 1-topic-1-partition-100b | 1 | krabka | 1 | 50010 | 50011 | 1.77 | 2.00 | 325.03 | 47.47 |
| 1-topic-1-partition-100b | 1 | kafka | 1 | 50015 | 50015 | 2.06 | 2.00 | 745.79 | 981.40 |
| 1-topic-1-partition-100b | 1 | redpanda | 1 | 50014 | 50014 | 2.00 | 2.00 | 840.05 | 2618.11 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | krabka | 1 | 378130 | 378134 | 116.18 | 123.00 | 248.86 | 89.18 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | kafka | 1 | 292190 | 292187 | 196.38 | 199.00 | 387.47 | 727.42 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | redpanda | 1 | 275434 | 275436 | 151.74 | 155.00 | 436.97 | 2642.73 |
| 1-topic-3-partition-100b-3producers | 1 | krabka | 1 | 974338 | 974335 | 8.54 | 96.00 | 640.18 | 103.15 |
| 1-topic-3-partition-100b-3producers | 1 | kafka | 1 | 605899 | 605881 | 25.78 | 203.00 | 1024.80 | 1150.15 |
| 1-topic-3-partition-100b-3producers | 1 | redpanda | 1 | 579686 | 579650 | 6.02 | 165.00 | 1105.91 | 5796.17 |
| max-rate-1-topic-16-partitions-100b | 1 | krabka | 1 | 1049692 | 1049693 | 17.95 | 27.00 | 185.93 | 47.35 |
| max-rate-1-topic-16-partitions-100b | 1 | kafka | 1 | 470239 | 470253 | 17.45 | 54.00 | 223.28 | 1045.09 |
| max-rate-1-topic-16-partitions-100b | 1 | redpanda | 1 | 345424 | 345444 | 4.27 | 60.00 | 225.12 | 7356.73 |
| max-rate-1-topic-100-partitions-100b | 1 | krabka | 1 | 861953 | 861954 | 12.50 | 34.00 | 209.75 | 51.32 |
| max-rate-1-topic-100-partitions-100b | 1 | kafka | 1 | 211987 | 211992 | 5.68 | 50.00 | 225.12 | 999.53 |
| max-rate-1-topic-100-partitions-100b | 1 | redpanda | 1 | 220456 | 220456 | 3.54 | 51.00 | 245.27 | 7334.43 |
| backlog-1-topic-1-partition-1kb | 3 | krabka | 1 | 100012 | 99894 | 3.08 | 1037881.34 | 2827.26 | 382.92 |
| backlog-1-topic-1-partition-1kb | 3 | kafka | 1 | 95059 | 94771 | 788.05 | 1039228.93 | 2485.31 | 3633.01 |
| backlog-1-topic-1-partition-1kb | 3 | redpanda | 1 | 100009 | 99960 | 5.34 | 1039273.98 | 6506.36 | 8105.09 |
| simple-workload | 3 | krabka | 1 | 10006 | 10006 | 2.83 | 3.00 | 538.94 | 123.07 |
| simple-workload | 3 | kafka | 1 | 10009 | 10009 | 5.27 | 6.00 | 872.63 | 2937.19 |
| simple-workload | 3 | redpanda | 1 | 10006 | 10006 | 2.86 | 3.00 | 761.22 | 14923.25 |
| 1-topic-1-partition-100b | 3 | krabka | 1 | 50010 | 50010 | 2.75 | 3.00 | 1434.42 | 155.30 |
| 1-topic-1-partition-100b | 3 | kafka | 1 | 50011 | 50011 | 3.06 | 3.00 | 1867.16 | 2921.22 |
| 1-topic-1-partition-100b | 3 | redpanda | 1 | 50019 | 50019 | 2.59 | 3.00 | 1721.40 | 7847.47 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | krabka | 1 | 172798 | 172795 | 229.15 | 231.00 | 466.25 | 172.21 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | kafka | 1 | 138589 | 138589 | 498.49 | 500.00 | 640.29 | 2144.15 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | redpanda | 1 | 150438 | 150438 | 260.26 | 261.00 | 675.73 | 7912.91 |
| 1-topic-3-partition-100b-3producers | 3 | krabka | 1 | 1013074 | 1013045 | 63.94 | 1202.01 | 2563.53 | 323.36 |
| 1-topic-3-partition-100b-3producers | 3 | kafka | 1 | 810623 | 810591 | 832.34 | 1674.01 | 3097.97 | 3665.21 |
| 1-topic-3-partition-100b-3producers | 3 | redpanda | 1 | 505846 | 505853 | 38.26 | 503.00 | 2968.24 | 17425.52 |
| max-rate-1-topic-16-partitions-100b | 3 | krabka | 1 | 1016357 | 1016351 | 71.08 | 96.00 | 481.02 | 174.12 |
| max-rate-1-topic-16-partitions-100b | 3 | kafka | 1 | 134570 | 134597 | 12.55 | 497.00 | 745.51 | 2964.63 |
| max-rate-1-topic-16-partitions-100b | 3 | redpanda | 1 | 134058 | 134068 | 4.21 | 498.00 | 641.22 | 20275.97 |
| max-rate-1-topic-100-partitions-100b | 3 | krabka | 1 | 869846 | 869839 | 51.51 | 95.00 | 565.98 | 171.00 |
| max-rate-1-topic-100-partitions-100b | 3 | kafka | 1 | 131669 | 131669 | 16.91 | 482.00 | 807.06 | 3012.12 |
| max-rate-1-topic-100-partitions-100b | 3 | redpanda | 1 | 147860 | 147934 | 4.48 | 484.00 | 691.57 | 21115.30 |
| backlog-1-topic-16-partitions-1kb | 1 | krabka | 1 | 100010 | 99934 | 2.91 | 1038553.09 | 832.66 | 542.29 |
| backlog-1-topic-16-partitions-1kb | 1 | kafka | 1 | 100010 | 99968 | 17.31 | 1039572.99 | 1587.12 | 1120.78 |
| backlog-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 100009 | 99968 | 6.97 | 1040556.03 | 1962.40 | 7426.35 |
| 1-topic-1-partition-1kb | 1 | krabka | 1 | 50009 | 50009 | 2.54 | 3.00 | 520.51 | 60.89 |
| 1-topic-1-partition-1kb | 1 | kafka | 1 | 50007 | 50008 | 4.58 | 6.00 | 953.82 | 1006.62 |
| 1-topic-1-partition-1kb | 1 | redpanda | 1 | 50011 | 50011 | 3.58 | 4.00 | 867.14 | 2628.50 |
| 1-topic-16-partitions-1kb | 1 | krabka | 1 | 50008 | 50008 | 2.57 | 3.00 | 566.30 | 60.57 |
| 1-topic-16-partitions-1kb | 1 | kafka | 1 | 50009 | 50009 | 3.88 | 6.00 | 1032.19 | 1008.42 |
| 1-topic-16-partitions-1kb | 1 | redpanda | 1 | 50011 | 50012 | 3.71 | 6.00 | 1392.28 | 7347.06 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | krabka | 1 | 1787607 | 1787606 | 130.71 | 135.00 | 161.70 | 75.00 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | kafka | 1 | 1457213 | 1457195 | 143.29 | 149.00 | 285.91 | 912.58 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | redpanda | 1 | 1495069 | 1495079 | 196.46 | 204.00 | 306.80 | 2637.74 |
| max-rate-1-topic-16-partitions-1kb | 1 | krabka | 1 | 374726 | 374724 | 167.60 | 172.00 | 312.91 | 81.79 |
| max-rate-1-topic-16-partitions-1kb | 1 | kafka | 1 | 273323 | 273324 | 238.15 | 245.00 | 458.38 | 1218.25 |
| max-rate-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 241297 | 241297 | 232.48 | 240.00 | 500.99 | 7383.25 |
| max-rate-1-topic-100-partitions-1kb | 1 | krabka | 1 | 379419 | 379421 | 147.91 | 153.00 | 337.40 | 90.45 |
| max-rate-1-topic-100-partitions-1kb | 1 | kafka | 1 | 290795 | 290795 | 194.04 | 202.00 | 474.09 | 1210.12 |
| max-rate-1-topic-100-partitions-1kb | 1 | redpanda | 1 | 234591 | 234588 | 228.59 | 238.00 | 568.14 | 7390.20 |
| backlog-1-topic-16-partitions-1kb | 3 | krabka | 1 | 100014 | 99797 | 3.33 | 1037959.17 | 2665.75 | 1250.68 |
| backlog-1-topic-16-partitions-1kb | 3 | kafka | 1 | 100017 | 99823 | 102.18 | 1037635.58 | 2919.48 | 3656.93 |
| backlog-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 100024 | 99794 | 6.88 | 1038172.16 | 3572.09 | 22647.12 |
| 1-topic-1-partition-1kb | 3 | krabka | 1 | 50012 | 50012 | 2.96 | 3.00 | 1725.89 | 183.91 |
| 1-topic-1-partition-1kb | 3 | kafka | 1 | 50021 | 50021 | 199.40 | 201.00 | 2142.53 | 3530.84 |
| 1-topic-1-partition-1kb | 3 | redpanda | 1 | 50015 | 50015 | 3.20 | 3.00 | 1755.32 | 7897.82 |
| 1-topic-16-partitions-1kb | 3 | krabka | 1 | 50012 | 50012 | 2.95 | 3.00 | 1745.79 | 201.65 |
| 1-topic-16-partitions-1kb | 3 | kafka | 1 | 50013 | 50013 | 36.50 | 68.00 | 2100.36 | 3252.08 |
| 1-topic-16-partitions-1kb | 3 | redpanda | 1 | 50012 | 50012 | 3.83 | 5.00 | 2555.74 | 22045.29 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | krabka | 1 | 937624 | 937626 | 355.58 | 358.00 | 300.39 | 175.98 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | kafka | 1 | 713340 | 713340 | 384.81 | 388.00 | 425.30 | 2298.62 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | redpanda | 1 | 876187 | 876164 | 37.17 | 40.00 | 464.97 | 7909.30 |
| max-rate-1-topic-16-partitions-1kb | 3 | krabka | 1 | 178195 | 178197 | 397.69 | 402.00 | 515.50 | 198.95 |
| max-rate-1-topic-16-partitions-1kb | 3 | kafka | 1 | 172145 | 172151 | 147.05 | 234.00 | 887.84 | 3349.16 |
| max-rate-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 162278 | 162263 | 151.80 | 265.00 | 932.26 | 22279.57 |
| max-rate-1-topic-100-partitions-1kb | 3 | krabka | 1 | 176346 | 176343 | 708.60 | 716.00 | 630.22 | 213.18 |
| max-rate-1-topic-100-partitions-1kb | 3 | kafka | 1 | 177590 | 177586 | 418.72 | 434.00 | 967.37 | 3644.10 |
| max-rate-1-topic-100-partitions-1kb | 3 | redpanda | 1 | 168555 | 168559 | 401.18 | 422.00 | 909.35 | 22371.74 |
