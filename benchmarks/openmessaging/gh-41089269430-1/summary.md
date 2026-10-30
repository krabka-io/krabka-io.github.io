# OpenMessaging broker comparison

Run: gh-41089269430-1. Mode: full. Status: complete.

Upstream: https://github.com/openmessaging/benchmark.git at 5b1fa70951a323da26bd587174b58bb2c65b0b5c; Kafka client 3.6.1 for every broker.

Shared host, sequential fresh clusters; 4 logical CPUs and 10 GiB per broker. Buffered acknowledgments; Redpanda uses Raft majorities.
OMB reports latency and rates, not exact delivery/duplicate verification. Resource windows include client startup, warm-up, probing, measurement and shutdown.

- krabka: ghcr.io/krabka-io/krabka-broker@sha256:dd438443a18bfc073590df19cd5a702b35d94d8ddeb6c5193ac6638ddaf2c586
- kafka: apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
- redpanda: docker.redpanda.com/redpandadata/redpanda@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96

| Workload | RF | Broker | Round | Publish msg/s | Consume msg/s | Publish p99 ms | End-to-end p99 ms | CPU seconds | Peak RSS MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| backlog-1-topic-1-partition-1kb | 1 | krabka | 1 | 100012 | 99929 | 2.13 | 1038249.98 | 668.11 | 169.57 |
| backlog-1-topic-1-partition-1kb | 1 | kafka | 1 | 100014 | 99854 | 21.90 | 1038090.24 | 1162.37 | 1184.07 |
| backlog-1-topic-1-partition-1kb | 1 | redpanda | 1 | 100009 | 99956 | 3.09 | 1038454.78 | 2039.92 | 2676.90 |
| simple-workload | 1 | krabka | 1 | 10005 | 10005 | 1.73 | 2.00 | 127.27 | 39.50 |
| simple-workload | 1 | kafka | 1 | 10006 | 10006 | 1.94 | 2.00 | 272.15 | 966.08 |
| simple-workload | 1 | redpanda | 1 | 10009 | 10009 | 1.93 | 2.00 | 402.39 | 4983.27 |
| 1-topic-1-partition-100b | 1 | krabka | 1 | 50011 | 50011 | 1.63 | 2.00 | 305.69 | 48.43 |
| 1-topic-1-partition-100b | 1 | kafka | 1 | 50013 | 50013 | 1.89 | 2.00 | 637.37 | 982.30 |
| 1-topic-1-partition-100b | 1 | redpanda | 1 | 50016 | 50016 | 2.10 | 2.00 | 616.29 | 2616.48 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | krabka | 1 | 418007 | 418000 | 106.33 | 116.00 | 241.62 | 91.57 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | kafka | 1 | 321629 | 321629 | 188.30 | 190.00 | 369.39 | 712.08 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | redpanda | 1 | 293747 | 293747 | 141.68 | 144.00 | 360.22 | 2635.50 |
| 1-topic-3-partition-100b-3producers | 1 | krabka | 1 | 1022742 | 1022734 | 9.25 | 130.00 | 607.30 | 112.10 |
| 1-topic-3-partition-100b-3producers | 1 | kafka | 1 | 606503 | 606504 | 11.15 | 164.00 | 961.88 | 1093.62 |
| 1-topic-3-partition-100b-3producers | 1 | redpanda | 1 | 582212 | 582257 | 6.78 | 182.00 | 1113.19 | 5809.88 |
| max-rate-1-topic-16-partitions-100b | 1 | krabka | 1 | 1087154 | 1087150 | 10.39 | 20.00 | 178.00 | 50.26 |
| max-rate-1-topic-16-partitions-100b | 1 | kafka | 1 | 278388 | 278427 | 4.46 | 72.00 | 181.07 | 973.70 |
| max-rate-1-topic-16-partitions-100b | 1 | redpanda | 1 | 245160 | 245165 | 3.26 | 79.00 | 213.76 | 7330.04 |
| max-rate-1-topic-100-partitions-100b | 1 | krabka | 1 | 995350 | 995348 | 11.20 | 27.00 | 193.19 | 52.54 |
| max-rate-1-topic-100-partitions-100b | 1 | kafka | 1 | 258884 | 258880 | 4.46 | 42.00 | 201.28 | 1002.72 |
| max-rate-1-topic-100-partitions-100b | 1 | redpanda | 1 | 204960 | 204955 | 3.52 | 51.00 | 253.99 | 7318.85 |
| backlog-1-topic-1-partition-1kb | 3 | krabka | 1 | 100014 | 99918 | 3.30 | 1038057.47 | 2960.70 | 377.65 |
| backlog-1-topic-1-partition-1kb | 3 | kafka | 1 | 88502 | 88235 | 1016.95 | 1040633.85 | 2450.86 | 3612.66 |
| backlog-1-topic-1-partition-1kb | 3 | redpanda | 1 | 100009 | 99975 | 20.59 | 1040375.81 | 7318.18 | 8114.64 |
| simple-workload | 3 | krabka | 1 | 10008 | 10008 | 2.90 | 3.00 | 539.54 | 122.07 |
| simple-workload | 3 | kafka | 1 | 10008 | 10008 | 4.31 | 5.00 | 906.98 | 2923.43 |
| simple-workload | 3 | redpanda | 1 | 10010 | 10010 | 2.99 | 3.00 | 782.99 | 14956.45 |
| 1-topic-1-partition-100b | 3 | krabka | 1 | 50010 | 50010 | 2.81 | 3.00 | 1416.75 | 151.02 |
| 1-topic-1-partition-100b | 3 | kafka | 1 | 50010 | 50010 | 3.23 | 3.00 | 2203.33 | 2934.65 |
| 1-topic-1-partition-100b | 3 | redpanda | 1 | 50011 | 50011 | 2.87 | 3.00 | 1562.72 | 7822.86 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | krabka | 1 | 164557 | 164558 | 246.70 | 249.00 | 476.58 | 178.48 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | kafka | 1 | 129830 | 129828 | 510.10 | 512.00 | 629.67 | 2153.95 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | redpanda | 1 | 152862 | 152863 | 270.46 | 273.00 | 738.63 | 7904.50 |
| 1-topic-3-partition-100b-3producers | 3 | krabka | 1 | 974070 | 974054 | 58.21 | 1010.00 | 2671.12 | 316.64 |
| 1-topic-3-partition-100b-3producers | 3 | kafka | 1 | 842918 | 842882 | 506.20 | 1178.01 | 3166.82 | 3692.77 |
| 1-topic-3-partition-100b-3producers | 3 | redpanda | 1 | 511247 | 511257 | 7.48 | 157.00 | 3489.21 | 17345.96 |
| max-rate-1-topic-16-partitions-100b | 3 | krabka | 1 | 935044 | 935084 | 69.09 | 100.00 | 491.09 | 197.84 |
| max-rate-1-topic-16-partitions-100b | 3 | kafka | 1 | 121074 | 120994 | 8.88 | 498.00 | 783.98 | 2937.05 |
| max-rate-1-topic-16-partitions-100b | 3 | redpanda | 1 | 129015 | 129057 | 4.10 | 496.00 | 686.19 | 19842.59 |
| max-rate-1-topic-100-partitions-100b | 3 | krabka | 1 | 865697 | 865696 | 49.50 | 82.00 | 553.14 | 178.77 |
| max-rate-1-topic-100-partitions-100b | 3 | kafka | 1 | 132634 | 132664 | 20.73 | 487.00 | 795.02 | 3001.85 |
| max-rate-1-topic-100-partitions-100b | 3 | redpanda | 1 | 131303 | 131250 | 4.05 | 486.00 | 756.95 | 19414.63 |
| backlog-1-topic-16-partitions-1kb | 1 | krabka | 1 | 100017 | 99838 | 2.70 | 1039200.26 | 668.20 | 460.91 |
| backlog-1-topic-16-partitions-1kb | 1 | kafka | 1 | 100016 | 99881 | 16.20 | 1039892.48 | 1152.37 | 1131.55 |
| backlog-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 100015 | 99883 | 3.99 | 1039851.52 | 1518.31 | 7392.15 |
| 1-topic-1-partition-1kb | 1 | krabka | 1 | 50010 | 50010 | 2.06 | 2.00 | 418.98 | 59.95 |
| 1-topic-1-partition-1kb | 1 | kafka | 1 | 50017 | 50017 | 2.79 | 4.00 | 717.59 | 1018.80 |
| 1-topic-1-partition-1kb | 1 | redpanda | 1 | 50012 | 50012 | 2.50 | 3.00 | 796.61 | 2632.20 |
| 1-topic-16-partitions-1kb | 1 | krabka | 1 | 50012 | 50012 | 1.94 | 2.00 | 415.53 | 63.61 |
| 1-topic-16-partitions-1kb | 1 | kafka | 1 | 50015 | 50015 | 2.85 | 5.00 | 829.49 | 1021.88 |
| 1-topic-16-partitions-1kb | 1 | redpanda | 1 | 50014 | 50014 | 2.46 | 3.00 | 1383.98 | 7345.27 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | krabka | 1 | 985491 | 985488 | 24.76 | 28.00 | 163.64 | 68.79 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | kafka | 1 | 941168 | 941173 | 136.19 | 139.00 | 261.09 | 1209.54 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | redpanda | 1 | 999579 | 999536 | 90.87 | 93.00 | 269.38 | 2660.26 |
| max-rate-1-topic-16-partitions-1kb | 1 | krabka | 1 | 344041 | 344043 | 195.14 | 202.00 | 274.09 | 82.95 |
| max-rate-1-topic-16-partitions-1kb | 1 | kafka | 1 | 277397 | 277393 | 231.87 | 239.00 | 435.27 | 1219.21 |
| max-rate-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 276492 | 276488 | 244.85 | 252.00 | 499.78 | 7435.18 |
| max-rate-1-topic-100-partitions-1kb | 1 | krabka | 1 | 352368 | 352368 | 164.08 | 181.00 | 276.97 | 96.44 |
| max-rate-1-topic-100-partitions-1kb | 1 | kafka | 1 | 251185 | 251181 | 254.45 | 264.00 | 481.66 | 1215.90 |
| max-rate-1-topic-100-partitions-1kb | 1 | redpanda | 1 | 236710 | 236716 | 282.11 | 292.00 | 550.08 | 7422.47 |
| backlog-1-topic-16-partitions-1kb | 3 | krabka | 1 | 100013 | 99801 | 3.65 | 1038168.06 | 2561.64 | 1251.69 |
| backlog-1-topic-16-partitions-1kb | 3 | kafka | 1 | 100023 | 99863 | 142.61 | 1037750.27 | 2929.43 | 3660.59 |
| backlog-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 100022 | 99837 | 6.34 | 1038356.48 | 3519.90 | 22487.09 |
| 1-topic-1-partition-1kb | 3 | krabka | 1 | 50013 | 50013 | 2.93 | 3.00 | 1480.13 | 155.30 |
| 1-topic-1-partition-1kb | 3 | kafka | 1 | 50017 | 50017 | 144.75 | 147.00 | 2203.93 | 3260.00 |
| 1-topic-1-partition-1kb | 3 | redpanda | 1 | 50012 | 50012 | 3.07 | 3.00 | 2040.96 | 7893.99 |
| 1-topic-16-partitions-1kb | 3 | krabka | 1 | 50012 | 50012 | 2.98 | 3.00 | 1578.31 | 180.98 |
| 1-topic-16-partitions-1kb | 3 | kafka | 1 | 50013 | 50013 | 38.90 | 68.00 | 2158.85 | 3290.98 |
| 1-topic-16-partitions-1kb | 3 | redpanda | 1 | 50012 | 50013 | 3.45 | 5.00 | 2383.10 | 22044.40 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | krabka | 1 | 1016049 | 1016053 | 306.18 | 309.00 | 290.21 | 188.58 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | kafka | 1 | 765837 | 765829 | 349.57 | 353.00 | 436.28 | 2300.38 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | redpanda | 1 | 922556 | 922545 | 59.46 | 61.00 | 463.34 | 7903.22 |
| max-rate-1-topic-16-partitions-1kb | 3 | krabka | 1 | 178536 | 178541 | 412.79 | 417.00 | 521.20 | 176.45 |
| max-rate-1-topic-16-partitions-1kb | 3 | kafka | 1 | 170667 | 170663 | 146.41 | 214.00 | 906.62 | 3637.91 |
| max-rate-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 167807 | 167812 | 95.07 | 221.00 | 906.92 | 22156.96 |
| max-rate-1-topic-100-partitions-1kb | 3 | krabka | 1 | 177158 | 177167 | 712.79 | 718.00 | 624.97 | 198.73 |
| max-rate-1-topic-100-partitions-1kb | 3 | kafka | 1 | 176491 | 176489 | 183.20 | 201.00 | 958.41 | 3323.59 |
| max-rate-1-topic-100-partitions-1kb | 3 | redpanda | 1 | 167142 | 167145 | 413.55 | 441.00 | 972.37 | 22278.51 |
