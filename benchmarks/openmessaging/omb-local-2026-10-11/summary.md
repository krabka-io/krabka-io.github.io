# OpenMessaging broker comparison

Run: omb-local-2026-10-11. Mode: full. Status: complete.

Upstream: https://github.com/openmessaging/benchmark.git at 5b1fa70951a323da26bd587174b58bb2c65b0b5c; Kafka client 3.6.1 for every broker.

Shared host, sequential fresh clusters; 4 logical CPUs and 10 GiB per broker. Buffered acknowledgments; Redpanda uses Raft majorities.
OMB reports latency and rates, not exact delivery/duplicate verification. Resource windows include client startup, warm-up, probing, measurement and shutdown.

- krabka: ghcr.io/krabka-io/krabka-broker@sha256:d0a383b12176a55eb771c870d819ccb4f60ce0bb6b5356b5ec44285c01d7fd93
- kafka: apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
- redpanda: docker.redpanda.com/redpandadata/redpanda@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96

| Workload | RF | Broker | Round | Publish msg/s | Consume msg/s | Publish p99 ms | End-to-end p99 ms | CPU seconds | Peak RSS MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| simple-workload | 1 | krabka | 1 | 10002 | 10002 | 1.26 | 2.00 | 82.22 | 49.73 |
| 1-topic-1-partition-1kb | 1 | krabka | 1 | 50004 | 50004 | 292.79 | 344.00 | 231.64 | 288.82 |
| 1-topic-1-partition-100b | 1 | krabka | 1 | 50004 | 50004 | 1.25 | 2.00 | 203.44 | 329.04 |
| 1-topic-16-partitions-1kb | 1 | krabka | 1 | 50005 | 50005 | 165.50 | 215.00 | 258.01 | 297.96 |
| backlog-1-topic-1-partition-1kb | 1 | krabka | 1 | 100009 | 100013 | 238.41 | 1038417.92 | 314.52 | 90.87 |
| backlog-1-topic-16-partitions-1kb | 1 | krabka | 1 | 99996 | 99987 | 286.25 | 1150394.37 | 316.36 | 97.84 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | krabka | 1 | 552171 | 552181 | 677.06 | 688.00 | 156.53 | 60.71 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | krabka | 1 | 3053626 | 3053626 | 345.84 | 349.00 | 132.55 | 63.01 |
| 1-topic-3-partition-100b-3producers | 1 | krabka | 1 | 1178843 | 1179718 | 746.36 | 1241.01 | 492.86 | 453.26 |
| max-rate-1-topic-16-partitions-1kb | 1 | krabka | 1 | 352108 | 352109 | 586.94 | 597.00 | 137.51 | 65.80 |
| max-rate-1-topic-16-partitions-100b | 1 | krabka | 1 | 555894 | 555910 | 286.11 | 305.00 | 56.26 | 51.12 |
| max-rate-1-topic-100-partitions-1kb | 1 | krabka | 1 | 370716 | 370716 | 360.64 | 364.00 | 164.92 | 66.97 |
| max-rate-1-topic-100-partitions-100b | 1 | krabka | 1 | 649998 | 649988 | 351.40 | 365.00 | 71.97 | 57.44 |
| simple-workload | 1 | kafka | 1 | 10005 | 10005 | 1.23 | 2.00 | 91.29 | 962.24 |
| 1-topic-1-partition-1kb | 1 | kafka | 1 | 50004 | 50004 | 294.65 | 343.00 | 254.03 | 1100.84 |
| 1-topic-1-partition-100b | 1 | kafka | 1 | 50004 | 50004 | 1.23 | 2.00 | 229.13 | 995.33 |
| 1-topic-16-partitions-1kb | 1 | kafka | 1 | 50004 | 50004 | 121.50 | 175.00 | 277.38 | 1033.72 |
| backlog-1-topic-1-partition-1kb | 1 | kafka | 1 | 100005 | 99987 | 206.51 | 1038249.98 | 308.98 | 1119.36 |
| backlog-1-topic-16-partitions-1kb | 1 | kafka | 1 | 100008 | 99996 | 216.82 | 1037938.69 | 298.79 | 1115.09 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | kafka | 1 | 743948 | 743948 | 375.44 | 384.00 | 278.26 | 708.96 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | kafka | 1 | 3054976 | 3054986 | 341.13 | 342.00 | 175.25 | 1013.39 |
| 1-topic-3-partition-100b-3producers | 1 | kafka | 1 | 1194045 | 1194017 | 535.65 | 769.00 | 489.36 | 1226.02 |
| max-rate-1-topic-16-partitions-1kb | 1 | kafka | 1 | 363516 | 363517 | 522.90 | 532.00 | 181.22 | 1213.50 |
| max-rate-1-topic-16-partitions-100b | 1 | kafka | 1 | 491280 | 491283 | 311.89 | 340.00 | 70.87 | 1111.74 |
| max-rate-1-topic-100-partitions-1kb | 1 | kafka | 1 | 389361 | 389357 | 286.41 | 291.00 | 214.11 | 1217.14 |
| max-rate-1-topic-100-partitions-100b | 1 | kafka | 1 | 552150 | 552159 | 360.81 | 370.00 | 76.41 | 1214.25 |
| simple-workload | 1 | redpanda | 1 | 10002 | 10002 | 1.26 | 2.00 | 184.43 | 5076.32 |
| 1-topic-1-partition-1kb | 1 | redpanda | 1 | 50006 | 50006 | 1.30 | 2.00 | 394.78 | 2624.70 |
| 1-topic-1-partition-100b | 1 | redpanda | 1 | 50003 | 50003 | 1.24 | 2.00 | 254.60 | 2624.93 |
| 1-topic-16-partitions-1kb | 1 | redpanda | 1 | 50005 | 50005 | 1.31 | 2.00 | 516.71 | 7318.03 |
| backlog-1-topic-1-partition-1kb | 1 | redpanda | 1 | 100005 | 99992 | 2.18 | 1038045.18 | 433.47 | 2689.62 |
| backlog-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 100005 | 99983 | 3.77 | 1037643.78 | 461.78 | 7412.22 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | redpanda | 1 | 509369 | 509369 | 100.96 | 102.00 | 219.93 | 2736.53 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | redpanda | 1 | 3770200 | 3770200 | 9.04 | 13.00 | 244.40 | 2742.12 |
| 1-topic-3-partition-100b-3producers | 1 | redpanda | 1 | 1904261 | 1904260 | 5.35 | 55.00 | 635.23 | 5823.24 |
| max-rate-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 464281 | 464286 | 149.47 | 157.00 | 265.84 | 7382.79 |
| max-rate-1-topic-16-partitions-100b | 1 | redpanda | 1 | 717800 | 717793 | 2.43 | 27.00 | 99.48 | 7357.12 |
| max-rate-1-topic-100-partitions-1kb | 1 | redpanda | 1 | 415203 | 415189 | 165.55 | 174.00 | 276.19 | 7399.66 |
| max-rate-1-topic-100-partitions-100b | 1 | redpanda | 1 | 555269 | 555275 | 1.95 | 20.00 | 105.52 | 7359.36 |
| simple-workload | 3 | krabka | 1 | 10005 | 10005 | 1.73 | 2.00 | 335.77 | 141.41 |
| 1-topic-1-partition-1kb | 3 | krabka | 1 | 50006 | 50006 | 1774.92 | 1933.01 | 903.57 | 719.82 |
| 1-topic-1-partition-100b | 3 | krabka | 1 | 50005 | 50005 | 15.06 | 57.00 | 816.36 | 172.74 |
| 1-topic-16-partitions-1kb | 3 | krabka | 1 | 50006 | 50006 | 359.99 | 405.00 | 964.41 | 275.80 |
| backlog-1-topic-1-partition-1kb | 3 | krabka | 1 | 99985 | 99958 | 1691.62 | 1039876.09 | 1131.21 | 222.98 |
| backlog-1-topic-16-partitions-1kb | 3 | krabka | 1 | 100010 | 99983 | 893.00 | 1037635.58 | 1163.62 | 309.85 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | krabka | 1 | 139207 | 139204 | 3004.05 | 3004.01 | 157.86 | 150.28 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | krabka | 1 | 1510230 | 1510236 | 1858.77 | 1860.01 | 199.60 | 169.74 |
| 1-topic-3-partition-100b-3producers | 3 | krabka | 1 | 896306 | 896075 | 2785.70 | 3040.01 | 1554.59 | 1113.87 |
| max-rate-1-topic-16-partitions-1kb | 3 | krabka | 1 | 84958 | 84959 | 1383.02 | 1432.01 | 334.36 | 262.61 |
| max-rate-1-topic-16-partitions-100b | 3 | krabka | 1 | 283343 | 283383 | 241.40 | 458.00 | 296.06 | 214.20 |
| max-rate-1-topic-100-partitions-100b | 3 | krabka | 1 | 297455 | 297510 | 204.44 | 366.00 | 377.48 | 217.86 |
| simple-workload | 3 | kafka | 1 | 10004 | 10004 | 2.49 | 39.00 | 312.01 | 2949.57 |
| 1-topic-1-partition-1kb | 3 | kafka | 1 | 50006 | 50006 | 1528.10 | 1605.01 | 815.16 | 3613.39 |
| 1-topic-1-partition-100b | 3 | kafka | 1 | 50004 | 50004 | 1.65 | 4.00 | 755.58 | 3010.64 |
| 1-topic-16-partitions-1kb | 3 | kafka | 1 | 50007 | 50007 | 369.91 | 413.00 | 872.46 | 3652.52 |
| backlog-1-topic-1-partition-1kb | 3 | kafka | 1 | 100008 | 99917 | 952.97 | 1038700.54 | 1072.87 | 3614.09 |
| backlog-1-topic-16-partitions-1kb | 3 | kafka | 1 | 100006 | 99960 | 417.36 | 1037078.53 | 1190.22 | 3659.58 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | kafka | 1 | 184922 | 184922 | 2559.81 | 2560.01 | 285.18 | 2248.66 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | kafka | 1 | 1647753 | 1647748 | 1476.30 | 1477.01 | 318.20 | 2420.32 |
| 1-topic-3-partition-100b-3producers | 3 | kafka | 1 | 1100137 | 1100059 | 1951.00 | 2296.01 | 1611.43 | 3677.52 |
| max-rate-1-topic-16-partitions-1kb | 3 | kafka | 1 | 118208 | 118208 | 1156.75 | 1226.01 | 368.30 | 3672.39 |
| max-rate-1-topic-16-partitions-100b | 3 | kafka | 1 | 268932 | 269089 | 271.18 | 495.00 | 302.90 | 3371.78 |
| max-rate-1-topic-100-partitions-100b | 3 | kafka | 1 | 275018 | 275059 | 290.28 | 458.00 | 328.95 | 3448.87 |
| simple-workload | 3 | redpanda | 1 | 10003 | 10003 | 1.42 | 2.00 | 273.11 | 15262.37 |
| 1-topic-1-partition-1kb | 3 | redpanda | 1 | 50004 | 50004 | 2.41 | 3.00 | 747.80 | 7903.26 |
| 1-topic-1-partition-100b | 3 | redpanda | 1 | 50004 | 50004 | 1.36 | 2.00 | 665.18 | 7607.40 |
| 1-topic-16-partitions-1kb | 3 | redpanda | 1 | 50004 | 50004 | 1.78 | 2.00 | 899.61 | 22074.51 |
| backlog-1-topic-1-partition-1kb | 3 | redpanda | 1 | 100007 | 99992 | 6.23 | 1038118.91 | 1262.69 | 7959.63 |
| backlog-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 100005 | 99952 | 69.89 | 1036902.40 | 1189.52 | 22506.23 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | redpanda | 1 | 140705 | 140705 | 343.16 | 343.00 | 233.91 | 7964.54 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | redpanda | 1 | 1435378 | 1435406 | 350.09 | 351.00 | 271.64 | 7959.16 |
| 1-topic-3-partition-100b-3producers | 3 | redpanda | 1 | 1418228 | 1418213 | 415.62 | 467.00 | 1195.63 | 17923.51 |
| max-rate-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 137043 | 137062 | 497.66 | 532.00 | 335.67 | 22656.37 |
| max-rate-1-topic-16-partitions-100b | 3 | redpanda | 1 | 353737 | 353782 | 2.38 | 389.00 | 251.15 | 22048.49 |
| max-rate-1-topic-100-partitions-100b | 3 | redpanda | 1 | 367776 | 367816 | 2.36 | 253.00 | 272.51 | 22036.42 |
| max-rate-1-topic-100-partitions-1kb | 3 | krabka | 1 | 103698 | 103696 | 1593.30 | 1641.01 | 369.25 | 286.54 |
| max-rate-1-topic-100-partitions-1kb | 3 | kafka | 1 | 103624 | 103624 | 1245.99 | 1291.01 | 403.60 | 3663.84 |
| max-rate-1-topic-100-partitions-1kb | 3 | redpanda | 1 | 135484 | 135478 | 526.82 | 556.00 | 343.62 | 22344.66 |
