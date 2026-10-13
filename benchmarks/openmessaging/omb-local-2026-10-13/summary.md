# OpenMessaging broker comparison

Run: omb-local-2026-10-13. Mode: full. Status: complete.

Upstream: https://github.com/openmessaging/benchmark.git at 5b1fa70951a323da26bd587174b58bb2c65b0b5c; Kafka client 3.6.1 for every broker.

Shared host, sequential fresh clusters; 4 logical CPUs and 10 GiB per broker. Buffered acknowledgments; Redpanda uses Raft majorities.
OMB reports latency and rates, not exact delivery/duplicate verification. Resource windows include client startup, warm-up, probing, measurement and shutdown.

- krabka: ghcr.io/krabka-io/krabka-broker@sha256:ee5b77a5e83474ae051e2ea99f5d70a92fae352255297846e82b89e9ced60103
- kafka: apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
- redpanda: docker.redpanda.com/redpandadata/redpanda@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96

| Workload | RF | Broker | Round | Publish msg/s | Consume msg/s | Publish p99 ms | End-to-end p99 ms | CPU seconds | Peak RSS MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| simple-workload | 1 | krabka | 1 | 10002 | 10002 | 1.26 | 2.00 | 83.34 | 44.55 |
| 1-topic-1-partition-1kb | 1 | krabka | 1 | 50004 | 50004 | 1.30 | 2.00 | 245.99 | 359.87 |
| 1-topic-1-partition-100b | 1 | krabka | 1 | 50004 | 50004 | 1.25 | 2.00 | 208.75 | 337.60 |
| 1-topic-16-partitions-1kb | 1 | krabka | 1 | 50005 | 50005 | 1.31 | 2.00 | 269.06 | 299.64 |
| backlog-1-topic-1-partition-1kb | 1 | krabka | 1 | 100005 | 99976 | 30.16 | 1037316.09 | 327.29 | 93.88 |
| backlog-1-topic-16-partitions-1kb | 1 | krabka | 1 | 100005 | 99989 | 23.31 | 1126146.05 | 330.30 | 113.32 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | krabka | 1 | 1123074 | 1123071 | 115.28 | 117.00 | 325.43 | 74.23 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | krabka | 1 | 3524449 | 3524456 | 107.14 | 109.00 | 151.49 | 69.70 |
| 1-topic-3-partition-100b-3producers | 1 | krabka | 1 | 1440912 | 1440912 | 205.33 | 288.00 | 515.76 | 564.80 |
| max-rate-1-topic-16-partitions-1kb | 1 | krabka | 1 | 579048 | 579044 | 235.09 | 238.00 | 192.67 | 76.63 |
| max-rate-1-topic-16-partitions-100b | 1 | krabka | 1 | 585154 | 585160 | 2.07 | 40.00 | 60.83 | 51.38 |
| max-rate-1-topic-100-partitions-1kb | 1 | krabka | 1 | 609453 | 609457 | 224.96 | 227.00 | 229.88 | 77.43 |
| max-rate-1-topic-100-partitions-100b | 1 | krabka | 1 | 554186 | 554164 | 106.05 | 113.00 | 69.76 | 58.43 |
| simple-workload | 1 | kafka | 1 | 10003 | 10003 | 1.23 | 2.00 | 91.46 | 967.73 |
| 1-topic-1-partition-1kb | 1 | kafka | 1 | 50004 | 50004 | 10.62 | 47.00 | 255.65 | 1025.40 |
| 1-topic-1-partition-100b | 1 | kafka | 1 | 50006 | 50006 | 1.23 | 2.00 | 227.83 | 978.27 |
| 1-topic-16-partitions-1kb | 1 | kafka | 1 | 50006 | 50006 | 1.33 | 2.00 | 273.21 | 1014.45 |
| backlog-1-topic-1-partition-1kb | 1 | kafka | 1 | 100005 | 99953 | 56.94 | 1038340.09 | 311.00 | 1077.31 |
| backlog-1-topic-16-partitions-1kb | 1 | kafka | 1 | 100006 | 99998 | 24.66 | 1037799.42 | 305.43 | 1115.38 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | kafka | 1 | 969409 | 969409 | 102.94 | 107.00 | 358.43 | 721.36 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | kafka | 1 | 3360320 | 3360311 | 113.34 | 114.00 | 188.34 | 961.97 |
| 1-topic-3-partition-100b-3producers | 1 | kafka | 1 | 1447706 | 1447702 | 303.48 | 443.00 | 524.24 | 1229.76 |
| max-rate-1-topic-16-partitions-1kb | 1 | kafka | 1 | 469785 | 469786 | 311.79 | 316.00 | 222.30 | 1213.02 |
| max-rate-1-topic-16-partitions-100b | 1 | kafka | 1 | 632670 | 632664 | 98.17 | 112.00 | 75.34 | 1008.09 |
| max-rate-1-topic-100-partitions-1kb | 1 | kafka | 1 | 555428 | 555425 | 226.12 | 230.00 | 280.33 | 1214.28 |
| max-rate-1-topic-100-partitions-100b | 1 | kafka | 1 | 784523 | 784527 | 275.19 | 293.00 | 92.67 | 1214.45 |
| simple-workload | 1 | redpanda | 1 | 10005 | 10005 | 1.25 | 2.00 | 167.43 | 5074.98 |
| 1-topic-1-partition-1kb | 1 | redpanda | 1 | 50005 | 50005 | 1.33 | 2.00 | 353.84 | 2625.96 |
| 1-topic-1-partition-100b | 1 | redpanda | 1 | 50007 | 50007 | 1.24 | 2.00 | 340.28 | 2624.71 |
| 1-topic-16-partitions-1kb | 1 | redpanda | 1 | 50004 | 50004 | 1.30 | 2.00 | 532.95 | 7314.86 |
| backlog-1-topic-1-partition-1kb | 1 | redpanda | 1 | 100005 | 99964 | 1.32 | 1037840.38 | 453.79 | 2651.50 |
| backlog-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 100005 | 99999 | 1.39 | 1038123.01 | 473.92 | 7415.81 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | redpanda | 1 | 903417 | 903413 | 45.51 | 49.00 | 422.40 | 2746.16 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | redpanda | 1 | 3488347 | 3488344 | 111.80 | 112.00 | 196.30 | 2737.64 |
| 1-topic-3-partition-100b-3producers | 1 | redpanda | 1 | 2208591 | 2208563 | 2.96 | 47.00 | 675.23 | 5829.37 |
| max-rate-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 751051 | 751047 | 80.61 | 85.00 | 406.43 | 7391.62 |
| max-rate-1-topic-16-partitions-100b | 1 | redpanda | 1 | 871636 | 871633 | 1.82 | 23.00 | 112.07 | 7352.40 |
| max-rate-1-topic-100-partitions-1kb | 1 | redpanda | 1 | 716286 | 716302 | 90.41 | 95.00 | 440.68 | 7394.53 |
| max-rate-1-topic-100-partitions-100b | 1 | redpanda | 1 | 645804 | 645816 | 1.68 | 17.00 | 113.70 | 7355.14 |
| simple-workload | 3 | krabka | 1 | 10002 | 10002 | 1.75 | 2.00 | 341.68 | 151.38 |
| 1-topic-1-partition-1kb | 3 | krabka | 1 | 50004 | 50004 | 780.34 | 822.00 | 1001.12 | 945.83 |
| 1-topic-1-partition-100b | 3 | krabka | 1 | 50007 | 50007 | 1.71 | 2.00 | 942.04 | 755.43 |
| 1-topic-16-partitions-1kb | 3 | krabka | 1 | 50005 | 50005 | 118.01 | 140.00 | 1060.96 | 905.34 |
| backlog-1-topic-1-partition-1kb | 3 | krabka | 1 | 100008 | 99994 | 988.48 | 1039233.02 | 1151.67 | 236.55 |
| backlog-1-topic-16-partitions-1kb | 3 | krabka | 1 | 100007 | 99982 | 567.28 | 1038065.66 | 1263.39 | 349.77 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | krabka | 1 | 1711915 | 1711920 | 1111.15 | 1118.01 | 229.91 | 190.37 |
| simple-workload | 3 | kafka | 1 | 10004 | 10004 | 1.67 | 3.00 | 322.85 | 2942.50 |
| 1-topic-1-partition-1kb | 3 | kafka | 1 | 50007 | 50007 | 605.26 | 650.00 | 847.44 | 3399.88 |
| 1-topic-1-partition-100b | 3 | kafka | 1 | 50005 | 50005 | 1.58 | 2.00 | 754.51 | 2941.52 |
| 1-topic-16-partitions-1kb | 3 | kafka | 1 | 50005 | 50005 | 127.68 | 151.00 | 920.76 | 3246.82 |
| backlog-1-topic-1-partition-1kb | 3 | kafka | 1 | 100011 | 99914 | 891.18 | 1038622.72 | 1119.90 | 3633.54 |
| backlog-1-topic-16-partitions-1kb | 3 | kafka | 1 | 99994 | 99924 | 217.57 | 1037041.66 | 1177.48 | 3652.61 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | kafka | 1 | 1864473 | 1864531 | 900.60 | 904.00 | 339.57 | 2447.84 |
| simple-workload | 3 | redpanda | 1 | 10002 | 10002 | 1.41 | 2.00 | 298.06 | 15267.18 |
| 1-topic-1-partition-1kb | 3 | redpanda | 1 | 50004 | 50004 | 1.54 | 2.00 | 815.23 | 7890.05 |
| 1-topic-1-partition-100b | 3 | redpanda | 1 | 50004 | 50004 | 1.33 | 2.00 | 548.22 | 7664.99 |
| 1-topic-16-partitions-1kb | 3 | redpanda | 1 | 50004 | 50004 | 1.59 | 2.00 | 917.62 | 22063.64 |
| backlog-1-topic-1-partition-1kb | 3 | redpanda | 1 | 100005 | 100000 | 2.80 | 1038008.32 | 1245.83 | 8083.44 |
| backlog-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 100006 | 99972 | 2.47 | 1037238.27 | 1190.87 | 22590.75 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | redpanda | 1 | 1841374 | 1841390 | 241.68 | 243.00 | 361.59 | 7939.86 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | krabka | 1 | 192663 | 192662 | 1953.22 | 1954.01 | 235.37 | 174.10 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | kafka | 1 | 267116 | 267119 | 1455.11 | 1455.01 | 373.74 | 2274.05 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | redpanda | 1 | 189728 | 189728 | 255.29 | 259.00 | 313.16 | 7974.73 |
| 1-topic-3-partition-100b-3producers | 3 | krabka | 1 | 893297 | 892469 | 2729.66 | 3162.01 | 1568.75 | 950.68 |
| max-rate-1-topic-16-partitions-1kb | 3 | krabka | 1 | 125197 | 125197 | 1203.36 | 1236.01 | 352.89 | 264.01 |
| max-rate-1-topic-16-partitions-100b | 3 | krabka | 1 | 294253 | 294250 | 98.11 | 442.00 | 292.86 | 215.82 |
| max-rate-1-topic-100-partitions-1kb | 3 | krabka | 1 | 124355 | 124358 | 1057.07 | 1065.01 | 418.93 | 266.27 |
| max-rate-1-topic-100-partitions-100b | 3 | krabka | 1 | 297159 | 297233 | 192.26 | 365.00 | 387.22 | 219.62 |
| 1-topic-3-partition-100b-3producers | 3 | kafka | 1 | 1302407 | 1303007 | 1359.67 | 1937.01 | 1630.57 | 3694.38 |
| max-rate-1-topic-16-partitions-1kb | 3 | kafka | 1 | 128258 | 128255 | 1105.75 | 1142.01 | 397.80 | 3672.68 |
| max-rate-1-topic-16-partitions-100b | 3 | kafka | 1 | 281892 | 281891 | 130.14 | 438.00 | 304.03 | 3033.28 |
| max-rate-1-topic-100-partitions-1kb | 3 | kafka | 1 | 128245 | 128245 | 970.86 | 991.00 | 428.10 | 3668.64 |
| max-rate-1-topic-100-partitions-100b | 3 | kafka | 1 | 294909 | 294962 | 161.59 | 335.00 | 332.25 | 3299.02 |
| 1-topic-3-partition-100b-3producers | 3 | redpanda | 1 | 1453537 | 1453543 | 163.81 | 196.00 | 1507.34 | 17891.84 |
| max-rate-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 151977 | 151974 | 393.46 | 427.00 | 372.31 | 22252.87 |
| max-rate-1-topic-16-partitions-100b | 3 | redpanda | 1 | 345731 | 345995 | 2.19 | 392.00 | 257.45 | 22048.70 |
| max-rate-1-topic-100-partitions-1kb | 3 | redpanda | 1 | 173702 | 173700 | 448.89 | 471.00 | 404.53 | 22557.76 |
| max-rate-1-topic-100-partitions-100b | 3 | redpanda | 1 | 341778 | 341815 | 2.13 | 284.00 | 279.13 | 22034.28 |
