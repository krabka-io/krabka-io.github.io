[Dated report and per-trial results](openmessaging/omb-local-2026-10-14/summary.md)

# OpenMessaging broker comparison

Run: omb-local-2026-10-14. Mode: full. Status: complete.

Upstream: https://github.com/openmessaging/benchmark.git at 5b1fa70951a323da26bd587174b58bb2c65b0b5c; Kafka client 3.6.1 for every broker.

Shared host, sequential fresh clusters; 4 logical CPUs and 10 GiB per broker. Buffered acknowledgments; Redpanda uses Raft majorities.
OMB reports latency and rates, not exact delivery/duplicate verification. Resource windows include client startup, warm-up, probing, measurement and shutdown.

- krabka: ghcr.io/krabka-io/krabka-broker@sha256:2a81d1ba38de4e8ffb032fedf67952845185158a7607ac76ba38fa162dc37e50
- kafka: apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
- redpanda: docker.redpanda.com/redpandadata/redpanda@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96

| Workload | RF | Broker | Round | Publish msg/s | Consume msg/s | Publish p99 ms | End-to-end p99 ms | CPU seconds | Peak RSS MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| simple-workload | 1 | krabka | 1 | 10002 | 10002 | 1.25 | 2.00 | 86.48 | 45.38 |
| 1-topic-1-partition-1kb | 1 | krabka | 1 | 50004 | 50004 | 1.30 | 2.00 | 250.32 | 302.84 |
| 1-topic-1-partition-100b | 1 | krabka | 1 | 50006 | 50006 | 1.25 | 2.00 | 210.54 | 256.04 |
| 1-topic-16-partitions-1kb | 1 | krabka | 1 | 50003 | 50003 | 1.31 | 2.00 | 273.94 | 294.61 |
| backlog-1-topic-1-partition-1kb | 1 | krabka | 1 | 100005 | 99989 | 10.26 | 1037762.56 | 296.05 | 220.57 |
| backlog-1-topic-16-partitions-1kb | 1 | krabka | 1 | 100007 | 99989 | 5.07 | 1076977.66 | 335.58 | 275.51 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | krabka | 1 | 1139780 | 1139781 | 103.50 | 104.00 | 329.00 | 74.47 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | krabka | 1 | 3593900 | 3593884 | 110.75 | 111.00 | 151.57 | 71.02 |
| 1-topic-3-partition-100b-3producers | 1 | krabka | 1 | 1620371 | 1620343 | 131.28 | 197.00 | 537.35 | 566.39 |
| max-rate-1-topic-16-partitions-1kb | 1 | krabka | 1 | 741306 | 741313 | 175.35 | 177.00 | 233.36 | 78.41 |
| max-rate-1-topic-16-partitions-100b | 1 | krabka | 1 | 628123 | 628130 | 1.72 | 41.00 | 61.41 | 53.23 |
| max-rate-1-topic-100-partitions-1kb | 1 | krabka | 1 | 818326 | 818326 | 144.21 | 146.00 | 286.18 | 78.31 |
| max-rate-1-topic-100-partitions-100b | 1 | krabka | 1 | 2229882 | 2229852 | 137.03 | 141.00 | 159.84 | 62.81 |
| simple-workload | 1 | kafka | 1 | 10002 | 10002 | 1.22 | 2.00 | 88.23 | 962.41 |
| 1-topic-1-partition-1kb | 1 | kafka | 1 | 50004 | 50004 | 1.32 | 2.00 | 254.34 | 1013.13 |
| 1-topic-1-partition-100b | 1 | kafka | 1 | 50003 | 50003 | 1.23 | 2.00 | 229.26 | 988.98 |
| 1-topic-16-partitions-1kb | 1 | kafka | 1 | 50004 | 50004 | 1.35 | 2.00 | 271.35 | 1026.95 |
| backlog-1-topic-1-partition-1kb | 1 | kafka | 1 | 100004 | 99987 | 45.39 | 1038172.16 | 317.74 | 1154.75 |
| backlog-1-topic-16-partitions-1kb | 1 | kafka | 1 | 100004 | 99988 | 9.26 | 1038213.12 | 303.39 | 1078.29 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | kafka | 1 | 1016551 | 1016534 | 93.53 | 96.00 | 373.56 | 716.55 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | kafka | 1 | 3497974 | 3497963 | 77.21 | 79.00 | 194.99 | 991.66 |
| 1-topic-3-partition-100b-3producers | 1 | kafka | 1 | 1748596 | 1748647 | 172.75 | 273.00 | 540.95 | 1225.39 |
| max-rate-1-topic-16-partitions-1kb | 1 | kafka | 1 | 697199 | 697201 | 174.07 | 177.00 | 277.54 | 1207.82 |
| max-rate-1-topic-16-partitions-100b | 1 | kafka | 1 | 1178434 | 1178421 | 79.01 | 89.00 | 104.12 | 1075.06 |
| max-rate-1-topic-100-partitions-1kb | 1 | kafka | 1 | 729900 | 729896 | 150.29 | 153.00 | 311.03 | 1200.49 |
| max-rate-1-topic-100-partitions-100b | 1 | kafka | 1 | 1353444 | 1353442 | 66.79 | 72.00 | 127.78 | 1129.53 |
| simple-workload | 1 | redpanda | 1 | 10003 | 10003 | 1.26 | 2.00 | 180.84 | 5099.21 |
| 1-topic-1-partition-1kb | 1 | redpanda | 1 | 50005 | 50005 | 1.33 | 2.00 | 303.68 | 2622.54 |
| 1-topic-1-partition-100b | 1 | redpanda | 1 | 50004 | 50004 | 1.24 | 2.00 | 380.95 | 2626.30 |
| 1-topic-16-partitions-1kb | 1 | redpanda | 1 | 50004 | 50004 | 1.30 | 2.00 | 539.24 | 7318.03 |
| backlog-1-topic-1-partition-1kb | 1 | redpanda | 1 | 100006 | 99983 | 1.40 | 1037959.17 | 426.39 | 2683.94 |
| backlog-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 100005 | 99971 | 1.48 | 1038163.97 | 476.03 | 7414.72 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | redpanda | 1 | 1054932 | 1054928 | 39.35 | 43.00 | 457.51 | 2734.08 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | redpanda | 1 | 3973941 | 3973941 | 43.81 | 45.00 | 255.34 | 2640.35 |
| 1-topic-3-partition-100b-3producers | 1 | redpanda | 1 | 2057763 | 2057775 | 2.62 | 50.00 | 642.28 | 5800.03 |
| max-rate-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 908991 | 908991 | 47.90 | 52.00 | 457.83 | 7391.48 |
| max-rate-1-topic-16-partitions-100b | 1 | redpanda | 1 | 594485 | 594499 | 1.47 | 33.00 | 92.13 | 7360.96 |
| max-rate-1-topic-100-partitions-1kb | 1 | redpanda | 1 | 897697 | 897700 | 48.05 | 53.00 | 500.81 | 7403.07 |
| max-rate-1-topic-100-partitions-100b | 1 | redpanda | 1 | 625489 | 625496 | 1.61 | 17.00 | 115.13 | 7362.15 |
| simple-workload | 3 | krabka | 1 | 10003 | 10003 | 1.76 | 2.00 | 348.86 | 141.50 |
| 1-topic-1-partition-1kb | 3 | krabka | 1 | 50004 | 50005 | 457.44 | 476.00 | 1013.78 | 851.19 |
| 1-topic-1-partition-100b | 3 | krabka | 1 | 50004 | 50004 | 1.72 | 2.00 | 944.57 | 907.61 |
| 1-topic-16-partitions-1kb | 3 | krabka | 1 | 50004 | 50004 | 36.30 | 60.00 | 1067.38 | 1004.18 |
| backlog-1-topic-1-partition-1kb | 3 | krabka | 1 | 100009 | 99964 | 700.51 | 1037574.14 | 1181.15 | 368.76 |
| backlog-1-topic-16-partitions-1kb | 3 | krabka | 1 | 100006 | 99915 | 215.13 | 1037156.35 | 1403.41 | 506.02 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | krabka | 1 | 377487 | 377487 | 693.15 | 694.00 | 438.64 | 160.14 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | krabka | 1 | 2108784 | 2108783 | 672.90 | 674.00 | 289.55 | 167.51 |
| 1-topic-3-partition-100b-3producers | 3 | krabka | 1 | 1531034 | 1531032 | 845.75 | 1702.01 | 1956.28 | 928.66 |
| simple-workload | 3 | kafka | 1 | 10003 | 10003 | 1.60 | 2.00 | 325.27 | 2938.30 |
| 1-topic-1-partition-1kb | 3 | kafka | 1 | 50004 | 50004 | 475.20 | 508.00 | 861.79 | 3242.01 |
| 1-topic-1-partition-100b | 3 | kafka | 1 | 50004 | 50004 | 1.56 | 2.00 | 757.32 | 2967.52 |
| 1-topic-16-partitions-1kb | 3 | kafka | 1 | 50004 | 50004 | 47.88 | 76.00 | 927.45 | 3151.89 |
| backlog-1-topic-1-partition-1kb | 3 | kafka | 1 | 100008 | 99897 | 871.62 | 1038852.09 | 1090.56 | 3645.90 |
| backlog-1-topic-16-partitions-1kb | 3 | kafka | 1 | 100007 | 99956 | 146.45 | 1037017.09 | 1195.63 | 3621.94 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | kafka | 1 | 374241 | 374241 | 712.27 | 713.00 | 520.07 | 2256.92 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | kafka | 1 | 2203941 | 2203941 | 490.40 | 491.00 | 377.61 | 2406.90 |
| 1-topic-3-partition-100b-3producers | 3 | kafka | 1 | 1622371 | 1621663 | 819.50 | 1488.01 | 1856.11 | 3671.66 |
| simple-workload | 3 | redpanda | 1 | 10002 | 10002 | 1.42 | 2.00 | 308.11 | 15211.46 |
| 1-topic-1-partition-1kb | 3 | redpanda | 1 | 50004 | 50004 | 1.54 | 2.00 | 844.26 | 7917.11 |
| 1-topic-1-partition-100b | 3 | redpanda | 1 | 50007 | 50006 | 1.36 | 2.00 | 686.76 | 7665.69 |
| 1-topic-16-partitions-1kb | 3 | redpanda | 1 | 50004 | 50004 | 1.59 | 2.00 | 909.47 | 22063.89 |
| backlog-1-topic-1-partition-1kb | 3 | redpanda | 1 | 100004 | 99977 | 1.69 | 1037684.73 | 1225.88 | 8024.83 |
| backlog-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 100006 | 99980 | 2.07 | 1037332.48 | 1190.51 | 22510.00 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | redpanda | 1 | 387738 | 387740 | 103.78 | 105.00 | 577.40 | 7931.41 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | redpanda | 1 | 2859077 | 2859077 | 133.66 | 134.00 | 497.83 | 7923.63 |
| max-rate-1-topic-16-partitions-1kb | 3 | krabka | 1 | 292399 | 292409 | 302.75 | 315.00 | 506.99 | 266.59 |
| max-rate-1-topic-16-partitions-100b | 3 | krabka | 1 | 313798 | 313946 | 3.10 | 439.00 | 303.32 | 196.50 |
| max-rate-1-topic-100-partitions-1kb | 3 | krabka | 1 | 336115 | 336107 | 363.80 | 368.00 | 626.72 | 272.42 |
| max-rate-1-topic-100-partitions-100b | 3 | krabka | 1 | 287511 | 287517 | 3.00 | 303.00 | 404.00 | 190.75 |
| max-rate-1-topic-16-partitions-1kb | 3 | kafka | 1 | 281411 | 281396 | 316.36 | 333.00 | 543.90 | 3646.03 |
| max-rate-1-topic-16-partitions-100b | 3 | kafka | 1 | 271813 | 271813 | 20.22 | 468.00 | 298.14 | 3025.86 |
| max-rate-1-topic-100-partitions-1kb | 3 | kafka | 1 | 325701 | 325699 | 318.50 | 325.00 | 654.50 | 3665.69 |
| max-rate-1-topic-100-partitions-100b | 3 | kafka | 1 | 331275 | 331370 | 50.53 | 297.00 | 323.41 | 3283.93 |
| max-rate-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 371910 | 371910 | 60.58 | 102.00 | 702.58 | 22207.52 |
| max-rate-1-topic-16-partitions-100b | 3 | redpanda | 1 | 350888 | 351028 | 2.02 | 385.00 | 257.23 | 22057.76 |
| max-rate-1-topic-100-partitions-1kb | 3 | redpanda | 1 | 380969 | 380971 | 173.82 | 184.00 | 786.89 | 22277.30 |
| max-rate-1-topic-100-partitions-100b | 3 | redpanda | 1 | 364312 | 364267 | 2.12 | 262.00 | 263.24 | 22037.31 |

## Not measured

- 1-topic-3-partition-100b-3producers, RF3, redpanda: Redpanda's trial reached the runner's 20 GiB disk reserve before its 15 minutes ended: with unlimited retention at RF3, its three brokers filled the about 670 GiB that was free above the reserve. The trial was not run again.
