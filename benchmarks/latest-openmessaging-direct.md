[Dated report and per-trial results](openmessaging-direct/gh-41217693736-1/summary.md)

# OpenMessaging broker comparison

Run: gh-41217693736-1. Mode: full. Status: complete.

Upstream: https://github.com/openmessaging/benchmark.git at 5b1fa70951a323da26bd587174b58bb2c65b0b5c; Kafka client 3.6.1 for every broker.

Shared host, sequential fresh clusters; 4 logical CPUs and 10 GiB per broker. Buffered acknowledgments; Redpanda uses Raft majorities.
OMB reports latency and rates, not exact delivery/duplicate verification. Resource windows include client startup, warm-up, probing, measurement and shutdown.

- krabka: ghcr.io/krabka-io/krabka-broker@sha256:577a686c1b24a5a655ec8a2a82c877e119e3dd3f07b5fa617e91d05851714e1e
- kafka: apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
- redpanda: docker.redpanda.com/redpandadata/redpanda@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96

| Workload | RF | Broker | Round | Publish msg/s | Consume msg/s | Publish p99 ms | End-to-end p99 ms | CPU seconds | Peak RSS MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| simple-workload | 1 | krabka | 1 | 10004 | 10004 | 1.90 | 2.00 | 182.52 | 1385.07 |
| simple-workload | 1 | kafka | 1 | 10009 | 10009 | 2.16 | 2.00 | 309.55 | 967.57 |
| simple-workload | 1 | redpanda | 1 | 10011 | 10011 | 2.43 | 3.00 | 483.29 | 4971.60 |
| 1-topic-1-partition-100b | 1 | krabka | 1 | 50013 | 50013 | 1.90 | 2.00 | 458.67 | 1371.38 |
| 1-topic-1-partition-100b | 1 | kafka | 1 | 50011 | 50011 | 2.22 | 2.00 | 798.17 | 980.48 |
| 1-topic-1-partition-100b | 1 | redpanda | 1 | 50013 | 50013 | 2.10 | 2.00 | 831.98 | 2615.06 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | krabka | 1 | 315268 | 315263 | 168.88 | 171.00 | 259.27 | 1598.78 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | kafka | 1 | 272465 | 272463 | 211.16 | 214.00 | 395.70 | 717.27 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | redpanda | 1 | 258882 | 258884 | 160.54 | 163.00 | 439.82 | 2639.72 |
| 1-topic-3-partition-100b-3producers | 1 | krabka | 1 | 941815 | 941820 | 12.81 | 177.00 | 931.02 | 1740.99 |
| 1-topic-3-partition-100b-3producers | 1 | kafka | 1 | 618984 | 618978 | 16.38 | 175.00 | 1084.59 | 1143.14 |
| 1-topic-3-partition-100b-3producers | 1 | redpanda | 1 | 559575 | 559567 | 9.88 | 244.00 | 1149.37 | 5800.54 |
| max-rate-1-topic-16-partitions-100b | 1 | krabka | 1 | 1054446 | 1054441 | 18.45 | 37.00 | 221.01 | 1627.75 |
| max-rate-1-topic-16-partitions-100b | 1 | kafka | 1 | 260788 | 260813 | 4.92 | 77.00 | 206.06 | 973.00 |
| max-rate-1-topic-16-partitions-100b | 1 | redpanda | 1 | 230676 | 230695 | 3.93 | 84.00 | 224.06 | 7330.74 |
| max-rate-1-topic-100-partitions-100b | 1 | krabka | 1 | 795926 | 795929 | 7.90 | 22.00 | 253.48 | 1851.07 |
| max-rate-1-topic-100-partitions-100b | 1 | kafka | 1 | 228064 | 228072 | 5.74 | 51.00 | 230.64 | 1017.95 |
| max-rate-1-topic-100-partitions-100b | 1 | redpanda | 1 | 190797 | 190809 | 3.17 | 54.00 | 275.30 | 7297.26 |
| simple-workload | 3 | krabka | 1 | 10005 | 10005 | 3.15 | 4.00 | 779.14 | 4176.10 |
| simple-workload | 3 | kafka | 1 | 10009 | 10009 | 4.64 | 5.00 | 894.93 | 2889.96 |
| simple-workload | 3 | redpanda | 1 | 10007 | 10007 | 5.34 | 7.00 | 975.64 | 15015.04 |
| 1-topic-1-partition-100b | 3 | krabka | 1 | 50014 | 50014 | 3.48 | 4.00 | 2124.33 | 4166.43 |
| 1-topic-1-partition-100b | 3 | kafka | 1 | 50012 | 50012 | 8.83 | 9.00 | 2248.27 | 2915.01 |
| 1-topic-1-partition-100b | 3 | redpanda | 1 | 50018 | 50018 | 3.73 | 5.00 | 2174.87 | 7859.64 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | krabka | 1 | 177631 | 177633 | 236.19 | 239.00 | 559.46 | 4435.73 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | kafka | 1 | 101984 | 101984 | 754.90 | 756.00 | 652.48 | 2135.30 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | redpanda | 1 | 113957 | 113957 | 396.61 | 398.00 | 688.81 | 7909.21 |
| 1-topic-3-partition-100b-3producers | 3 | krabka | 1 | 871457 | 871454 | 45.86 | 751.00 | 3200.34 | 5384.13 |
| 1-topic-3-partition-100b-3producers | 3 | kafka | 1 | 763845 | 763809 | 767.48 | 1721.01 | 3279.41 | 3657.75 |
| 1-topic-3-partition-100b-3producers | 3 | redpanda | 1 | 428196 | 428163 | 10.01 | 245.00 | 3211.80 | 17351.77 |
| max-rate-1-topic-16-partitions-100b | 3 | krabka | 1 | 892952 | 892948 | 36.66 | 67.00 | 574.17 | 4612.63 |
| max-rate-1-topic-16-partitions-100b | 3 | kafka | 1 | 126172 | 126107 | 10.50 | 497.00 | 762.41 | 2953.55 |
| max-rate-1-topic-16-partitions-100b | 3 | redpanda | 1 | 119383 | 119413 | 5.63 | 499.00 | 755.08 | 19666.46 |
| max-rate-1-topic-100-partitions-100b | 3 | krabka | 1 | 618605 | 618609 | 44.25 | 94.00 | 687.54 | 5998.57 |
| max-rate-1-topic-100-partitions-100b | 3 | kafka | 1 | 103695 | 103685 | 16.38 | 492.00 | 786.92 | 2969.50 |
| max-rate-1-topic-100-partitions-100b | 3 | redpanda | 1 | 109288 | 109262 | 5.47 | 493.00 | 878.80 | 17002.50 |
| backlog-1-topic-16-partitions-1kb | 1 | krabka | 1 | 100013 | 99896 | 4.39 | 1041047.55 | 1244.61 | 2506.32 |
| backlog-1-topic-16-partitions-1kb | 1 | kafka | 1 | 100014 | 99923 | 15.94 | 1041764.35 | 1860.19 | 1220.84 |
| backlog-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 100015 | 99904 | 9.61 | 1039966.21 | 2377.29 | 7540.38 |
| 1-topic-1-partition-1kb | 1 | krabka | 1 | 50011 | 50011 | 2.56 | 3.00 | 665.68 | 1468.01 |
| 1-topic-1-partition-1kb | 1 | kafka | 1 | 50015 | 50015 | 7.54 | 10.00 | 1112.55 | 1022.58 |
| 1-topic-1-partition-1kb | 1 | redpanda | 1 | 50016 | 50016 | 3.20 | 3.00 | 908.97 | 2629.91 |
| 1-topic-16-partitions-1kb | 1 | krabka | 1 | 50014 | 50014 | 2.57 | 3.00 | 718.70 | 1622.42 |
| 1-topic-16-partitions-1kb | 1 | kafka | 1 | 50015 | 50015 | 7.66 | 11.00 | 1215.45 | 1029.18 |
| 1-topic-16-partitions-1kb | 1 | redpanda | 1 | 50016 | 50016 | 4.40 | 7.00 | 1574.70 | 7347.25 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | krabka | 1 | 1074334 | 1074329 | 339.64 | 352.00 | 171.39 | 1567.20 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | kafka | 1 | 801773 | 801799 | 351.63 | 394.00 | 245.04 | 1195.48 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | redpanda | 1 | 927974 | 927988 | 254.01 | 262.00 | 279.98 | 2635.31 |
| max-rate-1-topic-16-partitions-1kb | 1 | krabka | 1 | 245470 | 245474 | 360.20 | 390.00 | 308.66 | 1947.76 |
| max-rate-1-topic-16-partitions-1kb | 1 | kafka | 1 | 205810 | 205807 | 283.43 | 293.00 | 503.26 | 1210.31 |
| max-rate-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 207662 | 207661 | 315.46 | 325.00 | 508.08 | 7379.89 |
| max-rate-1-topic-100-partitions-1kb | 1 | krabka | 1 | 260931 | 260929 | 195.93 | 206.00 | 351.53 | 2296.76 |
| max-rate-1-topic-100-partitions-1kb | 1 | kafka | 1 | 227810 | 227811 | 245.65 | 263.00 | 499.83 | 1218.66 |
| max-rate-1-topic-100-partitions-1kb | 1 | redpanda | 1 | 173057 | 173054 | 323.49 | 335.00 | 562.14 | 7424.11 |
| backlog-1-topic-16-partitions-1kb | 3 | krabka | 1 | 100019 | 99795 | 8.72 | 1039036.42 | 3333.96 | 6853.19 |
| backlog-1-topic-16-partitions-1kb | 3 | kafka | 1 | 100015 | 99789 | 109.67 | 1038123.01 | 3566.03 | 3642.99 |
| backlog-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 100033 | 99866 | 15.51 | 1038426.11 | 3910.09 | 22362.54 |
| 1-topic-1-partition-1kb | 3 | krabka | 1 | 50014 | 50014 | 3.66 | 4.00 | 1935.52 | 4362.85 |
| 1-topic-1-partition-1kb | 3 | kafka | 1 | 50015 | 50015 | 257.67 | 260.00 | 1762.04 | 3580.71 |
| 1-topic-1-partition-1kb | 3 | redpanda | 1 | 50020 | 50020 | 4.45 | 5.00 | 2103.57 | 7906.84 |
| 1-topic-16-partitions-1kb | 3 | krabka | 1 | 50011 | 50011 | 3.85 | 4.00 | 2180.99 | 4702.66 |
| 1-topic-16-partitions-1kb | 3 | kafka | 1 | 50016 | 50016 | 52.71 | 89.00 | 2146.49 | 3326.38 |
| 1-topic-16-partitions-1kb | 3 | redpanda | 1 | 50017 | 50017 | 8.30 | 11.00 | 2830.60 | 22115.08 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | krabka | 1 | 900454 | 900369 | 292.39 | 304.00 | 358.90 | 4210.04 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | kafka | 1 | 597101 | 597088 | 794.27 | 798.00 | 480.19 | 2232.30 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | redpanda | 1 | 728805 | 728818 | 104.65 | 109.00 | 531.73 | 7909.46 |
| max-rate-1-topic-16-partitions-1kb | 3 | krabka | 1 | 178417 | 178418 | 451.99 | 456.00 | 648.73 | 5860.38 |
| max-rate-1-topic-16-partitions-1kb | 3 | kafka | 1 | 121404 | 121420 | 250.97 | 328.00 | 967.81 | 3453.77 |
| max-rate-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 129140 | 129111 | 208.90 | 291.00 | 1023.65 | 22238.60 |
| max-rate-1-topic-100-partitions-1kb | 3 | krabka | 1 | 175655 | 175659 | 834.68 | 840.00 | 786.71 | 8328.77 |
| max-rate-1-topic-100-partitions-1kb | 3 | kafka | 1 | 144167 | 144157 | 123.97 | 162.00 | 1058.63 | 3601.95 |
| max-rate-1-topic-100-partitions-1kb | 3 | redpanda | 1 | 144236 | 144240 | 180.08 | 209.00 | 1105.27 | 22155.42 |
| backlog-1-topic-1-partition-1kb | 1 | krabka | 1 | 100013 | 99877 | 2.55 | 1037942.78 | 1132.53 | 1816.46 |
| backlog-1-topic-1-partition-1kb | 1 | kafka | 1 | 100013 | 99939 | 22.56 | 1038491.65 | 1801.02 | 1168.81 |
| backlog-1-topic-1-partition-1kb | 1 | redpanda | 1 | 100011 | 99976 | 4.75 | 1038934.02 | 2564.18 | 2700.67 |
| backlog-1-topic-1-partition-1kb | 3 | krabka | 1 | 100015 | 99943 | 3.31 | 1038258.18 | 3274.07 | 5083.03 |
| backlog-1-topic-1-partition-1kb | 3 | kafka | 1 | 95309 | 95009 | 707.20 | 1039753.21 | 2567.74 | 3625.35 |
| backlog-1-topic-1-partition-1kb | 3 | redpanda | 1 | 100010 | 99954 | 6.56 | 1039962.11 | 6162.56 | 8118.25 |
