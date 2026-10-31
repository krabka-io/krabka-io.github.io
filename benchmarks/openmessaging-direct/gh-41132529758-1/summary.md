# OpenMessaging broker comparison

Run: gh-41132529758-1. Mode: full. Status: complete.

Upstream: https://github.com/openmessaging/benchmark.git at 5b1fa70951a323da26bd587174b58bb2c65b0b5c; Kafka client 3.6.1 for every broker.

Shared host, sequential fresh clusters; 4 logical CPUs and 10 GiB per broker. Buffered acknowledgments; Redpanda uses Raft majorities.
OMB reports latency and rates, not exact delivery/duplicate verification. Resource windows include client startup, warm-up, probing, measurement and shutdown.

- krabka: ghcr.io/krabka-io/krabka-broker@sha256:dd438443a18bfc073590df19cd5a702b35d94d8ddeb6c5193ac6638ddaf2c586
- kafka: apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
- redpanda: docker.redpanda.com/redpandadata/redpanda@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96

| Workload | RF | Broker | Round | Publish msg/s | Consume msg/s | Publish p99 ms | End-to-end p99 ms | CPU seconds | Peak RSS MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| backlog-1-topic-1-partition-1kb | 1 | krabka | 1 | 100012 | 99929 | 2.39 | 1038086.14 | 954.85 | 436.09 |
| backlog-1-topic-1-partition-1kb | 1 | kafka | 1 | 100012 | 99949 | 16.45 | 1039142.91 | 1320.18 | 1156.48 |
| backlog-1-topic-1-partition-1kb | 1 | redpanda | 1 | 100009 | 99966 | 2.88 | 1039400.96 | 2108.65 | 2667.99 |
| simple-workload | 1 | krabka | 1 | 10006 | 10006 | 1.85 | 2.00 | 186.50 | 254.45 |
| simple-workload | 1 | kafka | 1 | 10006 | 10006 | 2.13 | 2.00 | 306.48 | 965.30 |
| simple-workload | 1 | redpanda | 1 | 10010 | 10010 | 2.13 | 2.00 | 446.26 | 4987.85 |
| 1-topic-1-partition-100b | 1 | krabka | 1 | 50013 | 50013 | 1.93 | 2.00 | 517.90 | 91.23 |
| 1-topic-1-partition-100b | 1 | kafka | 1 | 50012 | 50012 | 2.17 | 2.00 | 765.20 | 976.48 |
| 1-topic-1-partition-100b | 1 | redpanda | 1 | 50015 | 50015 | 2.19 | 2.00 | 679.94 | 2612.88 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | krabka | 1 | 358737 | 294551 | 117.19 | 64611.07 | 350.70 | 573.97 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | kafka | 1 | 276545 | 276543 | 211.79 | 214.00 | 388.27 | 728.98 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 1 | redpanda | 1 | 279233 | 279226 | 149.01 | 152.00 | 447.46 | 2642.65 |
| 1-topic-3-partition-100b-3producers | 1 | krabka | 1 | 921461 | 921463 | 8.78 | 137.00 | 965.73 | 646.86 |
| 1-topic-3-partition-100b-3producers | 1 | kafka | 1 | 619645 | 619733 | 13.17 | 177.00 | 1026.34 | 1223.09 |
| 1-topic-3-partition-100b-3producers | 1 | redpanda | 1 | 563868 | 563866 | 18.93 | 489.00 | 1152.79 | 5814.40 |
| max-rate-1-topic-16-partitions-100b | 1 | krabka | 1 | 1003890 | 1003887 | 13.85 | 24.00 | 210.42 | 892.28 |
| max-rate-1-topic-16-partitions-100b | 1 | kafka | 1 | 257113 | 257115 | 4.75 | 76.00 | 221.40 | 976.43 |
| max-rate-1-topic-16-partitions-100b | 1 | redpanda | 1 | 248216 | 248228 | 2.96 | 78.00 | 219.78 | 7326.47 |
| max-rate-1-topic-100-partitions-100b | 1 | krabka | 1 | 776415 | 776397 | 6.66 | 22.00 | 254.99 | 2167.78 |
| max-rate-1-topic-100-partitions-100b | 1 | kafka | 1 | 248300 | 248295 | 5.15 | 44.00 | 220.79 | 999.15 |
| max-rate-1-topic-100-partitions-100b | 1 | redpanda | 1 | 211364 | 211365 | 5.10 | 55.00 | 254.57 | 7329.68 |
| backlog-1-topic-1-partition-1kb | 3 | krabka | 1 | 100010 | 99952 | 3.67 | 1037549.57 | 3070.02 | 1430.57 |
| backlog-1-topic-1-partition-1kb | 3 | kafka | 1 | 97678 | 97560 | 986.73 | 1039409.15 | 2330.73 | 3625.54 |
| backlog-1-topic-1-partition-1kb | 3 | redpanda | 1 | 100006 | 99991 | 6.17 | 1039872.00 | 6048.47 | 8103.48 |
| simple-workload | 3 | krabka | 1 | 10004 | 10004 | 2.96 | 3.00 | 754.14 | 867.78 |
| simple-workload | 3 | kafka | 1 | 10004 | 10004 | 3.52 | 4.00 | 844.37 | 2935.49 |
| simple-workload | 3 | redpanda | 1 | 10004 | 10004 | 2.92 | 3.00 | 701.71 | 14906.26 |
| 1-topic-1-partition-100b | 3 | krabka | 1 | 50008 | 50008 | 2.91 | 3.00 | 2109.46 | 427.61 |
| 1-topic-1-partition-100b | 3 | kafka | 1 | 50006 | 50006 | 3.33 | 4.00 | 2082.02 | 2928.31 |
| 1-topic-1-partition-100b | 3 | redpanda | 1 | 50009 | 50009 | 2.84 | 3.00 | 1620.70 | 7841.30 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | krabka | 1 | 178317 | 178320 | 203.99 | 205.00 | 447.23 | 911.27 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | kafka | 1 | 161451 | 161451 | 438.50 | 439.00 | 624.97 | 2147.50 |
| max-rate-1-topic-1-partition-1p-1c-1kb | 3 | redpanda | 1 | 150024 | 150024 | 250.12 | 251.00 | 705.03 | 7910.34 |
| 1-topic-3-partition-100b-3producers | 3 | krabka | 1 | 1603782 | 1603786 | 103.09 | 208.00 | 3150.66 | 2242.00 |
| 1-topic-3-partition-100b-3producers | 3 | kafka | 1 | 1126681 | 1126658 | 869.42 | 1718.01 | 3130.44 | 3673.48 |
| 1-topic-3-partition-100b-3producers | 3 | redpanda | 1 | 938343 | 938362 | 9.61 | 91.00 | 3179.35 | 17376.10 |
| max-rate-1-topic-16-partitions-100b | 3 | krabka | 1 | 1651257 | 1651248 | 163.99 | 175.00 | 598.07 | 3046.14 |
| max-rate-1-topic-16-partitions-100b | 3 | kafka | 1 | 170385 | 170411 | 6.63 | 488.00 | 709.00 | 2956.61 |
| max-rate-1-topic-16-partitions-100b | 3 | redpanda | 1 | 156334 | 156304 | 4.04 | 491.00 | 655.51 | 21059.66 |
| max-rate-1-topic-100-partitions-100b | 3 | krabka | 1 | 1553885 | 1553863 | 24.08 | 35.00 | 724.81 | 8754.32 |
| max-rate-1-topic-100-partitions-100b | 3 | kafka | 1 | 157467 | 157467 | 9.93 | 479.00 | 762.04 | 3001.26 |
| max-rate-1-topic-100-partitions-100b | 3 | redpanda | 1 | 183035 | 183059 | 4.47 | 472.00 | 723.49 | 21282.38 |
| backlog-1-topic-16-partitions-1kb | 1 | krabka | 1 | 100013 | 99866 | 3.71 | 1040662.53 | 1193.58 | 1730.93 |
| backlog-1-topic-16-partitions-1kb | 1 | kafka | 1 | 100013 | 99924 | 18.06 | 1040945.15 | 1764.87 | 1160.51 |
| backlog-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 100020 | 99859 | 17.48 | 1043070.97 | 2318.09 | 7615.23 |
| 1-topic-1-partition-1kb | 1 | krabka | 1 | 50012 | 50012 | 2.50 | 3.00 | 640.77 | 224.21 |
| 1-topic-1-partition-1kb | 1 | kafka | 1 | 50011 | 50011 | 6.04 | 8.00 | 1022.34 | 1006.96 |
| 1-topic-1-partition-1kb | 1 | redpanda | 1 | 50019 | 50019 | 7.65 | 12.00 | 1263.83 | 2647.45 |
| 1-topic-16-partitions-1kb | 1 | krabka | 1 | 50013 | 50013 | 2.48 | 3.00 | 704.67 | 869.75 |
| 1-topic-16-partitions-1kb | 1 | kafka | 1 | 50013 | 50013 | 9.74 | 14.00 | 1218.40 | 1023.14 |
| 1-topic-16-partitions-1kb | 1 | redpanda | 1 | 50018 | 50017 | 6.66 | 12.00 | 1578.40 | 7353.12 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | krabka | 1 | 1037008 | 1037111 | 336.29 | 422.00 | 181.40 | 429.13 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | kafka | 1 | 853467 | 853483 | 295.51 | 317.00 | 257.55 | 1145.88 |
| max-rate-1-topic-1-partition-1p-1c-100b | 1 | redpanda | 1 | 943858 | 943879 | 330.60 | 340.00 | 262.18 | 2636.07 |
| max-rate-1-topic-16-partitions-1kb | 1 | krabka | 1 | 273854 | 273859 | 241.49 | 247.00 | 327.33 | 1220.32 |
| max-rate-1-topic-16-partitions-1kb | 1 | kafka | 1 | 219176 | 219186 | 284.23 | 293.00 | 467.23 | 1228.01 |
| max-rate-1-topic-16-partitions-1kb | 1 | redpanda | 1 | 188178 | 188178 | 297.53 | 309.00 | 513.00 | 7437.35 |
| max-rate-1-topic-100-partitions-1kb | 1 | krabka | 1 | 273059 | 273059 | 217.78 | 254.00 | 348.77 | 3912.39 |
| max-rate-1-topic-100-partitions-1kb | 1 | kafka | 1 | 203027 | 203030 | 301.71 | 315.00 | 496.50 | 1221.45 |
| max-rate-1-topic-100-partitions-1kb | 1 | redpanda | 1 | 166889 | 166891 | 370.37 | 383.00 | 571.70 | 7446.87 |
| backlog-1-topic-16-partitions-1kb | 3 | krabka | 1 | 100021 | 99827 | 8.20 | 1038442.49 | 3343.41 | 4369.50 |
| backlog-1-topic-16-partitions-1kb | 3 | kafka | 1 | 100016 | 99845 | 84.69 | 1037869.06 | 3362.49 | 3644.45 |
| backlog-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 100018 | 99758 | 18.83 | 1037524.99 | 4086.38 | 22481.41 |
| 1-topic-1-partition-1kb | 3 | krabka | 1 | 50016 | 50016 | 3.58 | 4.00 | 2271.67 | 641.31 |
| 1-topic-1-partition-1kb | 3 | kafka | 1 | 50016 | 50016 | 236.85 | 239.00 | 1741.80 | 3562.83 |
| 1-topic-1-partition-1kb | 3 | redpanda | 1 | 50013 | 50013 | 6.17 | 7.00 | 2496.69 | 7906.75 |
| 1-topic-16-partitions-1kb | 3 | krabka | 1 | 50018 | 50018 | 4.63 | 5.00 | 2207.64 | 2658.03 |
| 1-topic-16-partitions-1kb | 3 | kafka | 1 | 50017 | 50017 | 57.38 | 89.00 | 2137.16 | 3360.44 |
| 1-topic-16-partitions-1kb | 3 | redpanda | 1 | 50013 | 50013 | 7.32 | 9.00 | 2862.88 | 22051.01 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | krabka | 1 | 903503 | 903509 | 379.29 | 387.00 | 341.76 | 539.94 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | kafka | 1 | 567708 | 567708 | 891.84 | 906.00 | 474.15 | 2219.57 |
| max-rate-1-topic-1-partition-1p-1c-100b | 3 | redpanda | 1 | 761605 | 761623 | 156.01 | 160.00 | 513.50 | 7905.20 |
| max-rate-1-topic-16-partitions-1kb | 3 | krabka | 1 | 178271 | 178271 | 465.44 | 470.00 | 620.04 | 4005.29 |
| max-rate-1-topic-16-partitions-1kb | 3 | kafka | 1 | 137942 | 137964 | 185.65 | 300.00 | 984.77 | 3627.54 |
| max-rate-1-topic-16-partitions-1kb | 3 | redpanda | 1 | 137172 | 137159 | 55.39 | 216.00 | 1034.42 | 22150.45 |
| max-rate-1-topic-100-partitions-1kb | 3 | krabka | 1 | 176521 | 176516 | 817.38 | 821.00 | 803.88 | 11878.03 |
| max-rate-1-topic-100-partitions-1kb | 3 | kafka | 1 | 137278 | 137279 | 159.42 | 189.00 | 1092.16 | 3005.89 |
| max-rate-1-topic-100-partitions-1kb | 3 | redpanda | 1 | 133437 | 133441 | 276.20 | 305.00 | 1152.52 | 22154.25 |
