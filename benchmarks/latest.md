[Dated report and per-trial results](results/2026-10-03T21-05-54Z-d06d64d6/summary.md)

# Krabka, Kafka 4.3.1, and Redpanda — local benchmark

> **Krabka version:** these results are from krabka-broker 0.7.0. They have not been re-run for 1.0.0; a 1.0.0 run needs the dedicated benchmark host.

Run: 2026-10-03T21-05-54Z-d06d64d6. Completed: 2026-10-03T21:55:43.496Z.

These are buffered-write, local end-to-end throughput measurements. Each row reports the median of three independent repetitions, followed by the observed minimum–maximum range. Comparisons apply only to these images, this host, and this workload.

## Images and host

- krabka: `ghcr.io/krabka-io/krabka-broker:v0.7.0` → `ghcr.io/krabka-io/krabka-broker@sha256:54ae711e5a7c8bdb03414402722f5c41dd50980ab315ab84d0803cca775fb463`.
- kafka: `apache/kafka:4.3.1` → `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
- redpanda: `docker.redpanda.com/redpandadata/redpanda:v26.2.2` → `docker.redpanda.com/redpandadata/redpanda@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96`.
- Host: AMD EPYC 4344P 8-Core Processor; 16 logical CPUs; 7.0.0-31-generic; 65933004800 bytes RAM.
- Broker CPU sets: 0,8,1,9 / 2,10,3,11 / 4,12,5,13. Client CPUs: 6,14,7,15. Logical CPUs can be SMT siblings; topology is retained in provenance.
- Per broker: 4 logical CPUs, 4 CPU quota, 10 GiB memory, no swap, 131,072 open files. Kafka heap: 1 GiB. Redpanda: 4 shards, 8 GiB application memory, 1 GiB reserve.
- Redpanda networking AIO control blocks: 1,024 per shard, explicitly fixed so three nodes fit the shared host AIO budget. Host sysctls are not changed.
- Same Kafka 4.3.1 client jars for all vendors. Fresh cluster/storage per repetition; vendor order rotates. Warm-up: 3 million records for RF1, 1 million for RF3. Case order matches the tables.
- Twelve partitions, RF1/minISR1 or RF3/minISR2, full ISR before each workload, acks=all, idempotence, 65,536-byte batches, 5 ms linger. All acknowledged records consumed exactly once, with no producer errors.
- Kafka/Krabka acknowledge their in-sync replication contract; Redpanda uses Raft majority acknowledgment with write.caching=true. This does not establish identical failure or crash-durability guarantees.

## Measurement boundaries

Throughput and latency come from the Java producer/consumer workload. Broker CPU is the cgroup cpu.stat usage delta; its window includes client startup, initialization, and shutdown. CPU per record divides aggregate broker CPU by acknowledged records, not replicas. Memory is sampled every 250 ms, and RF3 peaks use the simultaneous sum across brokers. Working set is memory.current minus inactive_file; RSS excludes most disk page cache. Sampling gaps are retained per trial. Warm-up is excluded; brokers retain warm-up and earlier-case data until the repetition ends.

Each per-trial JSON retains the CPU and memory time series with its UTC start, actual elapsed sample times, raw per-broker counters, simultaneous cluster totals, and interval CPU cores used. Throughput and latency remain whole-workload summaries.

No TLS, authentication, tiered storage, compaction, restart, failure injection, or forced per-append fsync is tested. Redpanda write caching is explicitly enabled, without dev-container mode or unsafe-bypass-fsync. Results include shared-host and client bottlenecks; they are not universal performance rankings or production qualification.

## RF1

| Workload | Broker | Records/s | Logical MiB/s | CPU µs/record | p50 ms | p95 ms | p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|
| 1k-random-lz4 | krabka | 514769 (504820–527476) | 502.70 (492.99–515.11) | 1.51 (1.49–1.53) | 34.07 (29.72–34.07) | 226.77 (109.91–268.86) | 429.88 (420.99–645.57) |
| 1k-random-lz4 | kafka | 566743 (545187–573004) | 553.46 (532.41–559.57) | 1.59 (1.59–1.63) | 15.01 (10.80–17.47) | 178.14 (156.63–218.72) | 445.60 (438.71–448.45) |
| 1k-random-lz4 | redpanda | 590830 (588889–617229) | 576.98 (575.09–602.76) | 2.13 (2.11–2.13) | 53.28 (53.10–53.63) | 95.32 (80.39–142.97) | 210.04 (177.18–220.29) |
| 100b-random-lz4 | krabka | 2312423 (2311959–2345251) | 220.53 (220.49–223.66) | 0.29 (0.29–0.30) | 1.82 (1.79–1.96) | 8.83 (6.61–12.30) | 15.97 (14.65–25.95) |
| 100b-random-lz4 | kafka | 2269459 (2266682–2333652) | 216.43 (216.17–222.55) | 0.32 (0.30–0.32) | 1.80 (1.62–1.92) | 9.48 (8.88–9.62) | 20.19 (15.60–20.87) |
| 100b-random-lz4 | redpanda | 2319519 (2259419–2332334) | 221.21 (215.47–222.43) | 0.34 (0.32–0.34) | 1.80 (1.71–1.99) | 9.81 (8.41–18.59) | 23.02 (14.43–59.51) |
| 1k-zeros-lz4 | krabka | 2066454 (2036267–2139620) | 2018.02 (1988.54–2089.47) | 0.28 (0.27–0.28) | 154.23 (133.72–171.06) | 356.83 (285.41–360.40) | 439.23 (353.71–466.78) |
| 1k-zeros-lz4 | kafka | 2104439 (2016424–2177826) | 2055.12 (1969.16–2126.78) | 0.17 (0.16–0.18) | 94.70 (66.98–115.17) | 178.93 (139.88–253.67) | 206.13 (179.68–359.96) |
| 1k-zeros-lz4 | redpanda | 2155392 (2060988–2165655) | 2104.88 (2012.68–2114.90) | 0.13 (0.13–0.14) | 130.94 (97.47–151.30) | 302.77 (197.35–332.18) | 367.61 (237.19–407.85) |
| 1k-random-none | krabka | 556851 (554678–586872) | 543.80 (541.68–573.12) | 1.35 (1.33–1.39) | 35.36 (35.35–35.67) | 99.43 (77.24–237.04) | 617.30 (390.16–809.72) |
| 1k-random-none | kafka | 611127 (579068–636676) | 596.80 (565.50–621.75) | 1.27 (1.27–1.32) | 34.84 (33.95–35.62) | 216.92 (132.70–293.34) | 419.79 (337.46–426.01) |
| 1k-random-none | redpanda | 617635 (549848–666443) | 603.16 (536.96–650.82) | 1.94 (1.93–1.98) | 52.76 (47.34–58.44) | 204.92 (189.56–214.73) | 259.64 (242.37–278.47) |
| 100k-random-lz4 | krabka | 6130 (6098–6365) | 598.64 (595.55–621.63) | 135.86 (132.92–137.95) | 36.78 (35.39–37.25) | 132.33 (64.08–247.37) | 510.93 (331.07–1125.32) |
| 100k-random-lz4 | kafka | 6468 (5852–6472) | 631.67 (571.45–632.00) | 120.86 (119.59–124.42) | 32.30 (32.23–34.64) | 226.99 (213.13–238.88) | 334.82 (278.01–509.44) |
| 100k-random-lz4 | redpanda | 6207 (5307–6802) | 606.15 (518.30–664.24) | 200.25 (199.05–202.25) | 53.41 (48.00–61.83) | 84.01 (69.70–110.31) | 165.25 (143.27–195.99) |
| 1k-random-20k | krabka | 19998 (19997–19998) | 19.53 (19.53–19.53) | 6.99 (6.97–7.01) | 1.84 (1.84–1.85) | 3.20 (3.19–3.20) | 3.47 (3.44–3.48) |
| 1k-random-20k | kafka | 19997 (19997–19998) | 19.53 (19.53–19.53) | 5.36 (5.30–5.40) | 1.83 (1.83–1.84) | 3.16 (3.16–3.20) | 3.43 (3.42–5.28) |
| 1k-random-20k | redpanda | 19998 (19998–19998) | 19.53 (19.53–19.53) | 9.85 (9.75–9.99) | 1.80 (1.80–1.80) | 3.14 (3.14–3.14) | 3.44 (3.38–3.45) |

| Workload | Broker | CPU seconds | Peak RSS MiB | Peak anonymous MiB | Peak working set MiB |
|---|---|---:|---:|---:|---:|
| 1k-random-lz4 | krabka | 15.09 (14.91–15.28) | 51.94 (51.10–53.61) | 26.22 (24.26–26.76) | 331.51 (327.27–356.69) |
| 1k-random-lz4 | kafka | 15.93 (15.91–16.29) | 930.52 (874.49–948.91) | 897.85 (842.40–917.10) | 1203.13 (1135.50–1264.53) |
| 1k-random-lz4 | redpanda | 21.25 (21.15–21.33) | 7337.25 (7329.30–7347.41) | 7273.64 (7266.09–7286.42) | 7293.73 (7287.32–7436.77) |
| 100b-random-lz4 | krabka | 2.93 (2.88–2.97) | 52.75 (51.87–54.84) | 27.04 (25.02–28.00) | 333.47 (330.12–360.02) |
| 100b-random-lz4 | kafka | 3.18 (2.98–3.20) | 1125.53 (1106.52–1165.25) | 1092.58 (1073.58–1132.20) | 1376.34 (1366.02–1498.46) |
| 100b-random-lz4 | redpanda | 3.39 (3.22–3.41) | 7341.78 (7333.77–7351.76) | 7278.10 (7270.32–7290.59) | 7297.40 (7291.05–7441.93) |
| 1k-zeros-lz4 | krabka | 2.81 (2.71–2.84) | 84.98 (84.35–91.23) | 59.27 (57.51–64.39) | 369.50 (363.60–393.44) |
| 1k-zeros-lz4 | kafka | 1.69 (1.63–1.83) | 1109.14 (1108.25–1172.55) | 1076.02 (1075.12–1139.40) | 1370.00 (1362.48–1508.81) |
| 1k-zeros-lz4 | redpanda | 1.30 (1.28–1.36) | 7341.95 (7334.14–7353.04) | 7278.27 (7270.63–7291.83) | 7298.23 (7292.30–7443.04) |
| 1k-random-none | krabka | 6.74 (6.64–6.96) | 86.70 (84.44–91.29) | 60.95 (57.57–64.42) | 380.26 (359.80–392.20) |
| 1k-random-none | kafka | 6.36 (6.36–6.61) | 1126.05 (1125.51–1174.13) | 1092.05 (1091.39–1139.91) | 1398.60 (1392.75–1524.50) |
| 1k-random-none | redpanda | 9.71 (9.67–9.88) | 7364.68 (7356.26–7376.49) | 7300.78 (7292.54–7314.74) | 7320.71 (7313.47–7467.45) |
| 100k-random-lz4 | krabka | 8.15 (7.97–8.28) | 87.25 (86.57–91.33) | 61.50 (59.70–64.46) | 393.79 (390.13–418.95) |
| 100k-random-lz4 | kafka | 7.25 (7.18–7.47) | 1180.84 (1177.84–1184.46) | 1146.26 (1143.34–1149.77) | 1470.16 (1466.32–1549.77) |
| 100k-random-lz4 | redpanda | 12.02 (11.94–12.14) | 7367.77 (7361.07–7380.39) | 7303.70 (7297.35–7318.64) | 7325.54 (7320.37–7473.79) |
| 1k-random-20k | krabka | 4.19 (4.18–4.21) | 87.29 (86.58–91.36) | 61.55 (59.71–64.48) | 394.26 (389.88–420.24) |
| 1k-random-20k | kafka | 3.22 (3.18–3.24) | 1182.45 (1179.73–1186.38) | 1147.31 (1144.57–1151.13) | 1473.94 (1473.34–1554.63) |
| 1k-random-20k | redpanda | 5.91 (5.85–5.99) | 7368.15 (7361.25–7380.59) | 7303.72 (7297.35–7318.66) | 7324.60 (7319.43–7472.14) |

## RF3

| Workload | Broker | Records/s | Logical MiB/s | CPU µs/record | p50 ms | p95 ms | p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|
| 1k-random-lz4 | krabka | 181538 (167150–231487) | 177.28 (163.23–226.06) | 9.47 (9.16–9.54) | 44.95 (44.55–45.59) | 1141.68 (693.76–1213.74) | 2422.08 (1928.26–2726.62) |
| 1k-random-lz4 | kafka | 212666 (162277–253222) | 207.68 (158.47–247.29) | 9.61 (9.49–9.67) | 44.48 (41.29–45.89) | 770.57 (725.11–1130.15) | 1646.89 (1276.64–2300.83) |
| 1k-random-lz4 | redpanda | 172336 (171527–179472) | 168.30 (167.51–175.27) | 10.70 (10.66–10.77) | 159.02 (158.50–170.81) | 410.73 (367.81–456.82) | 777.91 (712.79–873.89) |
| 100b-random-lz4 | krabka | 2176178 (2068456–2188624) | 207.54 (197.26–208.72) | 1.26 (1.22–1.30) | 2.77 (2.73–3.29) | 15.38 (12.02–16.21) | 23.61 (22.91–72.23) |
| 100b-random-lz4 | kafka | 2156178 (2121943–2212288) | 205.63 (202.36–210.98) | 1.19 (1.17–1.21) | 3.05 (2.80–3.34) | 12.46 (10.97–44.43) | 20.48 (18.95–105.12) |
| 100b-random-lz4 | redpanda | 1230050 (1220459–1280165) | 117.31 (116.39–122.09) | 0.97 (0.95–0.99) | 158.62 (115.85–195.25) | 569.15 (498.53–791.77) | 932.43 (601.49–1354.96) |
| 1k-zeros-lz4 | krabka | 2060888 (1966632–2089231) | 2012.59 (1920.54–2040.27) | 0.79 (0.78–0.79) | 182.34 (162.23–259.38) | 291.97 (275.44–468.31) | 329.69 (306.12–537.51) |
| 1k-zeros-lz4 | kafka | 2150444 (2114816–2160318) | 2100.04 (2065.25–2109.69) | 0.61 (0.59–0.71) | 123.33 (85.89–147.29) | 207.80 (134.66–242.63) | 229.76 (154.63–278.82) |
| 1k-zeros-lz4 | redpanda | 2072164 (1943929–2168603) | 2023.60 (1898.37–2117.78) | 0.45 (0.42–0.47) | 112.03 (91.76–116.06) | 225.62 (192.65–303.06) | 293.20 (222.11–485.55) |
| 1k-random-none | krabka | 235774 (215044–238174) | 230.25 (210.00–232.59) | 9.66 (9.58–9.91) | 50.49 (50.36–50.85) | 815.39 (706.65–942.46) | 2160.76 (1872.13–2340.73) |
| 1k-random-none | kafka | 267502 (175923–312757) | 261.23 (171.80–305.43) | 7.27 (7.14–7.89) | 45.18 (44.56–45.88) | 613.05 (559.48–1101.77) | 1091.04 (1000.03–2439.94) |
| 1k-random-none | redpanda | 153808 (144515–155608) | 150.20 (141.13–151.96) | 9.64 (9.06–9.98) | 196.65 (179.24–200.02) | 558.80 (411.15–595.06) | 832.56 (592.07–967.88) |
| 100k-random-lz4 | krabka | 2101 (2073–2583) | 205.14 (202.40–252.28) | 826.35 (784.15–827.11) | 47.34 (47.08–47.70) | 671.98 (538.17–792.85) | 2589.18 (1925.04–3106.97) |
| 100k-random-lz4 | kafka | 2848 (2223–2887) | 278.13 (217.13–281.94) | 628.09 (614.10–635.42) | 46.08 (45.79–46.14) | 659.43 (566.33–707.02) | 1408.40 (1020.54–2268.31) |
| 100k-random-lz4 | redpanda | 1749 (1457–1795) | 170.82 (142.29–175.26) | 994.24 (983.90–1002.95) | 170.29 (170.22–216.00) | 390.38 (384.89–499.54) | 763.11 (711.05–771.42) |
| 1k-random-20k | krabka | 19997 (19997–19997) | 19.53 (19.53–19.53) | 20.35 (20.13–20.46) | 2.22 (2.22–2.23) | 3.55 (3.55–3.56) | 3.84 (3.83–3.85) |
| 1k-random-20k | kafka | 19997 (19997–19998) | 19.53 (19.53–19.53) | 18.44 (18.39–19.66) | 2.15 (2.15–2.16) | 3.48 (3.48–3.49) | 3.83 (3.80–3.84) |
| 1k-random-20k | redpanda | 19998 (19998–19998) | 19.53 (19.53–19.53) | 17.93 (17.66–18.18) | 2.01 (1.99–2.01) | 3.35 (3.33–3.37) | 4.75 (4.54–32.47) |

| Workload | Broker | CPU seconds | Peak RSS MiB | Peak anonymous MiB | Peak working set MiB |
|---|---|---:|---:|---:|---:|
| 1k-random-lz4 | krabka | 94.74 (91.59–95.43) | 150.30 (145.85–150.33) | 70.00 (67.95–70.78) | 831.36 (668.21–853.61) |
| 1k-random-lz4 | kafka | 96.13 (94.86–96.73) | 2909.85 (2883.93–2927.68) | 2814.13 (2790.56–2837.47) | 3555.41 (3458.54–3610.58) |
| 1k-random-lz4 | redpanda | 107.01 (106.60–107.66) | 22003.54 (21996.29–22012.11) | 21815.47 (21807.53–21824.74) | 22005.43 (21910.15–22014.49) |
| 100b-random-lz4 | krabka | 12.64 (12.17–13.02) | 151.75 (148.70–152.21) | 70.90 (70.48–72.35) | 828.93 (692.36–849.09) |
| 100b-random-lz4 | kafka | 11.93 (11.75–12.09) | 2922.84 (2901.62–2937.15) | 2825.68 (2804.46–2839.55) | 3595.12 (3506.16–3639.85) |
| 100b-random-lz4 | redpanda | 9.73 (9.49–9.85) | 22018.87 (22007.23–22029.59) | 21830.39 (21818.34–21841.40) | 22017.43 (21934.29–22040.34) |
| 1k-zeros-lz4 | krabka | 7.87 (7.83–7.93) | 236.42 (224.84–250.23) | 155.34 (146.63–170.17) | 916.67 (773.09–922.20) |
| 1k-zeros-lz4 | kafka | 6.06 (5.88–7.10) | 2921.92 (2918.97–2974.89) | 2824.35 (2821.25–2876.99) | 3622.33 (3547.71–3644.71) |
| 1k-zeros-lz4 | redpanda | 4.49 (4.22–4.71) | 22021.21 (22011.17–22030.82) | 21832.61 (21822.10–21842.57) | 22018.84 (21926.45–22031.74) |
| 1k-random-none | krabka | 48.30 (47.91–49.53) | 239.79 (227.14–252.89) | 158.60 (148.93–172.84) | 954.31 (813.14–960.01) |
| 1k-random-none | kafka | 36.35 (35.72–39.45) | 2947.52 (2935.73–2964.85) | 2849.56 (2837.08–2865.86) | 3687.40 (3559.69–3698.96) |
| 1k-random-none | redpanda | 48.22 (45.29–49.89) | 22161.25 (22127.63–22167.08) | 21972.66 (21936.80–21976.38) | 22137.64 (22061.29–22177.97) |
| 100k-random-lz4 | krabka | 49.58 (47.05–49.63) | 244.13 (235.88–255.32) | 162.94 (157.67–175.27) | 969.50 (806.54–980.13) |
| 100k-random-lz4 | kafka | 37.69 (36.85–38.13) | 2958.41 (2948.66–2982.31) | 2856.88 (2844.92–2878.67) | 3742.05 (3601.88–3742.86) |
| 100k-random-lz4 | redpanda | 59.65 (59.03–60.18) | 22207.63 (22193.84–22209.59) | 22015.85 (22004.56–22017.72) | 22219.29 (22111.32–22227.98) |
| 1k-random-20k | krabka | 12.21 (12.08–12.28) | 243.27 (236.20–255.37) | 162.08 (157.75–175.32) | 971.43 (726.18–1004.21) |
| 1k-random-20k | kafka | 11.06 (11.03–11.80) | 2977.43 (2968.17–2981.81) | 2873.27 (2864.02–2877.23) | 3683.87 (3592.48–3738.14) |
| 1k-random-20k | redpanda | 10.76 (10.60–10.91) | 22207.97 (22194.28–22210.25) | 22015.87 (22004.62–22017.79) | 22217.09 (22092.05–22221.46) |
