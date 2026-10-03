// Copyright 2026 The Krabka Authors. SPDX-License-Identifier: Apache-2.0
// Bounded, duration-based workload using the Kafka image's client and HDR jars.
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class BenchmarkTimeline {
    static final class Counters {
        final AtomicLong submitted = new AtomicLong();
        final AtomicLong acknowledged = new AtomicLong();
        final AtomicLong consumed = new AtomicLong();
        final AtomicLong errors = new AtomicLong();
        final Recorder ackIntervals = new Recorder(3);
        final Recorder latencyIntervals = new Recorder(3);
        final Recorder measuredAck = new Recorder(3);
        final Histogram measuredLatency = new Histogram(3);
    }

    static String quantile(Histogram histogram, double percentile) {
        return histogram.getTotalCount() == 0 ? "null"
                : String.format(Locale.ROOT, "%.3f", histogram.getValueAtPercentile(percentile) / 1000.0);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--self-test")) {
            Histogram histogram = new Histogram(3);
            if (!quantile(histogram, 99).equals("null")) throw new AssertionError();
            for (int i = 0; i < 99; i++) histogram.recordValue(1000);
            histogram.recordValue(100000);
            if (Double.parseDouble(quantile(histogram, 99)) > 1.01) throw new AssertionError();
            System.out.println("HDR quantile and empty interval checks passed");
            return;
        }
        String bootstrap = args[0], topic = args[1];
        int rate = Integer.parseInt(args[2]);
        int warmupSeconds = Integer.parseInt(args[3]), seconds = Integer.parseInt(args[4]);
        Path output = Path.of(args[5]);
        int maxRecords = Integer.parseInt(args[6]);
        if (rate == 0 || rate < -1 || warmupSeconds < 1 || seconds < 1 || maxRecords < 1) {
            throw new IllegalArgumentException("invalid timeline bounds");
        }
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", bootstrap);
        producerConfig.put("acks", "all");
        producerConfig.put("enable.idempotence", "true");
        producerConfig.put("compression.type", "lz4");
        producerConfig.put("batch.size", "65536");
        producerConfig.put("linger.ms", "5");
        producerConfig.put("request.timeout.ms", "10000");
        producerConfig.put("delivery.timeout.ms", "45000");
        producerConfig.put("max.block.ms", "30000");
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", bootstrap);
        consumerConfig.put("enable.auto.commit", "false");
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put("default.api.timeout.ms", "10000");
        byte[] randomPool = new byte[4 * 1024 * 1024];
        new Random(42).nextBytes(randomPool);
        Counters counts = new Counters();
        BitSet seen = new BitSet();
        AtomicBoolean reporting = new AtomicBoolean(true);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig,
                    new ByteArrayDeserializer(), new ByteArrayDeserializer());
                KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig,
                    new ByteArraySerializer(), new ByteArraySerializer());
                PrintWriter timeline = new PrintWriter(Files.newBufferedWriter(output.resolve("workload.jsonl")))) {
            ArrayList<TopicPartition> partitions = new ArrayList<>();
            for (int i = 0; i < 12; i++) partitions.add(new TopicPartition(topic, i));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            for (TopicPartition partition : partitions) consumer.position(partition);
            long start = System.nanoTime();
            long warmupEnd = start + warmupSeconds * 1_000_000_000L;
            long end = warmupEnd + seconds * 1_000_000_000L;
            String startedAt = Instant.now().toString();
            Files.writeString(output.resolve("started.tmp"), "{\"started_at\":\"" + startedAt + "\"}");
            Files.move(output.resolve("started.tmp"), output.resolve("started.json"), StandardCopyOption.ATOMIC_MOVE);
            var operatingSystem = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long cpuStart = operatingSystem.getProcessCpuTime();
            Thread reporter = new Thread(() -> {
                long previous = start, previousAck = 0, previousConsumed = 0;
                long previousCpu = cpuStart;
                while (reporting.get()) {
                    LockSupport.parkNanos(1_000_000_000L);
                    long now = System.nanoTime();
                    long ack = counts.acknowledged.get(), consumed = counts.consumed.get();
                    double interval = (now - previous) / 1e9;
                    double elapsed = (now - start) / 1e6;
                    long offered = rate > 0 ? (long) (Math.min(now - start, end - start) / 1e9 * rate)
                            : counts.submitted.get();
                    String phase = previous < warmupEnd ? "warmup" : previous < end ? "measure" : "drain";
                    Histogram ackHistogram = counts.ackIntervals.getIntervalHistogram();
                    Histogram latencyHistogram = counts.latencyIntervals.getIntervalHistogram();
                    long cpu = operatingSystem.getProcessCpuTime();
                    timeline.printf(Locale.ROOT,
                            "{\"elapsed_ms\":%.6f,\"interval_ms\":%.6f,\"phase\":\"%s\","
                            + "\"offered_records_per_second\":%d,\"offered_records\":%d,"
                            + "\"submitted\":%d,\"acknowledged\":%d,\"consumed\":%d,\"errors\":%d,"
                            + "\"ack_records_per_second\":%.6f,\"consume_records_per_second\":%.6f,"
                            + "\"consumer_lag_records\":%d,\"offered_backlog_records\":%d,"
                            + "\"ack_latency_ms_p99\":%s,\"latency_ms_p99\":%s,\"latency_samples\":%d,"
                            + "\"client_cpu_seconds\":%.6f,\"client_cpu_cores\":%.6f,\"client_heap_used_bytes\":%d}%n",
                            elapsed, interval * 1000, phase, rate, offered, counts.submitted.get(), ack, consumed,
                            counts.errors.get(), (ack - previousAck) / interval, (consumed - previousConsumed) / interval,
                            Math.max(0, ack - consumed), Math.max(0, offered - consumed),
                            quantile(ackHistogram, 99), quantile(latencyHistogram, 99), latencyHistogram.getTotalCount(),
                            (cpu - cpuStart) / 1e9, (cpu - previousCpu) / 1e9 / interval,
                            ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
                    timeline.flush();
                    previous = now;
                    previousAck = ack;
                    previousConsumed = consumed;
                    previousCpu = cpu;
                }
            }, "benchmark-telemetry");
            reporter.setDaemon(true);
            reporter.start();
            CompletableFuture<Void> producing = CompletableFuture.runAsync(() -> {
                try {
                    for (int sequence = 0; System.nanoTime() < end; sequence++) {
                        if (sequence >= maxRecords) throw new IllegalStateException("record/disk ceiling reached");
                        long scheduled = rate > 0 ? start + (long) (sequence * (1e9 / rate)) : System.nanoTime();
                        if (scheduled >= end) break;
                        while (System.nanoTime() < scheduled) {
                            LockSupport.parkNanos(scheduled - System.nanoTime());
                        }
                        byte[] value = new byte[1024];
                        System.arraycopy(randomPool, sequence % (randomPool.length / 1024) * 1024, value, 0, 1024);
                        ByteBuffer.wrap(value).putLong(scheduled).putLong(sequence);
                        counts.submitted.incrementAndGet();
                        producer.send(new ProducerRecord<>(topic, value), (metadata, error) -> {
                            if (error != null) {
                                if (counts.errors.incrementAndGet() <= 10) error.printStackTrace(System.err);
                            }
                            else {
                                long latencyUs = Math.max(0, (System.nanoTime() - scheduled) / 1000);
                                counts.ackIntervals.recordValue(latencyUs);
                                if (scheduled >= warmupEnd) counts.measuredAck.recordValue(latencyUs);
                                counts.acknowledged.incrementAndGet();
                            }
                        });
                    }
                    producer.flush();
                } catch (Exception error) {
                    throw new RuntimeException(error);
                }
            });
            int duplicates = 0;
            long deadline = end + 60_000_000_000L;
            try {
                while (System.nanoTime() < deadline) {
                    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(100))) {
                        ByteBuffer value = ByteBuffer.wrap(record.value());
                        long scheduled = value.getLong(), sequence = value.getLong();
                        if (sequence < 0 || sequence >= maxRecords) throw new IllegalStateException("invalid sequence");
                        if (seen.get((int) sequence)) duplicates++;
                        else {
                            seen.set((int) sequence);
                            long latencyUs = Math.max(0, (System.nanoTime() - scheduled) / 1000);
                            counts.latencyIntervals.recordValue(latencyUs);
                            if (scheduled >= warmupEnd) counts.measuredLatency.recordValue(latencyUs);
                            counts.consumed.incrementAndGet();
                        }
                    }
                    if (producing.isDone() && counts.consumed.get() >= counts.acknowledged.get()) {
                        producing.join();
                        break;
                    }
                }
                if (!producing.isDone() || counts.errors.get() != 0 || duplicates != 0
                        || counts.submitted.get() != counts.acknowledged.get()
                        || counts.consumed.get() != counts.acknowledged.get()) {
                    throw new IllegalStateException("timeline delivery failed: submitted=" + counts.submitted.get()
                            + ", acknowledged=" + counts.acknowledged.get() + ", consumed=" + counts.consumed.get()
                            + ", errors=" + counts.errors.get() + ", duplicates=" + duplicates
                            + ", producer_done=" + producing.isDone());
                }
            } finally {
                reporting.set(false);
                LockSupport.unpark(reporter);
                reporter.join(5000);
                if (timeline.checkError()) throw new IllegalStateException("telemetry write failed");
            }
            Histogram ack = counts.measuredAck.getIntervalHistogram();
            System.out.printf(Locale.ROOT,
                    "{\"sent\":%d,\"consumed\":%d,\"duplicates\":%d,\"errors\":%d,"
                    + "\"seconds\":%.6f,\"latency_ms_p50\":%s,\"latency_ms_p95\":%s,"
                    + "\"latency_ms_p99\":%s,\"ack_latency_ms_p99\":%s,\"measured_latency_samples\":%d}%n",
                    counts.acknowledged.get(), counts.consumed.get(), duplicates, counts.errors.get(),
                    (System.nanoTime() - start) / 1e9, quantile(counts.measuredLatency, 50),
                    quantile(counts.measuredLatency, 95), quantile(counts.measuredLatency, 99),
                    quantile(ack, 99), counts.measuredLatency.getTotalCount());
        }
    }
}
