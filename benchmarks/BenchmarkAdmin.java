// Copyright 2026 The Krabka Authors. SPDX-License-Identifier: Apache-2.0
// Small Kafka 4.3.1 admin client used for every vendor, outside broker cgroups.
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.RetriableException;

public final class BenchmarkAdmin {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", args[1]);
        properties.put("request.timeout.ms", "5000");
        properties.put("default.api.timeout.ms", "10000");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        try (Admin admin = Admin.create(properties)) {
            if (args[0].equals("ready")) {
                int brokers = Integer.parseInt(args[2]);
                Exception last = null;
                while (System.nanoTime() < deadline) {
                    try {
                        if (admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size() == brokers) {
                            System.out.println("{\"ready\":true,\"brokers\":" + brokers + "}");
                            return;
                        }
                    } catch (Exception error) {
                        last = error;
                    }
                    Thread.sleep(500);
                }
                throw new IllegalStateException("cluster readiness deadline exceeded", last);
            }
            String topic = args[2];
            int replicas = Integer.parseInt(args[3]);
            int minIsr = Integer.parseInt(args[4]);
            Map<String, String> configs = new TreeMap<>();
            // Redpanda uses a Raft majority; it accepts Kafka's min ISR key
            // but does not expose it as an enforced topic configuration.
            if (!args[5].equals("redpanda")) {
                configs.put("min.insync.replicas", Integer.toString(minIsr));
            }
            configs.put("retention.ms", "-1");
            configs.put("retention.bytes", "-1");
            if (args[5].equals("redpanda")) {
                configs.put("write.caching", "true");
            }
            admin.createTopics(List.of(new NewTopic(topic, 12, (short) replicas).configs(configs)))
                    .all().get(15, TimeUnit.SECONDS);
            TopicDescription description = null;
            while (System.nanoTime() < deadline) {
                try {
                    description = admin.describeTopics(List.of(topic)).allTopicNames()
                            .get(10, TimeUnit.SECONDS).get(topic);
                } catch (ExecutionException error) {
                    // CreateTopics can return before every broker has replayed
                    // metadata. Poll transient responses until the same deadline.
                    if (!(error.getCause() instanceof RetriableException)) throw error;
                    Thread.sleep(500);
                    continue;
                }
                if (description.partitions().size() == 12 && description.partitions().stream()
                        .allMatch(p -> p.leader() != null && p.leader().id() >= 0
                                && p.replicas().size() == replicas && p.isr().size() == replicas)) {
                    break;
                }
                Thread.sleep(500);
            }
            if (description == null || description.partitions().size() != 12
                    || description.partitions().stream().anyMatch(p -> p.leader() == null
                            || p.leader().id() < 0 || p.replicas().size() != replicas
                            || p.isr().size() != replicas)) {
                throw new IllegalStateException("topic did not reach full ISR: " + description);
            }
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Collection<ConfigEntry> actual = admin.describeConfigs(List.of(resource)).all()
                    .get(10, TimeUnit.SECONDS).get(resource).entries();
            for (Map.Entry<String, String> expected : configs.entrySet()) {
                if (actual.stream().noneMatch(c -> c.name().equals(expected.getKey())
                        && expected.getValue().equals(c.value()))) {
                    throw new IllegalStateException("topic config differs: " + expected);
                }
            }
            // Topic names and broker config strings contain no user-provided secrets.
            System.out.println("{\"topic\":\"" + topic + "\",\"description\":\""
                    + escape(description.toString()) + "\",\"configs\":\""
                    + escape(actual.toString()) + "\"}");
        }
    }

    private static String escape(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
