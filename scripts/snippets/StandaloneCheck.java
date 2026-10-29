import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

// Run against the extracted broker overview command, not the Compose broker.
public class StandaloneCheck {
  static Properties settings() {
    var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("request.timeout.ms", "10000");
    p.put("default.api.timeout.ms", "30000");
    return p;
  }

  static KafkaProducer<String, String> producer(boolean transactional) {
    var p = settings();
    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    p.put("max.block.ms", "30000");
    p.put("delivery.timeout.ms", "30000");
    if (transactional) {
      p.put("transactional.id", "standalone-transactions");
      p.put("transaction.timeout.ms", "30000");
    }
    return new KafkaProducer<>(p);
  }

  static KafkaConsumer<String, String> consumer(String group, String isolation) {
    var p = settings();
    p.put("group.id", group);
    p.put("enable.auto.commit", "false");
    p.put("auto.offset.reset", "earliest");
    p.put("isolation.level", isolation);
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    return new KafkaConsumer<>(p);
  }

  static List<String> read(KafkaConsumer<String, String> c, int count) {
    var values = new ArrayList<String>();
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (values.size() < count && System.nanoTime() < deadline) {
      for (var record : c.poll(Duration.ofSeconds(1))) values.add(record.value());
    }
    return values;
  }

  static void equal(Object actual, Object expected) {
    if (!expected.equals(actual)) throw new AssertionError(actual + " != " + expected);
  }

  public static void main(String[] args) throws Exception {
    try (var admin = Admin.create(settings())) {
      admin.createTopics(List.of(
          new NewTopic("standalone-roundtrip", 1, (short) 1),
          new NewTopic("standalone-transactions", 1, (short) 1)))
          .all().get(30, TimeUnit.SECONDS);
    }
    try (var p = producer(false)) {
      p.send(new ProducerRecord<>("standalone-roundtrip", "hello standalone"))
          .get(30, TimeUnit.SECONDS);
    }
    var partition = new TopicPartition("standalone-roundtrip", 0);
    try (var c = consumer("standalone-offsets", "read_uncommitted")) {
      c.assign(List.of(partition));
      equal(read(c, 1), List.of("hello standalone"));
      c.commitSync(Map.of(partition, new OffsetAndMetadata(1)));
    }
    try (var c = consumer("standalone-offsets", "read_uncommitted")) {
      equal(c.committed(Set.of(partition)).get(partition).offset(), 1L);
      c.assign(List.of(partition));
      equal(c.position(partition), 1L);
      equal(c.poll(Duration.ofSeconds(2)).count(), 0);
    }
    System.out.println("PASS: standalone round trip and committed offset resume");

    try (var p = producer(true)) {
      p.initTransactions();
      p.beginTransaction();
      p.send(new ProducerRecord<>("standalone-transactions", "committed"))
          .get(30, TimeUnit.SECONDS);
      p.commitTransaction();
      p.beginTransaction();
      p.send(new ProducerRecord<>("standalone-transactions", "aborted"))
          .get(30, TimeUnit.SECONDS);
      p.abortTransaction();
      p.beginTransaction();
      p.send(new ProducerRecord<>("standalone-transactions", "after-abort"))
          .get(30, TimeUnit.SECONDS);
      p.commitTransaction();
    }
    var transactionalPartition = new TopicPartition("standalone-transactions", 0);
    try (var c = consumer("standalone-raw", "read_uncommitted")) {
      c.assign(List.of(transactionalPartition));
      equal(read(c, 3), List.of("committed", "aborted", "after-abort"));
    }
    try (var c = consumer("standalone-isolated", "read_committed")) {
      c.assign(List.of(transactionalPartition));
      equal(read(c, 2), List.of("committed", "after-abort"));
      equal(c.poll(Duration.ofSeconds(2)).count(), 0);
    }
    System.out.println("PASS: standalone transaction commit, abort, and read_committed isolation");
  }
}
