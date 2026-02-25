import org.apache.kafka.clients.producer.*;
import java.io.FileInputStream;
import java.util.*;

/**
 * "Rogue" Producer — sends invalid JSON without Schema Registry.
 * Use this to demo that broker-side schema validation blocks non-conforming
 * data.
 *
 * Compile:
 * javac -cp "libs/*" BadProducer.java
 *
 * Run:
 * java -cp ".;libs/*" BadProducer client.properties
 * gosi.payments.pub.transaction-created.v1
 */
public class BadProducer {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java -cp \".;libs/*\" BadProducer <properties-file> <topic>");
            System.exit(1);
        }

        String propsFile = args[0];
        String topic = args[1];

        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propsFile)) {
            props.load(fis);
        }
        // Plain StringSerializer — NO Schema Registry, NO schema validation
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        System.out.println("==========================================================");
        System.out.println("  BAD Producer — No Schema Registry (Rogue Demo)");
        System.out.println("  Topic     : " + topic);
        System.out.println("==========================================================");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Test 1: Missing required fields (only transactionId, no amount/currency/etc)
            System.out.println("\n[TEST 1] Sending JSON with missing required fields...");
            String bad1 = "{\"transactionId\":\"TXN-BAD-001\",\"accountId\":\"ACC-TEST\"}";
            producer.send(new ProducerRecord<>(topic, "ACC-TEST", bad1), (meta, ex) -> {
                if (ex != null) {
                    System.err.println("  BLOCKED: " + ex.getMessage());
                } else {
                    System.out.printf(
                            "  ACCEPTED (partition=%d, offset=%d) <-- should NOT happen if validation is ON%n",
                            meta.partition(), meta.offset());
                }
            });
            producer.flush();

            // Test 2: Completely wrong structure
            System.out.println("\n[TEST 2] Sending garbage data...");
            String bad2 = "this is not even JSON";
            producer.send(new ProducerRecord<>(topic, "ACC-TEST", bad2), (meta, ex) -> {
                if (ex != null) {
                    System.err.println("  BLOCKED: " + ex.getMessage());
                } else {
                    System.out.printf(
                            "  ACCEPTED (partition=%d, offset=%d) <-- should NOT happen if validation is ON%n",
                            meta.partition(), meta.offset());
                }
            });
            producer.flush();

            // Test 3: Wrong types (amount as string instead of number)
            System.out.println("\n[TEST 3] Sending JSON with wrong types...");
            String bad3 = "{\"transactionId\":\"TXN-BAD-003\",\"accountId\":\"ACC-TEST\",\"amount\":\"not-a-number\",\"currency\":\"SAR\",\"merchant\":\"Test\",\"country\":\"SA\",\"timestamp\":123}";
            producer.send(new ProducerRecord<>(topic, "ACC-TEST", bad3), (meta, ex) -> {
                if (ex != null) {
                    System.err.println("  BLOCKED: " + ex.getMessage());
                } else {
                    System.out.printf(
                            "  ACCEPTED (partition=%d, offset=%d) <-- should NOT happen if validation is ON%n",
                            meta.partition(), meta.offset());
                }
            });
            producer.flush();

            System.out.println("\n==========================================================");
            System.out.println("  If all 3 tests show ACCEPTED, enable broker validation:");
            System.out.println("  confluent.value.schema.validation=true on the topic");
            System.out.println("==========================================================");
        }
    }
}
