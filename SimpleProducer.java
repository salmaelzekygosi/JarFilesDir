import org.apache.kafka.clients.producer.*;
import java.io.FileInputStream;
import java.util.*;

/**
 * Simple Kafka Producer — SASL_SSL + OAuthBearer + JSON Schema
 *
 * Uses a Transaction POJO so the serializer auto-registers
 * a proper JSON Schema on first publish.
 *
 * Compile:
 * javac -cp "libs/*" Transaction.java SimpleProducer.java
 *
 * Run:
 * java -cp ".;libs/*" SimpleProducer client.properties
 * gosi.payments.pub.transaction-created.v1
 */
public class SimpleProducer {

    private static final String[] ACCOUNTS = { "ACC-1001", "ACC-1002", "ACC-1003", "ACC-1004", "ACC-1005" };
    private static final String[] MERCHANTS = { "Amazon", "Jarir Bookstore", "Carrefour", "STC Pay", "Noon" };

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java -cp \".;libs/*\" SimpleProducer <properties-file> <topic>");
            System.out.println(
                    "Example: java -cp \".;libs/*\" SimpleProducer client.properties gosi.payments.pub.transaction-created.v1");
            System.exit(1);
        }

        String propsFile = args[0];
        String topic = args[1];

        // Load properties
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propsFile)) {
            props.load(fis);
        }
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
        props.put("schema.registry.url", "https://sr-test.apps.appsqaocp.gosi.ins");

        // Auto-register schema on first publish — no manual intervention needed
        props.put("auto.register.schemas", "true");
        props.put("json.fail.invalid.schema", "true");
        props.put("json.oneof.for.nullables", "false");

        // Schema Registry TLS (reuse broker truststore)
        props.put("schema.registry.ssl.truststore.location",
                props.getProperty("ssl.truststore.location", "truststore.jks"));
        props.put("schema.registry.ssl.truststore.password",
                props.getProperty("ssl.truststore.password", "mystorepassword"));
        props.put("schema.registry.ssl.endpoint.identification.algorithm", "");

        System.out.println("==========================================================");
        System.out.println("  Simple Kafka Producer — JSON Schema (Auto-Register)");
        System.out.println("  Topic     : " + topic);
        System.out.println("  Bootstrap : " + props.getProperty("bootstrap.servers"));
        System.out.println("  Security  : " + props.getProperty("security.protocol"));
        System.out.println("  Schema Reg: " + props.getProperty("schema.registry.url"));
        System.out.println("==========================================================");

        Random rnd = new Random();
        int count = 0;

        // Use Transaction POJO — serializer introspects it to generate the JSON Schema
        try (KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props)) {
            while (true) {
                String key = ACCOUNTS[rnd.nextInt(ACCOUNTS.length)];
                Transaction txn = buildTransaction(key, rnd);
                count++;

                producer.send(new ProducerRecord<>(topic, key, txn), (meta, ex) -> {
                    if (ex != null) {
                        System.err.println("FAIL: " + ex.getMessage());
                    } else {
                        System.out.printf(">> Sent  partition=%d  offset=%d  key=%s%n",
                                meta.partition(), meta.offset(), key);
                    }
                });

                if (count % 10 == 0)
                    System.out.println("-- Total sent: " + count + " --");
                Thread.sleep(3000);
            }
        }
    }

    private static Transaction buildTransaction(String accountId, Random rnd) {
        double amount = Math.round((100 + rnd.nextDouble() * 9900) * 100.0) / 100.0;
        String txnId = "TXN-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();
        int idx = rnd.nextInt(MERCHANTS.length);

        return new Transaction(
                txnId,
                accountId,
                amount,
                Transaction.Currency.values()[rnd.nextInt(Transaction.Currency.values().length)],
                MERCHANTS[idx],
                Transaction.Country.values()[rnd.nextInt(Transaction.Country.values().length)],
                System.currentTimeMillis());
    }
}
