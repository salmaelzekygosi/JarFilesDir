package com.gosi.kafka;

import javax.enterprise.context.ApplicationScoped;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import com.gosi.kafka.sdk.config.SerializationFormat;
import com.gosi.kafka.sdk.producer.GosiKafkaProducer;
import com.gosi.kafka.sdk.telemetry.Slf4jTelemetryReporter;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gosi.kafka.avro.PaymentRecord; // Generated Avro Class
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class PaymentProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);

    private GosiKafkaProducer<String, PaymentRecord> gosiKafkaProducer;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;
    
    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.schema.registry.url")
    String schemaRegistryUrl;

    @ConfigProperty(name = "app.kafka.topic")
    String topicName;

    @PostConstruct
    void init() {
        Map<String, Object> additionalKafkaProps = new HashMap<>();
        Config configProvider = ConfigProvider.getConfig();
        for (String propertyName : configProvider.getPropertyNames()) {
            if (propertyName.startsWith("kafka.ssl.") || 
                propertyName.startsWith("kafka.sasl.") || 
                propertyName.startsWith("kafka.security.") ||
                propertyName.startsWith("kafka.basic.") ||
                propertyName.startsWith("kafka.schema.registry.")) {
                configProvider.getOptionalValue(propertyName, String.class)
                        .ifPresent(val -> additionalKafkaProps.put(propertyName.substring(6), val));
            }
        }

        GosiKafkaClientConfig config = GosiKafkaClientConfig.builder()
                .bootstrapServers(bootstrapServers)
                .schemaRegistryUrl(schemaRegistryUrl)
                .keyFormat(SerializationFormat.STRING)
                .valueFormat(SerializationFormat.AVRO)
                .additionalProperties(additionalKafkaProps)
                .build();

        gosiKafkaProducer = new GosiKafkaProducer<>(config, new Slf4jTelemetryReporter());
    }

    @PreDestroy
    void cleanup() {
        if (gosiKafkaProducer != null) {
            gosiKafkaProducer.close();
        }
    }

    public void publishPayment(Payment payment) {
        try {
            // Create the generated Avro record
            PaymentRecord avroRecord = PaymentRecord.newBuilder()
                    .setId(payment.getId())
                    .setAmount(payment.getAmount())
                    .setCurrency(payment.getCurrency())
                    .setTraceId(payment.getTraceId() != null ? payment.getTraceId() : "")
                    .build();

            // Emit the Avro record through the GOSI SDK
            gosiKafkaProducer.sendAsync(topicName, payment.getId(), avroRecord);
            log.info("Quarkus: Produced Avro payment request initiated for: {}", payment.getId());
        } catch (Exception e) {
            log.error("Failed to produce Avro payment message: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to produce Avro payment message", e);
        }
    }
}
