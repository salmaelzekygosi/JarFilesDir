package com.gosi.kafka;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import com.gosi.kafka.sdk.config.SerializationFormat;
import com.gosi.kafka.sdk.consumer.GosiKafkaConsumer;
import com.gosi.kafka.sdk.telemetry.Slf4jTelemetryReporter;
import com.gosi.kafka.avro.PaymentRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@ApplicationScoped
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private GosiKafkaConsumer<String, PaymentRecord> consumer;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;
    
    @ConfigProperty(name = "mp.messaging.incoming.payments-in.group.id")
    String groupId;
    
    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.schema.registry.url")
    String schemaRegistryUrl;

    @ConfigProperty(name = "mp.messaging.incoming.payments-in.topic")
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
                .groupId(groupId)
                .schemaRegistryUrl(schemaRegistryUrl)
                .keyFormat(SerializationFormat.STRING)
                .valueFormat(SerializationFormat.AVRO)
                .additionalProperties(additionalKafkaProps)
                .build();

        consumer = new GosiKafkaConsumer<>(config, new Slf4jTelemetryReporter());
        
        consumer.topic(topicName).handler(record -> {
            log.info("Quarkus: Processing incoming Avro message | Key: {} | Partition: {} | Offset: {}", 
                     record.getKey(), record.getPartition(), record.getOffset());
            log.info("Quarkus payload details | Payment: {}", record.getValue());
        });

        // Start consumer in a separate thread to not block Quarkus startup
        new Thread(() -> consumer.start()).start();
    }

    @PreDestroy
    void cleanup() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
