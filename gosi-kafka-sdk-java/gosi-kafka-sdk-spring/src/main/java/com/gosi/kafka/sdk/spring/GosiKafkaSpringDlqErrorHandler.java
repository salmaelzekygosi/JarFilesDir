package com.gosi.kafka.sdk.spring;

import com.gosi.kafka.sdk.producer.GosiKafkaProducer;
import com.gosi.kafka.sdk.resilience.DefaultResilienceWrapper;
import com.gosi.kafka.sdk.resilience.ResilienceConfig;
import com.gosi.kafka.sdk.resilience.ResilienceWrapper;
import com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Spring Kafka CommonErrorHandler that delegates to the GOSI SDK ResilienceWrapper.
 * <p>
 * This replaces Spring's DefaultErrorHandler to ensure all org-standard metrics,
 * restart-loop detection, and per-stage DLQ routing are enforced natively within Spring Boot apps.
 * </p>
 */
public class GosiKafkaSpringDlqErrorHandler<K, V> implements CommonErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GosiKafkaSpringDlqErrorHandler.class);

    private final ResilienceWrapper<K, V> resilienceWrapper;

    public GosiKafkaSpringDlqErrorHandler(
            ResilienceConfig config, 
            GosiKafkaProducer<K, V> dlqProducer, 
            GosiTelemetryReporter telemetryReporter) {
            
        this.resilienceWrapper = new DefaultResilienceWrapper<>(config, dlqProducer, telemetryReporter);
    }

    @Override
    @SuppressWarnings("all")
    public void handleRecord(Exception thrownException, ConsumerRecord<?, ?> consumerRecord, org.apache.kafka.clients.consumer.Consumer<?, ?> consumer, MessageListenerContainer container) {
        handleFailedRecord(thrownException, consumerRecord);
    }

    @Override
    public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> consumerRecord, org.apache.kafka.clients.consumer.Consumer<?, ?> consumer, MessageListenerContainer container) {
        handleFailedRecord(thrownException, consumerRecord);
        return true;
    }

    private void handleFailedRecord(Exception thrownException, ConsumerRecord<?, ?> consumerRecord) {
        LOG.debug("Spring DLQ Error Handler intercepting exception for record: {}", consumerRecord.key());
        
        // Convert Spring's generic ConsumerRecord to our GosiRecord representation
        @SuppressWarnings("unchecked")
        com.gosi.kafka.sdk.consumer.GosiRecord<K, V> gosiRecord = new com.gosi.kafka.sdk.consumer.GosiRecord<>(
                (K) consumerRecord.key(),
                (V) consumerRecord.value(),
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                com.gosi.kafka.sdk.tracing.TraceContext.initFromHeaders(consumerRecord.headers()),
                consumerRecord.headers(),
                consumerRecord.timestamp()
        );

        // Process the failure via our ResilienceWrapper logic
        try {
            resilienceWrapper.process(gosiRecord, r -> {
                throw thrownException;
            });
        } catch (Exception e) {
            LOG.error("Failed to handle exception via ResilienceWrapper", e);
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new org.apache.kafka.common.KafkaException("Failed to process DLQ routing", e);
        } finally {
            com.gosi.kafka.sdk.tracing.TraceContext.clear();
        }
    }
}
