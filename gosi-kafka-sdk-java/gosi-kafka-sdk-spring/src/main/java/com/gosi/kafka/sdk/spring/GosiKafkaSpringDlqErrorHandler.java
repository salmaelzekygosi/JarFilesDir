package com.gosi.kafka.sdk.spring;

import com.gosi.kafka.sdk.producer.GosiKafkaProducer;
import com.gosi.kafka.sdk.resilience.DefaultResilienceWrapper;
import com.gosi.kafka.sdk.resilience.ResilienceConfig;
import com.gosi.kafka.sdk.resilience.ResilienceWrapper;
import com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, org.apache.kafka.clients.consumer.Consumer<?, ?> consumer, MessageListenerContainer container) {
        LOG.debug("Spring DLQ Error Handler intercepting exception for record: {}", record.key());
        
        // Convert Spring's generic ConsumerRecord to our GosiRecord representation
        @SuppressWarnings("unchecked")
        com.gosi.kafka.sdk.consumer.GosiRecord<K, V> gosiRecord = new com.gosi.kafka.sdk.consumer.GosiRecord<>(
                (K) record.key(),
                (V) record.value(),
                record.topic(),
                record.partition(),
                record.offset(),
                com.gosi.kafka.sdk.tracing.TraceContext.initFromHeaders(record.headers()),
                record.headers(),
                record.timestamp()
        );

        // Process the failure via our ResilienceWrapper logic (which handles DLQ routing, fail-fast, etc.)
        try {
            // We pass a dummy handler because we're already IN the error state.
            // The record has already failed. So we just need to trigger the failure path.
            resilienceWrapper.process(gosiRecord, r -> {
                throw thrownException;
            });
            return true; // Handled successfully (e.g. routed to DLQ)
        } catch (Exception e) {
            LOG.error("Failed to handle exception via ResilienceWrapper", e);
            // If the policy is FAIL_FAST, process() will rethrow.
            // We return false to let Spring know we didn't "recover" it.
            return false;
        } finally {
            com.gosi.kafka.sdk.tracing.TraceContext.clear();
        }
    }
}
