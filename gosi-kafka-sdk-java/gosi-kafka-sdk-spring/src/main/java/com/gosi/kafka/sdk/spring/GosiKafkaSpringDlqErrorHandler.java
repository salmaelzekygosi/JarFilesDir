package com.gosi.kafka.sdk.spring;

import com.gosi.kafka.sdk.producer.GosiKafkaProducer;
import com.gosi.kafka.sdk.resilience.DefaultResilienceWrapper;
import com.gosi.kafka.sdk.resilience.ResilienceConfig;
import com.gosi.kafka.sdk.resilience.ResilienceWrapper;
import com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;

/**
 * Spring Kafka DefaultErrorHandler that delegates to the GOSI SDK ResilienceWrapper.
 * <p>
 * This extends Spring's DefaultErrorHandler to ensure all org-standard metrics,
 * restart-loop detection, and per-stage DLQ routing are enforced natively within Spring Boot apps.
 * </p>
 */
public class GosiKafkaSpringDlqErrorHandler<K, V> extends DefaultErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GosiKafkaSpringDlqErrorHandler.class);

    public GosiKafkaSpringDlqErrorHandler(
            ResilienceConfig config, 
            GosiKafkaProducer<K, V> dlqProducer, 
            GosiTelemetryReporter telemetryReporter) {
            
        super((consumerRecord, thrownException) -> {
            LOG.debug("Spring DLQ Error Handler intercepting exception for record: {}", consumerRecord.key());
            
            // Re-instantiate the ResilienceWrapper for the recoverer logic
            ResilienceWrapper<K, V> wrapper = new DefaultResilienceWrapper<>(config, dlqProducer, telemetryReporter);
            
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

            try {
                wrapper.process(gosiRecord, r -> {
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
        });
    }
}
