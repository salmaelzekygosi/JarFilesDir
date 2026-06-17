package com.gosi.kafka.sdk.logging;

import com.gosi.kafka.sdk.tracing.TraceContext;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.MDC;

import java.util.Map;

public class GosiKafkaProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        TraceContext.injectIntoHeaders(record.headers());
        
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // No action required
    }

    @Override
    public void close() {
        // No action required
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No action required
    }
}
