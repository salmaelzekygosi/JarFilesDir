package com.gosi.kafka.sdk.spring;

import com.gosi.kafka.sdk.tracing.TraceContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * Spring Kafka interceptor to extract trace_id from headers and inject it into MDC
 * before the @KafkaListener method is invoked, and clean up afterwards.
 */
public class GosiKafkaSpringConsumerInterceptor<K, V> implements RecordInterceptor<K, V> {

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        TraceContext.initFromHeaders(record.headers());
        return record;
    }

    @Override
    public void success(ConsumerRecord<K, V> record, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        TraceContext.clear();
    }

    @Override
    public void failure(ConsumerRecord<K, V> record, Exception exception, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        TraceContext.clear();
    }
}
