package com.gosi.kafka.sdk.logging;

import com.gosi.kafka.sdk.tracing.TraceContext;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.MDC;

import java.util.Map;

public class GosiKafkaConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        if (!records.isEmpty()) {
            ConsumerRecord<K, V> firstRecord = records.iterator().next();
            TraceContext.initFromHeaders(firstRecord.headers());
        }
        
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
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
