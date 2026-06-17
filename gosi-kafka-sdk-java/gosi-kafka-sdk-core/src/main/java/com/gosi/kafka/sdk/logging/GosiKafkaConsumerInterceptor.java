package com.gosi.kafka.sdk.logging;

import com.gosi.kafka.sdk.tracing.TraceContext;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


import java.util.Map;

public class GosiKafkaConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        if (!records.isEmpty()) {
            ConsumerRecord<K, V> firstRecord = records.iterator().next();
            String traceId = extractTraceIdFromPayload(firstRecord.value());
            
            // Push extracted traceId into MDC, otherwise fallback to headers
            if (traceId != null && !traceId.trim().isEmpty()) {
                org.slf4j.MDC.put(TraceContext.TRACE_ID_KEY, traceId);
            } else {
                TraceContext.initFromHeaders(firstRecord.headers());
            }
        }
        
        return records;
    }

    private String extractTraceIdFromPayload(Object value) {
        if (value instanceof org.apache.avro.generic.IndexedRecord avroRec) {
            org.apache.avro.Schema schema = avroRec.getSchema();
            
            if (schema.getField("traceId") != null) {
                Object val = avroRec.get(schema.getField("traceId").pos());
                if (val != null) return val.toString();
            } else if (schema.getField("trace_id") != null) {
                Object val = avroRec.get(schema.getField("trace_id").pos());
                if (val != null) return val.toString();
            }
        }
        return null;
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
