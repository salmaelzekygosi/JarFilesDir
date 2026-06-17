package com.gosi.kafka.sdk.logging;

import com.gosi.kafka.sdk.tracing.TraceContext;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.util.Map;

public class GosiKafkaProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
        String traceId = null;
        
        // Dynamically extract traceId from Avro payload if present
        if (producerRecord.value() instanceof org.apache.avro.generic.IndexedRecord avroRec) {
            org.apache.avro.Schema schema = avroRec.getSchema();
            
            if (schema.getField("traceId") != null) {
                Object val = avroRec.get(schema.getField("traceId").pos());
                if (val != null) traceId = val.toString();
            } else if (schema.getField("trace_id") != null) {
                Object val = avroRec.get(schema.getField("trace_id").pos());
                if (val != null) traceId = val.toString();
            }
        }
        
        // Push extracted traceId into MDC so injectIntoHeaders uses it instead of generating a random one
        if (traceId != null && !traceId.trim().isEmpty()) {
            org.slf4j.MDC.put(TraceContext.TRACE_ID_KEY, traceId);
        }
        
        TraceContext.injectIntoHeaders(producerRecord.headers());
        return producerRecord;
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
