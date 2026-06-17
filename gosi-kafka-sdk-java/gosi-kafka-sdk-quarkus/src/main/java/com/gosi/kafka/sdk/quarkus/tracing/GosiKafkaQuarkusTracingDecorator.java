package com.gosi.kafka.sdk.quarkus.tracing;

import com.gosi.kafka.sdk.tracing.TraceContext;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.MDC;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;

@ApplicationScoped
public class GosiKafkaQuarkusTracingDecorator implements PublisherDecorator, SubscriberDecorator {

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, String channelName, boolean isConnector) {
        // Intercept incoming messages before they reach @Incoming on the event loop
        return publisher.onItem().invoke(this::extractAndSetTraceId);
    }

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName, boolean isConnector) {
        // Intercept outgoing messages before they are sent to the connector on the executor thread
        return publisher.onItem().invoke(this::extractAndSetTraceId);
    }

    @Override
    public int getPriority() {
        return 10;
    }
    
    private void extractAndSetTraceId(Message<?> message) {
        if (message == null) return;
        
        Object payload = message.getPayload();
        if (payload instanceof IndexedRecord avroRec) {
            String traceId = extractTraceIdFromAvro(avroRec);
            if (traceId != null && !traceId.trim().isEmpty()) {
                MDC.put(TraceContext.TRACE_ID_KEY, traceId);
            }
        }
    }

    private String extractTraceIdFromAvro(IndexedRecord avroRec) {
        Schema schema = avroRec.getSchema();
        if (schema.getField("traceId") != null) {
            Object val = avroRec.get(schema.getField("traceId").pos());
            if (val != null) return val.toString();
        } else if (schema.getField("trace_id") != null) {
            Object val = avroRec.get(schema.getField("trace_id").pos());
            if (val != null) return val.toString();
        }
        return null;
    }
}
