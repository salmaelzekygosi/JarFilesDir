package com.gosi.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;

@ApplicationScoped
public class PaymentProducer {

    @Inject
    @Channel("payments-out")
    Emitter<String> emitter;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void publishPayment(Payment payment) {
        try {
            // Serialize payload to JSON String
            String jsonPayload = objectMapper.writeValueAsString(payment);
            
            // Fallback trace-id if not provided
            String traceId = payment.getTraceId();
            if (traceId == null || traceId.isBlank()) {
                traceId = java.util.UUID.randomUUID().toString();
                payment.setTraceId(traceId);
            }

            // Construct mandatory headers
            RecordHeaders headers = new RecordHeaders();
            headers.add("trace-id", traceId.getBytes(StandardCharsets.UTF_8));

            // Set message key as payment ID and append headers
            OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
                    .withKey(payment.getId())
                    .withHeaders(headers)
                    .build();

            // Emit the message
            emitter.send(Message.of(jsonPayload, Metadata.of(metadata)));
            System.out.println("Quarkus: Produced payment successfully: " + payment.getId() + " with trace-id: " + traceId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize and produce payment message", e);
        }
    }
}
