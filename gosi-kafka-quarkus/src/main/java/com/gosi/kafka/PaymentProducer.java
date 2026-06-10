package com.gosi.kafka;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

// Generated Avro Class
import com.gosi.kafka.avro.PaymentRecord;

@ApplicationScoped
public class PaymentProducer {

    @Inject
    @Channel("payments-out")
    Emitter<PaymentRecord> emitter;

    public void publishPayment(Payment payment) {
        try {
            // Validate / create traceId
            String traceId = payment.getTraceId();
            if (traceId == null || traceId.isBlank()) {
                traceId = UUID.randomUUID().toString();
                payment.setTraceId(traceId);
            }

            // Construct mandatory headers
            RecordHeaders headers = new RecordHeaders();
            headers.add("trace-id", traceId.getBytes(StandardCharsets.UTF_8));

            // Create the generated Avro record
            PaymentRecord avroRecord = PaymentRecord.newBuilder()
                    .setId(payment.getId())
                    .setAmount(payment.getAmount())
                    .setCurrency(payment.getCurrency())
                    .setTraceId(traceId)
                    .build();

            // Set message key as payment ID and append headers
            OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
                    .withKey(payment.getId())
                    .withHeaders(headers)
                    .build();

            // Emit the Avro record
            emitter.send(Message.of(avroRecord, Metadata.of(metadata)));
            System.out.println("Quarkus: Produced Avro payment successfully: " + payment.getId() + " with trace-id: " + traceId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce Avro payment message", e);
        }
    }
}
