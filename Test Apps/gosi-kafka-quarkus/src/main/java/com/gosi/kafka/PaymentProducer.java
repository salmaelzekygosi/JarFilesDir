package com.gosi.kafka;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gosi.kafka.avro.PaymentRecord; // Generated Avro Class
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

@ApplicationScoped
public class PaymentProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);

    @Inject
    @Channel("payments-out")
    Emitter<PaymentRecord> emitter;

    public void publishPayment(Payment payment) {
        try {
            // Create the generated Avro record
            PaymentRecord avroRecord = PaymentRecord.newBuilder()
                    .setId(payment.getId())
                    .setAmount(payment.getAmount())
                    .setCurrency(payment.getCurrency())
                    .setTraceId(payment.getTraceId() != null ? payment.getTraceId() : "")
                    .build();

            // Emit the Avro record natively using Quarkus SmallRye Messaging
            // The Gosi SDK intercepts this and injects trace_id, handles serialization, etc.
            Message<PaymentRecord> message = Message.of(avroRecord)
                .addMetadata(OutgoingKafkaRecordMetadata.<String>builder()
                    .withKey(payment.getId())
                    .build());
                    
            emitter.send(message);
            
            log.info("Quarkus Native @Outgoing: Produced Avro payment request initiated for: {}", payment.getId());
        } catch (Exception e) {
            log.error("Failed to produce Avro payment message: {}", e.getMessage(), e);
            throw new org.apache.kafka.common.errors.SerializationException("Failed to produce Avro payment message", e);
        }
    }
}
