package com.gosi.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import com.gosi.kafka.avro.PaymentRecord; // Generated Avro class

@Service
public class PaymentProducerService {

    private final KafkaTemplate<String, PaymentRecord> kafkaTemplate;

    @org.springframework.beans.factory.annotation.Value("${app.kafka.topic}")
    private String topicName;

    public PaymentProducerService(KafkaTemplate<String, PaymentRecord> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPayment(Payment payment) {
        try {
            // Validate / create traceId
            String traceId = payment.getTraceId();
            if (traceId == null || traceId.isBlank()) {
                traceId = UUID.randomUUID().toString();
                payment.setTraceId(traceId);
            }

            // Construct the Avro PaymentRecord
            PaymentRecord avroRecord = PaymentRecord.newBuilder()
                    .setId(payment.getId())
                    .setAmount(payment.getAmount())
                    .setCurrency(payment.getCurrency())
                    .setTraceId(traceId)
                    .build();

            // Create Producer Record with Avro payload
            ProducerRecord<String, PaymentRecord> record = new ProducerRecord<>(
                topicName, 
                payment.getId(), 
                avroRecord
            );

            // Inject the mandatory trace-id header
            record.headers().add(new RecordHeader("trace-id", traceId.getBytes(StandardCharsets.UTF_8)));

            kafkaTemplate.send(record).completable().whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Spring Boot: Produced Avro payment successfully: " + payment.getId() + 
                                       " | Partition: " + result.getRecordMetadata().partition() +
                                       " | Offset: " + result.getRecordMetadata().offset());
                } else {
                    System.err.println("Spring Boot: Failed to produce Avro payment: " + ex.getMessage());
                    ex.printStackTrace();
                }
            });

        } catch (Exception e) {
            throw new RuntimeException("Failed to produce Avro payment message", e);
        }
    }
}
