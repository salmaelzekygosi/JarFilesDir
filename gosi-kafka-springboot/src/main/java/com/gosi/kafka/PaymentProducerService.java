package com.gosi.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
public class PaymentProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PaymentProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPayment(Payment payment) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(payment);
            
            // Validate / create traceId
            String traceId = payment.getTraceId();
            if (traceId == null || traceId.isBlank()) {
                traceId = UUID.randomUUID().toString();
                payment.setTraceId(traceId);
            }

            // Create Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "payments.demo-topic.v1", 
                payment.getId(), 
                jsonPayload
            );

            // Inject the mandatory trace-id header
            record.headers().add(new RecordHeader("trace-id", traceId.getBytes(StandardCharsets.UTF_8)));

            kafkaTemplate.send(record).whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Spring Boot: Produced payment successfully: " + payment.getId() + 
                                       " | Partition: " + result.getRecordMetadata().partition() +
                                       " | Offset: " + result.getRecordMetadata().offset());
                } else {
                    System.err.println("Spring Boot: Failed to produce payment: " + ex.getMessage());
                    ex.printStackTrace();
                }
            });

        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize and produce payment", e);
        }
    }
}
