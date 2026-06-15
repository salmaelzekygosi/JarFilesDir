package com.gosi.kafka;

import com.gosi.kafka.sdk.producer.GosiKafkaProducer;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gosi.kafka.avro.PaymentRecord; // Generated Avro class

@Service
public class PaymentProducerService {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducerService.class);

    private final GosiKafkaProducer<Object, Object> gosiKafkaProducer;

    @org.springframework.beans.factory.annotation.Value("${app.kafka.topic}")
    private String topicName;

    public PaymentProducerService(GosiKafkaProducer<Object, Object> gosiKafkaProducer) {
        this.gosiKafkaProducer = gosiKafkaProducer;
    }

    public void sendPayment(Payment payment) {
        try {
            // Construct the Avro PaymentRecord
            PaymentRecord avroRecord = PaymentRecord.newBuilder()
                    .setId(payment.getId())
                    .setAmount(payment.getAmount())
                    .setCurrency(payment.getCurrency())
                    .setTraceId(payment.getTraceId() != null ? payment.getTraceId() : "")
                    .build();

            log.info("Spring Boot initiating produce request for payment: {}", payment.getId());

            // GosiKafkaProducer automatically generates/injects trace_id, captures latency, and reports telemetry
            gosiKafkaProducer.sendAsync(topicName, payment.getId(), avroRecord);

        } catch (Exception e) {
            log.error("Exception during sending payment: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to produce Avro payment message", e);
        }
    }
}
