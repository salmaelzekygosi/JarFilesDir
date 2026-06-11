package com.gosi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;
import com.gosi.kafka.avro.PaymentRecord; // Generated Avro class

@Service
public class PaymentConsumerService {

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<String, PaymentRecord> record) {
        String key = record.key();
        PaymentRecord payload = record.value();
        long offset = record.offset();
        int partition = record.partition();
        
        String traceId = "N/A";
        
        // Extract trace-id header from Kafka record headers
        for (Header header : record.headers()) {
            if ("trace-id".equalsIgnoreCase(header.key())) {
                traceId = new String(header.value(), StandardCharsets.UTF_8);
            }
        }

        System.out.println("==================================================");
        System.out.println("Spring Boot Received Avro Kafka Message:");
        System.out.println("  Key:        " + key);
        System.out.println("  Partition:  " + partition);
        System.out.println("  Offset:     " + offset);
        System.out.println("  Trace ID:   " + traceId);
        if (payload != null) {
            System.out.println("  Payload ID: " + payload.getId());
            System.out.println("  Amount:     " + payload.getAmount());
            System.out.println("  Currency:   " + payload.getCurrency());
        } else {
            System.out.println("  Payload:    null");
        }
        System.out.println("==================================================");
    }
}
