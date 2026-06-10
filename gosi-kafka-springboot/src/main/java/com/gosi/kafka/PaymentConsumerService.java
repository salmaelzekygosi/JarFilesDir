package com.gosi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;

@Service
public class PaymentConsumerService {

    @KafkaListener(topics = "payments.demo-topic.v1", groupId = "kafka-demo-consumer-group")
    public void listen(ConsumerRecord<String, String> record) {
        String key = record.key();
        String payload = record.value();
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
        System.out.println("Spring Boot Received Kafka Message:");
        System.out.println("  Key:        " + key);
        System.out.println("  Partition:  " + partition);
        System.out.println("  Offset:     " + offset);
        System.out.println("  Trace ID:   " + traceId);
        System.out.println("  Payload:    " + payload);
        System.out.println("==================================================");
    }
}
