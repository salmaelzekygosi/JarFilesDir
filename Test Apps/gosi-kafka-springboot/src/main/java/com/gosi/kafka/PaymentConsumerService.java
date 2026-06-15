package com.gosi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gosi.kafka.avro.PaymentRecord; // Generated Avro class

@Service
public class PaymentConsumerService {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumerService.class);

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<String, PaymentRecord> record) {
        String key = record.key();
        PaymentRecord payload = record.value();
        long offset = record.offset();
        int partition = record.partition();
        
        log.info("Processing incoming Avro Kafka message: key={}, partition={}, offset={}", key, partition, offset);
        
        if (payload != null) {
            log.info("Payment details parsed | ID: {} | Amount: {} | Currency: {} | Payload Trace ID: {}", 
                     payload.getId(), payload.getAmount(), payload.getCurrency(), payload.getTraceId());
        } else {
            log.warn("Payment payload is null for key={}", key);
        }
    }
}
