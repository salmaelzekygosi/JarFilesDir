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

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${gosi.kafka.group-id}")
    public void listen(ConsumerRecord<String, PaymentRecord> consumerRecord) {
        String key = consumerRecord.key();
        PaymentRecord payload = consumerRecord.value();
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();
        
        log.info("Processing incoming Avro Kafka message: key={}, partition={}, offset={}", key, partition, offset);
        
        if (payload != null) {
            log.info("Payment details parsed | ID: {} | Amount: {} | Currency: {} | Payload Trace ID: {}", 
                     payload.getId(), payload.getAmount(), payload.getCurrency(), payload.getTraceId());
                     
            if (payload.getAmount() < 0) {
                throw new org.apache.kafka.common.KafkaException("Simulated processing failure! Amount is negative. This will trigger Spring's DefaultResilienceWrapper and route to DLQ.");
            }
        } else {
            log.warn("Payment payload is null for key={}", key);
        }
    }
}
