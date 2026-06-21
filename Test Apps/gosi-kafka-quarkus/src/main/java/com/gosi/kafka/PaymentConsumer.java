package com.gosi.kafka;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gosi.kafka.avro.PaymentRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@ApplicationScoped
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    @Incoming("payments-in")
    public void consume(ConsumerRecord<String, PaymentRecord> consumerRecord) {
        log.info("Quarkus Native @Incoming: Processing incoming Avro message | Key: {} | Partition: {} | Offset: {}", 
                 consumerRecord.key(), consumerRecord.partition(), consumerRecord.offset());
                 
        PaymentRecord payload = consumerRecord.value();
        if (payload != null) {
            log.info("Quarkus payload details | ID: {} | Amount: {} | TraceId: {}", 
                     payload.getId(), payload.getAmount(), payload.getTraceId());
                     
            if (payload.getAmount() < 0) {
                throw new org.apache.kafka.common.KafkaException("Simulated processing failure! Amount is negative. This will trigger Quarkus DefaultResilienceWrapper and route to DLQ.");
            }
        }
    }
}
