package com.gosi.kafka;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.apache.kafka.common.header.Header;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

// Generated Avro Class
import com.gosi.kafka.avro.PaymentRecord;

@ApplicationScoped
public class PaymentConsumer {

    @Incoming("payments-in")
    public CompletionStage<Void> consume(Message<PaymentRecord> message) {
        PaymentRecord payload = message.getPayload();
        
        // Extract Kafka Metadata to read headers
        var metadata = message.getMetadata(IncomingKafkaRecordMetadata.class);
        
        String key = "N/A";
        String traceId = "N/A";
        long offset = -1;
        int partition = -1;

        if (metadata.isPresent()) {
            IncomingKafkaRecordMetadata<String, PaymentRecord> kafkaMetadata = metadata.get();
            key = kafkaMetadata.getKey();
            offset = kafkaMetadata.getOffset();
            partition = kafkaMetadata.getPartition();
            
            // Read trace-id header
            for (Header header : kafkaMetadata.getHeaders()) {
                if ("trace-id".equalsIgnoreCase(header.key())) {
                    traceId = new String(header.value(), StandardCharsets.UTF_8);
                }
            }
        }

        System.out.println("==================================================");
        System.out.println("Quarkus Received Avro Kafka Message:");
        System.out.println("  Key:        " + key);
        System.out.println("  Partition:  " + partition);
        System.out.println("  Offset:     " + offset);
        System.out.println("  Trace ID:   " + traceId);
        System.out.println("  Payload:    " + payload);
        System.out.println("==================================================");

        return message.ack();
    }
}
