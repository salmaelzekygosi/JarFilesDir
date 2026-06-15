package com.gosi.kafka.sdk.consumer;

/**
 * Functional interface for handling consumed Kafka records.
 */
@FunctionalInterface
public interface RecordHandler<K, V> {
    void handle(GosiRecord<K, V> record) throws Exception;
}
