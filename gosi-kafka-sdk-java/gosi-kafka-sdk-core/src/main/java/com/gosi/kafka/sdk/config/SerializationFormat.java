package com.gosi.kafka.sdk.config;

/**
 * Supported serialization formats for Kafka message values.
 */
public enum SerializationFormat {
    AVRO,
    JSON_SCHEMA,
    STRING // For simple string values or testing
}
