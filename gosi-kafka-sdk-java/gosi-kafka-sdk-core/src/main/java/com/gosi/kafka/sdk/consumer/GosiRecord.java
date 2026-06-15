package com.gosi.kafka.sdk.consumer;

import org.apache.kafka.common.header.Headers;

/**
 * Encapsulates a consumed Kafka record along with its metadata.
 */
public class GosiRecord<K, V> {
    private final K key;
    private final V value;
    private final String topic;
    private final int partition;
    private final long offset;
    private final String traceId;
    private final Headers headers;
    private final long timestamp;

    public GosiRecord(K key, V value, String topic, int partition, long offset, String traceId, Headers headers, long timestamp) {
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.traceId = traceId;
        this.headers = headers;
        this.timestamp = timestamp;
    }

    public K getKey() { return key; }
    public V getValue() { return value; }
    public String getTopic() { return topic; }
    public int getPartition() { return partition; }
    public long getOffset() { return offset; }
    public String getTraceId() { return traceId; }
    public Headers getHeaders() { return headers; }
    public long getTimestamp() { return timestamp; }
}
