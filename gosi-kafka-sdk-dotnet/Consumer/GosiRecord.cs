namespace Gosi.Kafka.Sdk.Consumer;

using Confluent.Kafka;

public class GosiRecord<TKey, TValue>
{
    public TKey Key { get; }
    public TValue Value { get; }
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public string TraceId { get; }
    public Headers Headers { get; }
    public long Timestamp { get; }

    public GosiRecord(TKey key, TValue value, string topic, int partition, long offset, string traceId, Headers headers, long timestamp)
    {
        Key = key;
        Value = value;
        Topic = topic;
        Partition = partition;
        Offset = offset;
        TraceId = traceId;
        Headers = headers;
        Timestamp = timestamp;
    }
}
