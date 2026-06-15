namespace Gosi.Kafka.Sdk.Config;

/// <summary>
/// Supported serialization formats for Kafka message values in .NET.
/// </summary>
public enum SerializationFormat
{
    Avro,
    JsonSchema,
    String
}
