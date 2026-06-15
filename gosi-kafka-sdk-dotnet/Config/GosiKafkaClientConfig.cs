namespace Gosi.Kafka.Sdk.Config;

using System;
using Confluent.Kafka;
using Gosi.Kafka.Sdk.Auth;

/// <summary>
/// Immutable configuration class for GOSI .NET Kafka clients.
/// Enforces organizational standards and required properties.
/// </summary>
public class GosiKafkaClientConfig
{
    public string BootstrapServers { get; }
    public string? SchemaRegistryUrl { get; }
    public IAuthenticationHandler? AuthenticationHandler { get; }
    public string? ClientId { get; }
    public string? GroupId { get; }
    public SerializationFormat KeyFormat { get; }
    public SerializationFormat ValueFormat { get; }
    
    // Tuning parameters
    public int MaxInFlightRequests { get; }
    public int Retries { get; }
    public Acks Acks { get; }
    public bool EnableIdempotence { get; }
    public AutoOffsetReset AutoOffsetReset { get; }

    private GosiKafkaClientConfig(Builder builder)
    {
        BootstrapServers = builder._bootstrapServers!;
        SchemaRegistryUrl = builder._schemaRegistryUrl;
        AuthenticationHandler = builder._authenticationHandler;
        ClientId = builder._clientId;
        GroupId = builder._groupId;
        KeyFormat = builder._keyFormat;
        ValueFormat = builder._valueFormat;
        MaxInFlightRequests = builder._maxInFlightRequests;
        Retries = builder._retries;
        Acks = builder._acks;
        EnableIdempotence = builder._enableIdempotence;
        AutoOffsetReset = builder._autoOffsetReset;
    }

    public ProducerConfig BuildProducerConfig()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            ClientId = ClientId,
            Acks = Acks,
            EnableIdempotence = EnableIdempotence,
            MaxInFlight = MaxInFlightRequests,
            MessageSendMaxRetries = Retries
        };

        AuthenticationHandler?.Configure(config);

        return config;
    }

    public ConsumerConfig BuildConsumerConfig()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            ClientId = ClientId,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset,
            EnableAutoCommit = false // SDK manages commits
        };

        AuthenticationHandler?.Configure(config);

        return config;
    }

    public class Builder
    {
        internal string? _bootstrapServers;
        internal string? _schemaRegistryUrl;
        internal IAuthenticationHandler? _authenticationHandler;
        internal string? _clientId;
        internal string? _groupId;
        internal SerializationFormat _keyFormat = SerializationFormat.String;
        internal SerializationFormat _valueFormat = SerializationFormat.Avro;
        
        // Safe Organizational Defaults
        internal int _maxInFlightRequests = 5;
        internal int _retries = int.MaxValue;
        internal Acks _acks = Acks.All;
        internal bool _enableIdempotence = true;
        internal AutoOffsetReset _autoOffsetReset = AutoOffsetReset.Earliest;

        public Builder WithBootstrapServers(string bootstrapServers)
        {
            _bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder WithSchemaRegistryUrl(string schemaRegistryUrl)
        {
            _schemaRegistryUrl = schemaRegistryUrl;
            return this;
        }

        public Builder WithAuthenticationHandler(IAuthenticationHandler handler)
        {
            _authenticationHandler = handler;
            return this;
        }

        public Builder WithClientId(string clientId)
        {
            _clientId = clientId;
            return this;
        }

        public Builder WithGroupId(string groupId)
        {
            _groupId = groupId;
            return this;
        }

        public Builder WithKeyFormat(SerializationFormat format)
        {
            _keyFormat = format;
            return this;
        }

        public Builder WithValueFormat(SerializationFormat format)
        {
            _valueFormat = format;
            return this;
        }

        public GosiKafkaClientConfig Build()
        {
            if (string.IsNullOrWhiteSpace(_bootstrapServers))
                throw new ArgumentException("BootstrapServers is required");
                
            if ((_valueFormat == SerializationFormat.Avro || _valueFormat == SerializationFormat.JsonSchema) 
                && string.IsNullOrWhiteSpace(_schemaRegistryUrl))
                throw new ArgumentException("SchemaRegistryUrl is required when using Avro or JsonSchema");

            return new GosiKafkaClientConfig(this);
        }
    }
}
