namespace Gosi.Kafka.Sdk.Auth;

using Confluent.Kafka;

/// <summary>
/// Strategy interface for configuring Kafka authentication in .NET.
/// </summary>
public interface IAuthenticationHandler
{
    void Configure(ClientConfig config);
}
