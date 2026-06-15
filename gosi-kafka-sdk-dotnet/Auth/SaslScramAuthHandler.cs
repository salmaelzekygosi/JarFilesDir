namespace Gosi.Kafka.Sdk.Auth;

using System;
using Confluent.Kafka;

/// <summary>
/// Configures SASL/SCRAM-SHA-512 authentication for .NET clients.
/// </summary>
public class SaslScramAuthHandler : IAuthenticationHandler
{
    private readonly string _username;
    private readonly string _password;
    private readonly bool _useTls;

    public SaslScramAuthHandler(string username, string password, bool useTls)
    {
        if (string.IsNullOrWhiteSpace(username)) throw new ArgumentException("Username cannot be empty");
        if (string.IsNullOrWhiteSpace(password)) throw new ArgumentException("Password cannot be empty");

        _username = username;
        _password = password;
        _useTls = useTls;
    }

    public void Configure(ClientConfig config)
    {
        config.SecurityProtocol = _useTls ? SecurityProtocol.SaslSsl : SecurityProtocol.SaslPlaintext;
        config.SaslMechanism = SaslMechanism.ScramSha512;
        config.SaslUsername = _username;
        config.SaslPassword = _password;
    }
}
