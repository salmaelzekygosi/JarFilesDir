namespace Gosi.Kafka.Sdk.Auth;

using System;
using Confluent.Kafka;

/// <summary>
/// Configures Mutual TLS (mTLS) authentication for .NET clients.
/// </summary>
public class MutualTlsAuthHandler : IAuthenticationHandler
{
    private readonly string _keystoreLocation;
    private readonly string _keystorePassword;
    private readonly string _truststoreLocation;

    public MutualTlsAuthHandler(string keystoreLocation, string keystorePassword, string truststoreLocation)
    {
        if (string.IsNullOrWhiteSpace(keystoreLocation)) throw new ArgumentException("Keystore location cannot be empty");

        _keystoreLocation = keystoreLocation;
        _keystorePassword = keystorePassword;
        _truststoreLocation = truststoreLocation;
    }

    public void Configure(ClientConfig config)
    {
        config.SecurityProtocol = SecurityProtocol.Ssl;
        config.SslKeystoreLocation = _keystoreLocation;
        config.SslKeystorePassword = _keystorePassword;
        
        if (!string.IsNullOrWhiteSpace(_truststoreLocation))
        {
            config.SslCaLocation = _truststoreLocation;
        }
    }
}
