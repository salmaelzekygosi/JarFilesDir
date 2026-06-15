namespace Gosi.Kafka.Sdk.Auth;

using System;
using Confluent.Kafka;

/// <summary>
/// Configures OAuth authentication using Ping Identity OIDC flow for .NET clients.
/// </summary>
public class OAuthBearerAuthHandler : IAuthenticationHandler
{
    private readonly string _tokenEndpointUrl;
    private readonly string _clientId;
    private readonly string _clientSecret;
    private readonly bool _useTls;

    public OAuthBearerAuthHandler(string tokenEndpointUrl, string clientId, string clientSecret, bool useTls)
    {
        if (string.IsNullOrWhiteSpace(tokenEndpointUrl)) throw new ArgumentException("Token endpoint URL cannot be empty");
        if (string.IsNullOrWhiteSpace(clientId)) throw new ArgumentException("Client ID cannot be empty");
        if (string.IsNullOrWhiteSpace(clientSecret)) throw new ArgumentException("Client secret cannot be empty");

        _tokenEndpointUrl = tokenEndpointUrl;
        _clientId = clientId;
        _clientSecret = clientSecret;
        _useTls = useTls;
    }

    public void Configure(ClientConfig config)
    {
        config.SecurityProtocol = _useTls ? SecurityProtocol.SaslSsl : SecurityProtocol.SaslPlaintext;
        config.SaslMechanism = SaslMechanism.OAuthBearer;
        
        config.SaslOauthbearerTokenEndpointUrl = _tokenEndpointUrl;
        config.SaslOauthbearerClientId = _clientId;
        config.SaslOauthbearerClientSecret = _clientSecret;
        config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
    }
}
