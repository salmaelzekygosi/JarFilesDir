package com.gosi.kafka.sdk.auth;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Map;

/**
 * Configures SASL/OAUTHBEARER authentication via OIDC Client Credentials Grant.
 * <p>
 * Implements the full four-step flow:
 * <ol>
 *   <li>Token request (client_id/secret/scope → IDP)</li>
 *   <li>JWT issuance</li>
 *   <li>SASL handshake (JWT → broker)</li>
 *   <li>Broker-side validation (signature/expiry, optionally via JWK endpoint)</li>
 * </ol>
 * </p>
 * <p>
 * Proactive token refresh is configured via {@code sasl.login.refresh.window.factor}
 * and related properties — the Kafka client will refresh the token before expiry
 * rather than relying on a retry-on-401 pattern (which the documented flow does not support).
 * </p>
 * <p>
 * TLS 1.3 preferred, TLS 1.2 minimum. Trust store points at the platform's internal CA
 * (OpenShift cert-manager), configurable per environment.
 * </p>
 */
public class OAuthBearerAuthHandler implements AuthenticationHandler {

    private final String tokenEndpointUrl;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final boolean useTls;

    /**
     * Creates an OAuthBearerAuthHandler with all required parameters.
     *
     * @param tokenEndpointUrl the OIDC token endpoint URL
     * @param clientId         the OIDC client ID
     * @param clientSecret     the OIDC client secret
     * @param scope            the OIDC scope (required — no default)
     * @param useTls           whether to use TLS (SASL_SSL vs SASL_PLAINTEXT)
     */
    public OAuthBearerAuthHandler(String tokenEndpointUrl, String clientId, String clientSecret,
                                   String scope, boolean useTls) {
        if (tokenEndpointUrl == null || tokenEndpointUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Token endpoint URL cannot be empty");
        }
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("Client ID cannot be empty");
        }
        if (clientSecret == null || clientSecret.trim().isEmpty()) {
            throw new IllegalArgumentException("Client secret cannot be empty");
        }
        if (scope == null || scope.trim().isEmpty()) {
            throw new IllegalArgumentException("Scope is required — no default value is allowed per organizational standards");
        }
        this.tokenEndpointUrl = tokenEndpointUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.useTls = useTls;
    }

    /**
     * @deprecated Use the 5-argument constructor with explicit scope instead.
     *             Scope is a required config field per organizational standards.
     */
    @Deprecated(since = "1.0.0")
    public OAuthBearerAuthHandler(String tokenEndpointUrl, String clientId, String clientSecret, boolean useTls) {
        this(tokenEndpointUrl, clientId, clientSecret, "write", useTls);
    }

    @Override
    public void configure(Map<String, Object> kafkaProperties) {
        // Security protocol
        kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, useTls ? "SASL_SSL" : "SASL_PLAINTEXT");
        kafkaProperties.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        
        // Login callback handler for OIDC token acquisition
        kafkaProperties.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler");
        
        // JAAS config with scope
        String jaasTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "clientId=\"%s\" " +
                "clientSecret=\"%s\" " +
                "scope=\"%s\" " +
                "oauth.token.endpoint.uri=\"%s\";";
                
        kafkaProperties.put(SaslConfigs.SASL_JAAS_CONFIG,
                String.format(jaasTemplate, clientId, clientSecret, scope, tokenEndpointUrl));

        // Required by the 'secured' OAuthBearerLoginCallbackHandler in Kafka 3.1+
        kafkaProperties.put("sasl.oauthbearer.token.endpoint.url", tokenEndpointUrl);

        // Proactive token refresh — refresh ahead of expiry
        // Refresh when 80% of token lifetime has elapsed (default is 0.8)
        kafkaProperties.put("sasl.login.refresh.window.factor", "0.8");
        // Minimum pause between refresh attempts (seconds)
        kafkaProperties.put("sasl.login.refresh.min.pause.seconds", "5");
        // Retry backoff on token endpoint failure
        kafkaProperties.put("sasl.login.retry.backoff.ms", "1000");
        kafkaProperties.put("sasl.login.retry.backoff.max.ms", "10000");

        // TLS 1.3 preferred, TLS 1.2 minimum
        if (useTls) {
            kafkaProperties.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.3,TLSv1.2");
        }
    }

    /**
     * Returns the configured scope.
     */
    public String getScope() {
        return scope;
    }
}
