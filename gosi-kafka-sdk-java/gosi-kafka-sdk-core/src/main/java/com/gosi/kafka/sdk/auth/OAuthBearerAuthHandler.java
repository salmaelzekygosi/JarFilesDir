package com.gosi.kafka.sdk.auth;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Map;

/**
 * Configures OAuth authentication using Ping Identity OIDC flow.
 */
public class OAuthBearerAuthHandler implements AuthenticationHandler {

    private final String tokenEndpointUrl;
    private final String clientId;
    private final String clientSecret;
    private final boolean useTls;

    public OAuthBearerAuthHandler(String tokenEndpointUrl, String clientId, String clientSecret, boolean useTls) {
        if (tokenEndpointUrl == null || tokenEndpointUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Token endpoint URL cannot be empty");
        }
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("Client ID cannot be empty");
        }
        if (clientSecret == null || clientSecret.trim().isEmpty()) {
            throw new IllegalArgumentException("Client secret cannot be empty");
        }
        this.tokenEndpointUrl = tokenEndpointUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.useTls = useTls;
    }

    @Override
    public void configure(Map<String, Object> kafkaProperties) {
        kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, useTls ? "SASL_SSL" : "SASL_PLAINTEXT");
        kafkaProperties.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        
        kafkaProperties.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
        
        String jaasTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "clientId=\"%s\" " +
                "clientSecret=\"%s\" " +
                "oauth.token.endpoint.uri=\"%s\";";
                
        kafkaProperties.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(jaasTemplate, clientId, clientSecret, tokenEndpointUrl));
    }
}
