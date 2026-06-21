package com.gosi.kafka.sdk.quarkus;

import com.gosi.kafka.sdk.auth.AuthenticationHandler;
import com.gosi.kafka.sdk.auth.MutualTlsAuthHandler;
import com.gosi.kafka.sdk.auth.OAuthBearerAuthHandler;
import com.gosi.kafka.sdk.auth.SaslPlainAuthHandler;
import com.gosi.kafka.sdk.auth.SaslScramAuthHandler;
import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import com.gosi.kafka.sdk.producer.GosiKafkaProducer;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.Optional;

@ApplicationScoped
public class GosiKafkaQuarkusProducerFactory {

    @ConfigProperty(name = "gosi.kafka.bootstrap-servers")
    String bootstrapServers;

    @ConfigProperty(name = "gosi.kafka.schema-registry-url", defaultValue = "")
    String schemaRegistryUrl;

    @ConfigProperty(name = "gosi.kafka.client-id", defaultValue = "")
    String clientId;

    @ConfigProperty(name = "gosi.kafka.sasl-mechanism")
    Optional<String> saslMechanism;

    @ConfigProperty(name = "gosi.kafka.security-protocol", defaultValue = "SASL_PLAINTEXT")
    String securityProtocol;

    @ConfigProperty(name = "gosi.kafka.username")
    Optional<String> username;

    @ConfigProperty(name = "gosi.kafka.password")
    Optional<String> password;
    
    @ConfigProperty(name = "gosi.kafka.oauth-token-url")
    Optional<String> oauthTokenUrl;

    @ConfigProperty(name = "gosi.kafka.oauth-scope")
    Optional<String> oauthScope;

    @ConfigProperty(name = "gosi.kafka.keystore-location")
    Optional<String> keystoreLocation;
    
    @ConfigProperty(name = "gosi.kafka.keystore-password")
    Optional<String> keystorePassword;
    
    @ConfigProperty(name = "gosi.kafka.truststore-location")
    Optional<String> truststoreLocation;
    
    @ConfigProperty(name = "gosi.kafka.truststore-password")
    Optional<String> truststorePassword;

    @Produces
    @Singleton
    public GosiKafkaClientConfig gosiKafkaClientConfig() {
        GosiKafkaClientConfig.Builder builder = GosiKafkaClientConfig.builder()
                .bootstrapServers(bootstrapServers)
                .schemaRegistryUrl(schemaRegistryUrl)
                .clientId(clientId);

        if (saslMechanism.isPresent()) {
            boolean useTls = "SASL_SSL".equalsIgnoreCase(securityProtocol);
            AuthenticationHandler authHandler = null;

            switch (saslMechanism.get().toUpperCase()) {
                case "PLAIN":
                    authHandler = new SaslPlainAuthHandler(username.orElse(""), password.orElse(""), useTls);
                    break;
                case "OAUTHBEARER":
                    authHandler = new OAuthBearerAuthHandler(oauthTokenUrl.orElse(""), username.orElse(""), password.orElse(""), oauthScope.orElse("write"), useTls);
                    break;
                case "SCRAM-SHA-512":
                    authHandler = new SaslScramAuthHandler(username.orElse(""), password.orElse(""), useTls);
                    break;
            }
            if (authHandler != null) {
                builder.authenticationHandler(authHandler);
            }
        } else if ("SSL".equalsIgnoreCase(securityProtocol) && keystoreLocation.isPresent()) {
            builder.authenticationHandler(new MutualTlsAuthHandler(
                    keystoreLocation.get(), keystorePassword.orElse(null), keystorePassword.orElse(null),
                    truststoreLocation.orElse(null), truststorePassword.orElse(null)
            ));
        }

        return builder.build();
    }

    @Produces
    @ApplicationScoped
    public GosiKafkaProducer<Object, Object> gosiKafkaProducer(GosiKafkaClientConfig config) {
        return new GosiKafkaProducer<>(config);
    }
}
