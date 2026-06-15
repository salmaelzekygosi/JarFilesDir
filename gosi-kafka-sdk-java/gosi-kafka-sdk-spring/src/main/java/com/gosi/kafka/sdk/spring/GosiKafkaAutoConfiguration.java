package com.gosi.kafka.sdk.spring;

import com.gosi.kafka.sdk.auth.AuthenticationHandler;
import com.gosi.kafka.sdk.auth.MutualTlsAuthHandler;
import com.gosi.kafka.sdk.auth.OAuthBearerAuthHandler;
import com.gosi.kafka.sdk.auth.SaslPlainAuthHandler;
import com.gosi.kafka.sdk.auth.SaslScramAuthHandler;
import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import com.gosi.kafka.sdk.producer.GosiKafkaProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(GosiKafkaProducer.class)
@EnableConfigurationProperties(GosiKafkaProperties.class)
public class GosiKafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public GosiKafkaClientConfig gosiKafkaClientConfig(GosiKafkaProperties properties) {
        GosiKafkaClientConfig.Builder builder = GosiKafkaClientConfig.builder()
                .bootstrapServers(properties.getBootstrapServers())
                .schemaRegistryUrl(properties.getSchemaRegistryUrl())
                .clientId(properties.getClientId())
                .groupId(properties.getGroupId());

        if (properties.getSaslMechanism() != null) {
            boolean useTls = "SASL_SSL".equalsIgnoreCase(properties.getSecurityProtocol());
            AuthenticationHandler authHandler = null;

            switch (properties.getSaslMechanism().toUpperCase()) {
                case "PLAIN":
                    authHandler = new SaslPlainAuthHandler(properties.getUsername(), properties.getPassword(), useTls);
                    break;
                case "OAUTHBEARER":
                    authHandler = new OAuthBearerAuthHandler(properties.getOauthTokenUrl(), properties.getUsername(), properties.getPassword(), useTls);
                    break;
                case "SCRAM-SHA-512":
                    authHandler = new SaslScramAuthHandler(properties.getUsername(), properties.getPassword(), useTls);
                    break;
            }
            if (authHandler != null) {
                builder.authenticationHandler(authHandler);
            }
        } else if ("SSL".equalsIgnoreCase(properties.getSecurityProtocol())) {
            builder.authenticationHandler(new MutualTlsAuthHandler(
                    properties.getKeystoreLocation(), properties.getKeystorePassword(), properties.getKeystorePassword(),
                    properties.getTruststoreLocation(), properties.getTruststorePassword()
            ));
        }

        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public GosiKafkaProducer<Object, Object> gosiKafkaProducer(GosiKafkaClientConfig config) {
        return new GosiKafkaProducer<>(config);
    }
    
    @Bean
    public GosiKafkaSpringConsumerInterceptor gosiKafkaSpringConsumerInterceptor() {
        return new GosiKafkaSpringConsumerInterceptor();
    }
}
