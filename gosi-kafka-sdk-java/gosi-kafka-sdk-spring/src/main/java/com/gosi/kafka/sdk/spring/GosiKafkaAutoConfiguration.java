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
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import java.util.Map;

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
                .groupId(properties.getGroupId())
                .additionalProperties(new java.util.HashMap<>(properties.getProperties()));

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
                default:
                    // Unsupported or standard mechanism
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
    @ConditionalOnMissingBean
    public ConsumerFactory<Object, Object> gosiKafkaConsumerFactory(GosiKafkaClientConfig config, GosiKafkaProperties properties) {
        Map<String, Object> props = config.buildConsumerProperties();
        
        // Apply serialization configs based on format
        if (config.getSchemaRegistryUrl() != null && !config.getSchemaRegistryUrl().isEmpty()) {
            props.put("schema.registry.url", config.getSchemaRegistryUrl());
            if ("SASL_SSL".equalsIgnoreCase(properties.getSecurityProtocol()) || "SSL".equalsIgnoreCase(properties.getSecurityProtocol())) {
                if (properties.getTruststoreLocation() != null) {
                    props.put("schema.registry.ssl.truststore.location", properties.getTruststoreLocation());
                    props.put("schema.registry.ssl.truststore.password", properties.getTruststorePassword());
                    props.put("schema.registry.ssl.truststore.type", "JKS");
                }
                if (properties.getSaslMechanism() != null && "OAUTHBEARER".equalsIgnoreCase(properties.getSaslMechanism())) {
                    props.put("schema.registry.bearer.auth.credentials.source", "OAUTHBEARER");
                    props.put("schema.registry.bearer.auth.issuer.endpoint.url", properties.getOauthTokenUrl());
                    props.put("schema.registry.bearer.auth.client.id", properties.getUsername());
                    props.put("schema.registry.bearer.auth.client.secret", properties.getPassword());
                    props.put("schema.registry.bearer.auth.scope", "write");
                }
            }
        }

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("specific.avro.reader", "true");
        
        // Inject SDK Consumer Interceptor
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, 
                  GosiKafkaSpringConsumerInterceptor.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    @ConditionalOnMissingBean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> gosiKafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(gosiKafkaConsumerFactory);
        return factory;
    }
    
    @Bean
    public GosiKafkaSpringConsumerInterceptor<Object, Object> gosiKafkaSpringConsumerInterceptor() {
        return new GosiKafkaSpringConsumerInterceptor<>();
    }
}
