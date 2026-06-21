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

    private static final String OAUTHBEARER = "OAUTHBEARER";
    private static final String SASL_SSL = "SASL_SSL";
    private static final String SSL = "SSL";

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
            boolean useTls = SASL_SSL.equalsIgnoreCase(properties.getSecurityProtocol());
            AuthenticationHandler authHandler = null;

            switch (properties.getSaslMechanism().toUpperCase()) {
                case "PLAIN":
                    authHandler = new SaslPlainAuthHandler(properties.getUsername(), properties.getPassword(), useTls);
                    break;
                case OAUTHBEARER:
                    String scope = properties.getOauthScope() != null ? properties.getOauthScope() : "write";
                    authHandler = new OAuthBearerAuthHandler(properties.getOauthTokenUrl(), properties.getUsername(), properties.getPassword(), scope, useTls);
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

        // Apply resilience config to the client builder if populated
        GosiKafkaProperties.ResilienceProperties resProps = properties.getResilience();
        if (resProps.getNamespace() != null && resProps.getStage() != null) {
            com.gosi.kafka.sdk.resilience.ResilienceConfig resConfig = com.gosi.kafka.sdk.resilience.ResilienceConfig.builder()
                .namespace(resProps.getNamespace())
                .processingStage(resProps.getStage())
                .errorPolicy(com.gosi.kafka.sdk.resilience.ErrorPolicy.valueOf(resProps.getErrorPolicy().toUpperCase()))
                .maxRetries(resProps.getMaxRetries())
                .retryBackoffMs(resProps.getRetryBackoffMs())
                .dlqAccumulationAlertThreshold(resProps.getDlqAccumulationAlertThreshold())
                .restartLoopThreshold(resProps.getRestartLoopThreshold())
                .restartLoopWindowMs(resProps.getRestartLoopWindowMs())
                .build();
            builder.resilienceConfig(resConfig);
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
            applySchemaRegistryProps(props, properties);
        }

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("specific.avro.reader", "true");
        
        // Inject SDK Consumer Interceptor
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, 
                  GosiKafkaSpringConsumerInterceptor.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    private void applySchemaRegistryProps(Map<String, Object> props, GosiKafkaProperties properties) {
        if (SASL_SSL.equalsIgnoreCase(properties.getSecurityProtocol()) || SSL.equalsIgnoreCase(properties.getSecurityProtocol())) {
            if (properties.getTruststoreLocation() != null) {
                props.put("schema.registry.ssl.truststore.location", properties.getTruststoreLocation());
                props.put("schema.registry.ssl.truststore.password", properties.getTruststorePassword());
                props.put("schema.registry.ssl.truststore.type", "JKS");
            }
            if (properties.getSaslMechanism() != null && OAUTHBEARER.equalsIgnoreCase(properties.getSaslMechanism())) {
                props.put("schema.registry.bearer.auth.credentials.source", OAUTHBEARER);
                props.put("schema.registry.bearer.auth.issuer.endpoint.url", properties.getOauthTokenUrl());
                props.put("schema.registry.bearer.auth.client.id", properties.getUsername());
                props.put("schema.registry.bearer.auth.client.secret", properties.getPassword());
                props.put("schema.registry.bearer.auth.scope", properties.getOauthScope() != null ? properties.getOauthScope() : "write");
            }
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> gosiKafkaConsumerFactory,
            org.springframework.beans.factory.ObjectProvider<GosiKafkaSpringDlqErrorHandler<Object, Object>> errorHandlerProvider) {
            
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(gosiKafkaConsumerFactory);
        
        errorHandlerProvider.ifAvailable(factory::setCommonErrorHandler);
        
        return factory;
    }
    
    @Bean
    @ConditionalOnMissingBean
    public GosiKafkaSpringDlqErrorHandler<Object, Object> gosiKafkaSpringDlqErrorHandler(
            GosiKafkaClientConfig config,
            GosiKafkaProducer<Object, Object> dlqProducer,
            org.springframework.beans.factory.ObjectProvider<com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter> reporterProvider) {
            
        if (config.getResilienceConfig() != null) {
            com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter reporter = reporterProvider.getIfAvailable(com.gosi.kafka.sdk.telemetry.Slf4jTelemetryReporter::new);
            return new GosiKafkaSpringDlqErrorHandler<>(config.getResilienceConfig(), dlqProducer, reporter);
        }
        return null;
    }

    @Bean
    public GosiKafkaSpringConsumerInterceptor<Object, Object> gosiKafkaSpringConsumerInterceptor() {
        return new GosiKafkaSpringConsumerInterceptor<>();
    }
}
