package com.gosi.kafka.sdk.quarkus.resilience;

import com.gosi.kafka.sdk.resilience.DefaultResilienceWrapper;
import com.gosi.kafka.sdk.resilience.ErrorPolicy;
import com.gosi.kafka.sdk.resilience.ResilienceConfig;
import com.gosi.kafka.sdk.resilience.ResilienceWrapper;
import com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.Optional;

@ApplicationScoped
public class GosiKafkaQuarkusResilienceProducer {

    @ConfigProperty(name = "gosi.kafka.resilience.namespace")
    Optional<String> namespace;

    @ConfigProperty(name = "gosi.kafka.resilience.stage")
    Optional<String> stage;

    @ConfigProperty(name = "gosi.kafka.resilience.error-policy", defaultValue = "CAPTURE_DLQ")
    String errorPolicy;

    @ConfigProperty(name = "gosi.kafka.resilience.max-retries", defaultValue = "3")
    int maxRetries;

    @ConfigProperty(name = "gosi.kafka.resilience.retry-backoff-ms", defaultValue = "1000")
    long retryBackoffMs;

    @ConfigProperty(name = "gosi.kafka.resilience.dlq-accumulation-alert-threshold", defaultValue = "100")
    long dlqAccumulationAlertThreshold;

    @ConfigProperty(name = "gosi.kafka.resilience.restart-loop-threshold", defaultValue = "3")
    int restartLoopThreshold;

    @ConfigProperty(name = "gosi.kafka.resilience.restart-loop-window-ms", defaultValue = "600000")
    long restartLoopWindowMs;

    // SmallRye Kafka incoming channel name is typically the topic
    // So the source topic must be injected or configured per consumer
    // For a generic bean, the application might configure it dynamically.
    // Here we produce a generic config builder, but for the actual ResilienceWrapper,
    // the source topic is required. We'll use a placeholder or expect it to be supplied 
    // by the application when it instantiates the wrapper.

    @Produces
    @Singleton
    public ResilienceConfig.Builder resilienceConfigBuilder() {
        ResilienceConfig.Builder builder = ResilienceConfig.builder()
                .errorPolicy(ErrorPolicy.valueOf(errorPolicy.toUpperCase()))
                .maxRetries(maxRetries)
                .retryBackoffMs(retryBackoffMs)
                .dlqAccumulationAlertThreshold(dlqAccumulationAlertThreshold)
                .restartLoopThreshold(restartLoopThreshold)
                .restartLoopWindowMs(restartLoopWindowMs);

        namespace.ifPresent(builder::namespace);
        stage.ifPresent(builder::processingStage);

        return builder;
    }

    /**
     * Produces a fully formed ResilienceConfig if namespace and stage are provided.
     */
    @Produces
    @Singleton
    public ResilienceConfig resilienceConfig(ResilienceConfig.Builder builder) {
        return builder.build();
    }

    /**
     * Produces a DefaultResilienceWrapper.
     */
    @Produces
    @ApplicationScoped
    public <K, V> ResilienceWrapper<K, V> resilienceWrapper(
            ResilienceConfig resilienceConfig,
            com.gosi.kafka.sdk.producer.GosiKafkaProducer<K, V> dlqProducer,
            GosiTelemetryReporter telemetryReporter) {
        
        return new DefaultResilienceWrapper<>(resilienceConfig, dlqProducer, telemetryReporter);
    }
}
