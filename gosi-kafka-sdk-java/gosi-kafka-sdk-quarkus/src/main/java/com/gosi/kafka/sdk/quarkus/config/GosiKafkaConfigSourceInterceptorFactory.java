package com.gosi.kafka.sdk.quarkus.config;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigSourceInterceptorFactory;

import java.util.OptionalInt;

public class GosiKafkaConfigSourceInterceptorFactory implements ConfigSourceInterceptorFactory {
    
    @Override
    public ConfigSourceInterceptor getInterceptor(ConfigSourceInterceptorContext context) {
        return new GosiKafkaConfigSourceInterceptor();
    }

    @Override
    public OptionalInt getPriority() {
        return OptionalInt.of(200);
    }
}
