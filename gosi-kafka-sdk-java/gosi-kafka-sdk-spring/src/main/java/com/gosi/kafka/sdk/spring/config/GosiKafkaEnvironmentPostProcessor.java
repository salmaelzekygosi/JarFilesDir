package com.gosi.kafka.sdk.spring.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.HashMap;
import java.util.Map;

public class GosiKafkaEnvironmentPostProcessor implements EnvironmentPostProcessor {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        if (!environment.getPropertySources().contains("gosiKafkaLoggingDefaults")) {
            Map<String, Object> map = new HashMap<>();
            // Inject a default log format for observability (allows override in app config)
            map.put("logging.pattern.console", "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} [TraceID: %X{trace_id}] - %msg%n");
            
            environment.getPropertySources().addLast(new MapPropertySource("gosiKafkaLoggingDefaults", map));
        }
    }
}
