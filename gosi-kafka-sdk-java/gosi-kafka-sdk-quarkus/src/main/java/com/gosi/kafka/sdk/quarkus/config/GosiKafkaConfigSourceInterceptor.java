package com.gosi.kafka.sdk.quarkus.config;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;

public class GosiKafkaConfigSourceInterceptor implements ConfigSourceInterceptor {

    private static final String GOSI_KAFKA_SASL_MECHANISM = "gosi.kafka.sasl-mechanism";
    private static final String GOSI_KAFKA_OAUTH_TOKEN_URL = "gosi.kafka.oauth-token-url";
    private static final String GOSI_KAFKA_USERNAME = "gosi.kafka.username";
    private static final String GOSI_KAFKA_PASSWORD = "gosi.kafka.password";
    private static final String GOSI_KAFKA_TRUSTSTORE_LOCATION = "gosi.kafka.truststore-location";
    private static final String OAUTHBEARER = "OAUTHBEARER";

    @Override
    public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
        ConfigValue val = mapGlobalKafkaProperties(context, name);
        if (val != null) return val;

        val = mapOAuthBearerProperties(context, name);
        if (val != null) return val;

        val = mapTlsProperties(context, name);
        if (val != null) return val;

        val = mapSchemaRegistryProperties(context, name);
        if (val != null) return val;

        val = mapInterceptorProperties(name);
        if (val != null) return val;

        return context.proceed(name);
    }

    private ConfigValue mapGlobalKafkaProperties(ConfigSourceInterceptorContext context, String name) {
        if ("kafka.bootstrap.servers".equals(name)) {
            return getValidConfigValue(context, name, "gosi.kafka.bootstrap-servers");
        }
        if ("kafka.security.protocol".equals(name)) {
            return getValidConfigValue(context, name, "gosi.kafka.security-protocol");
        }
        if ("kafka.sasl.mechanism".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_SASL_MECHANISM);
        }
        return null;
    }

    private ConfigValue mapOAuthBearerProperties(ConfigSourceInterceptorContext context, String name) {
        if ("kafka.sasl.login.callback.handler.class".equals(name) && isOAuthBearer(context)) {
            return gosiValWithValue(name, "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler");
        }
        if ("kafka.sasl.oauthbearer.token.endpoint.url".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_OAUTH_TOKEN_URL);
        }
        if ("kafka.sasl.jaas.config".equals(name) && isOAuthBearer(context)) {
            ConfigValue user = context.proceed(GOSI_KAFKA_USERNAME);
            ConfigValue pass = context.proceed(GOSI_KAFKA_PASSWORD);
            if (isValid(user) && isValid(pass)) {
                String jaas = String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId=\"%s\" clientSecret=\"%s\" scope=\"write\";", user.getValue(), pass.getValue());
                return gosiValWithValue(name, jaas);
            }
        }
        return null;
    }

    private ConfigValue mapTlsProperties(ConfigSourceInterceptorContext context, String name) {
        if ("kafka.ssl.truststore.location".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_TRUSTSTORE_LOCATION);
        }
        if ("kafka.ssl.truststore.password".equals(name)) {
            return getValidConfigValue(context, name, "gosi.kafka.truststore-password");
        }
        if ("kafka.ssl.truststore.type".equals(name)) {
            ConfigValue gosiVal = context.proceed(GOSI_KAFKA_TRUSTSTORE_LOCATION);
            if (isValid(gosiVal)) return gosiValWithValue(name, "JKS");
        }
        return null;
    }

    private ConfigValue mapSchemaRegistryProperties(ConfigSourceInterceptorContext context, String name) {
        if ("mp.messaging.connector.smallrye-kafka.schema.registry.url".equals(name) || "kafka.schema.registry.url".equals(name)) {
            return getValidConfigValue(context, name, "gosi.kafka.schema-registry-url");
        }
        if ("kafka.bearer.auth.credentials.source".equals(name) && isOAuthBearer(context)) {
            return gosiValWithValue(name, OAUTHBEARER);
        }
        if ("kafka.bearer.auth.issuer.endpoint.url".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_OAUTH_TOKEN_URL);
        }
        if ("kafka.bearer.auth.client.id".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_USERNAME);
        }
        if ("kafka.bearer.auth.client.secret".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_PASSWORD);
        }
        if ("kafka.bearer.auth.scope".equals(name) && isOAuthBearer(context)) {
            return gosiValWithValue(name, "write");
        }
        if ("mp.messaging.connector.smallrye-kafka.schema.registry.ssl.truststore.location".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_TRUSTSTORE_LOCATION);
        }
        if ("mp.messaging.connector.smallrye-kafka.schema.registry.ssl.truststore.password".equals(name)) {
            return getValidConfigValue(context, name, "gosi.kafka.truststore-password");
        }
        if ("mp.messaging.connector.smallrye-kafka.schema.registry.ssl.truststore.type".equals(name)) {
            ConfigValue gosiVal = context.proceed(GOSI_KAFKA_TRUSTSTORE_LOCATION);
            if (isValid(gosiVal)) return gosiValWithValue(name, "JKS");
        }
        return null;
    }

    private ConfigValue mapInterceptorProperties(String name) {
        if (name != null && name.endsWith(".interceptor.classes")) {
            if (name.startsWith("mp.messaging.outgoing.") || name.startsWith("kafka.producer.")) {
                return gosiValWithValue(name, "com.gosi.kafka.sdk.logging.GosiKafkaProducerInterceptor");
            } else if (name.startsWith("mp.messaging.incoming.") || name.startsWith("kafka.consumer.")) {
                return gosiValWithValue(name, "com.gosi.kafka.sdk.logging.GosiKafkaConsumerInterceptor");
            }
        }
        return null;
    }

    private boolean isOAuthBearer(ConfigSourceInterceptorContext context) {
        ConfigValue mech = context.proceed(GOSI_KAFKA_SASL_MECHANISM);
        return mech != null && OAUTHBEARER.equalsIgnoreCase(mech.getValue());
    }

    private boolean isValid(ConfigValue val) {
        return val != null && val.getValue() != null;
    }

    private ConfigValue getValidConfigValue(ConfigSourceInterceptorContext context, String requestedName, String key) {
        ConfigValue val = context.proceed(key);
        if (isValid(val)) {
            return ConfigValue.builder()
                    .withName(requestedName)
                    .withValue(val.getValue())
                    .withConfigSourceName(val.getConfigSourceName() != null ? val.getConfigSourceName() : "GosiKafkaConfigSourceInterceptor")
                    .withConfigSourceOrdinal(val.getConfigSourceOrdinal())
                    .build();
        }
        return null;
    }

    private ConfigValue gosiValWithValue(String name, String value) {
        return ConfigValue.builder()
                .withName(name)
                .withValue(value)
                .withConfigSourceName("GosiKafkaConfigSourceInterceptor")
                .withConfigSourceOrdinal(200)
                .build();
    }
}
