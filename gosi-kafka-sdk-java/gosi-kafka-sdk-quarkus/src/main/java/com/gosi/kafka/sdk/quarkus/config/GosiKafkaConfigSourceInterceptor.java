package com.gosi.kafka.sdk.quarkus.config;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class GosiKafkaConfigSourceInterceptor implements ConfigSourceInterceptor {

    private static final String GOSI_KAFKA_SASL_MECHANISM = "gosi.kafka.sasl-mechanism";
    private static final String GOSI_KAFKA_OAUTH_TOKEN_URL = "gosi.kafka.oauth-token-url";
    private static final String GOSI_KAFKA_USERNAME = "gosi.kafka.username";
    private static final String GOSI_KAFKA_PASSWORD = "gosi.kafka.password";
    private static final String GOSI_KAFKA_OAUTH_SCOPE = "gosi.kafka.oauth-scope";
    private static final String GOSI_KAFKA_TRUSTSTORE_LOCATION = "gosi.kafka.truststore-location";
    private static final String OAUTHBEARER = "OAUTHBEARER";
    private static final String MP_MESSAGING_OUTGOING = "mp.messaging.outgoing.";
    private static final String MP_MESSAGING_INCOMING = "mp.messaging.incoming.";
    private static final String INTERCEPTOR_CLASSES_SUFFIX = ".interceptor.classes";

    @Override
    public Iterator<String> iterateNames(ConfigSourceInterceptorContext context) {
        Set<String> names = new HashSet<>();
        Iterator<String> originalNames = context.iterateNames();
        while (originalNames.hasNext()) {
            String n = originalNames.next();
            names.add(n);
            if (n.startsWith(MP_MESSAGING_OUTGOING)) {
                String[] parts = n.split("\\.");
                if (parts.length > 3) {
                    names.add(MP_MESSAGING_OUTGOING + parts[3] + INTERCEPTOR_CLASSES_SUFFIX);
                }
            } else if (n.startsWith(MP_MESSAGING_INCOMING)) {
                String[] parts = n.split("\\.");
                if (parts.length > 3) {
                    names.add(MP_MESSAGING_INCOMING + parts[3] + INTERCEPTOR_CLASSES_SUFFIX);
                    names.add(MP_MESSAGING_INCOMING + parts[3] + ".mdc-keys");
                }
            }
        }
        
        // Expose keys that SmallRye Kafka iterates over to build the Client map
        names.add("kafka.bootstrap.servers");
        names.add("kafka.security.protocol");
        names.add("kafka.sasl.mechanism");
        names.add("kafka.sasl.login.callback.handler.class");
        names.add("kafka.sasl.oauthbearer.token.endpoint.url");
        names.add("kafka.sasl.jaas.config");
        names.add("kafka.ssl.truststore.location");
        names.add("kafka.ssl.truststore.password");
        names.add("kafka.ssl.truststore.type");
        names.add("mp.messaging.connector.smallrye-kafka.schema.registry.url");
        names.add("mp.messaging.connector.smallrye-kafka.schema.registry.ssl.truststore.location");
        names.add("mp.messaging.connector.smallrye-kafka.schema.registry.ssl.truststore.password");
        names.add("mp.messaging.connector.smallrye-kafka.schema.registry.ssl.truststore.type");
        names.add("mp.messaging.connector.smallrye-kafka.bearer.auth.credentials.source");
        names.add("mp.messaging.connector.smallrye-kafka.bearer.auth.issuer.endpoint.url");
        names.add("mp.messaging.connector.smallrye-kafka.bearer.auth.client.id");
        names.add("mp.messaging.connector.smallrye-kafka.bearer.auth.client.secret");
        names.add("mp.messaging.connector.smallrye-kafka.bearer.auth.scope");
        
        // Expose default logging format
        names.add("quarkus.log.console.format");
        
        // Let SmallRye query mp.messaging.outgoing.[channel].interceptor.classes dynamically
        // We do not add them to the global names list to avoid 'unused' warnings in Kafka
        return names.iterator();
    }

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

        val = mapObservabilityProperties(context, name);
        if (val != null) return val;

        return context.proceed(name);
    }

    private ConfigValue mapObservabilityProperties(ConfigSourceInterceptorContext context, String name) {
        if ("quarkus.log.console.format".equals(name)) {
            ConfigValue existing = context.proceed(name);
            if (!isValid(existing)) {
                return gosiValWithValue(name, "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.3}] (thread-%t) [TraceID: %X{trace_id}] %s%e%n");
            }
        }
        return null;
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
            ConfigValue scope = context.proceed(GOSI_KAFKA_OAUTH_SCOPE);
            if (isValid(user) && isValid(pass) && isValid(scope)) {
                String jaas = String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId=\"%s\" clientSecret=\"%s\" scope=\"%s\";", user.getValue(), pass.getValue(), scope.getValue());
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
        if ("mp.messaging.connector.smallrye-kafka.schema.registry.url".equals(name)) {
            return getValidConfigValue(context, name, "gosi.kafka.schema-registry-url");
        }
        if ("mp.messaging.connector.smallrye-kafka.bearer.auth.credentials.source".equals(name) && isOAuthBearer(context)) {
            return gosiValWithValue(name, OAUTHBEARER);
        }
        if ("mp.messaging.connector.smallrye-kafka.bearer.auth.issuer.endpoint.url".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_OAUTH_TOKEN_URL);
        }
        if ("mp.messaging.connector.smallrye-kafka.bearer.auth.client.id".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_USERNAME);
        }
        if ("mp.messaging.connector.smallrye-kafka.bearer.auth.client.secret".equals(name)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_PASSWORD);
        }
        if ("mp.messaging.connector.smallrye-kafka.bearer.auth.scope".equals(name) && isOAuthBearer(context)) {
            return getValidConfigValue(context, name, GOSI_KAFKA_OAUTH_SCOPE);
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
        if (name != null) {
            if (name.endsWith(INTERCEPTOR_CLASSES_SUFFIX)) {
                if (name.startsWith(MP_MESSAGING_OUTGOING) || name.startsWith("kafka.producer.")) {
                    return gosiValWithValue(name, "com.gosi.kafka.sdk.logging.GosiKafkaProducerInterceptor");
                } else if (name.startsWith(MP_MESSAGING_INCOMING) || name.startsWith("kafka.consumer.")) {
                    return gosiValWithValue(name, "com.gosi.kafka.sdk.logging.GosiKafkaConsumerInterceptor");
                }
            } else if (name.endsWith(".mdc-keys") && name.startsWith(MP_MESSAGING_INCOMING)) {
                return gosiValWithValue(name, "trace_id");
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
