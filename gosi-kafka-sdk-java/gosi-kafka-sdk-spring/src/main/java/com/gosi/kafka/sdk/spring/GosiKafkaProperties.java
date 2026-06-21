package com.gosi.kafka.sdk.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gosi.kafka")
public class GosiKafkaProperties {

    private String bootstrapServers;
    private String schemaRegistryUrl;
    private String clientId;
    private String groupId;
    
    // Auth properties
    private String saslMechanism; // PLAIN, OAUTHBEARER, SCRAM-SHA-512
    private String securityProtocol = "SASL_PLAINTEXT";
    private String username;
    private String password;
    
    // OAuth specific
    private String oauthTokenUrl;
    private String oauthScope;
    
    // mTLS specific
    private String keystoreLocation;
    private String keystorePassword;
    private String truststoreLocation;
    private String truststorePassword;

    // Getters and Setters
    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getSchemaRegistryUrl() { return schemaRegistryUrl; }
    public void setSchemaRegistryUrl(String schemaRegistryUrl) { this.schemaRegistryUrl = schemaRegistryUrl; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public String getSaslMechanism() { return saslMechanism; }
    public void setSaslMechanism(String saslMechanism) { this.saslMechanism = saslMechanism; }

    public String getSecurityProtocol() { return securityProtocol; }
    public void setSecurityProtocol(String securityProtocol) { this.securityProtocol = securityProtocol; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public String getOauthTokenUrl() { return oauthTokenUrl; }
    public void setOauthTokenUrl(String oauthTokenUrl) { this.oauthTokenUrl = oauthTokenUrl; }

    public String getOauthScope() { return oauthScope; }
    public void setOauthScope(String oauthScope) { this.oauthScope = oauthScope; }

    public String getKeystoreLocation() { return keystoreLocation; }
    public void setKeystoreLocation(String keystoreLocation) { this.keystoreLocation = keystoreLocation; }

    public String getKeystorePassword() { return keystorePassword; }
    public void setKeystorePassword(String keystorePassword) { this.keystorePassword = keystorePassword; }

    public String getTruststoreLocation() { return truststoreLocation; }
    public void setTruststoreLocation(String truststoreLocation) { this.truststoreLocation = truststoreLocation; }

    public String getTruststorePassword() { return truststorePassword; }
    public void setTruststorePassword(String truststorePassword) { this.truststorePassword = truststorePassword; }

    private java.util.Map<String, String> properties = new java.util.HashMap<>();
    public java.util.Map<String, String> getProperties() { return properties; }
    public void setProperties(java.util.Map<String, String> properties) { this.properties = properties; }

    private ResilienceProperties resilience = new ResilienceProperties();
    public ResilienceProperties getResilience() { return resilience; }
    public void setResilience(ResilienceProperties resilience) { this.resilience = resilience; }

    public static class ResilienceProperties {
        private String namespace;
        private String stage;
        private String errorPolicy = "CAPTURE_DLQ";
        private int maxRetries = 3;
        private long retryBackoffMs = 1000;
        private long dlqAccumulationAlertThreshold = 100;
        private int restartLoopThreshold = 3;
        private long restartLoopWindowMs = 600000;

        public String getNamespace() { return namespace; }
        public void setNamespace(String namespace) { this.namespace = namespace; }

        public String getStage() { return stage; }
        public void setStage(String stage) { this.stage = stage; }

        public String getErrorPolicy() { return errorPolicy; }
        public void setErrorPolicy(String errorPolicy) { this.errorPolicy = errorPolicy; }

        public int getMaxRetries() { return maxRetries; }
        public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

        public long getRetryBackoffMs() { return retryBackoffMs; }
        public void setRetryBackoffMs(long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; }

        public long getDlqAccumulationAlertThreshold() { return dlqAccumulationAlertThreshold; }
        public void setDlqAccumulationAlertThreshold(long dlqAccumulationAlertThreshold) { this.dlqAccumulationAlertThreshold = dlqAccumulationAlertThreshold; }

        public int getRestartLoopThreshold() { return restartLoopThreshold; }
        public void setRestartLoopThreshold(int restartLoopThreshold) { this.restartLoopThreshold = restartLoopThreshold; }

        public long getRestartLoopWindowMs() { return restartLoopWindowMs; }
        public void setRestartLoopWindowMs(long restartLoopWindowMs) { this.restartLoopWindowMs = restartLoopWindowMs; }
    }
}
