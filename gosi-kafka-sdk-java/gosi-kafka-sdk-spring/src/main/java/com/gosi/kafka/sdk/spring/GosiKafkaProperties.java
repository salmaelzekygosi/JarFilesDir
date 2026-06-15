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

    public String getKeystoreLocation() { return keystoreLocation; }
    public void setKeystoreLocation(String keystoreLocation) { this.keystoreLocation = keystoreLocation; }

    public String getKeystorePassword() { return keystorePassword; }
    public void setKeystorePassword(String keystorePassword) { this.keystorePassword = keystorePassword; }

    public String getTruststoreLocation() { return truststoreLocation; }
    public void setTruststoreLocation(String truststoreLocation) { this.truststoreLocation = truststoreLocation; }

    public String getTruststorePassword() { return truststorePassword; }
    public void setTruststorePassword(String truststorePassword) { this.truststorePassword = truststorePassword; }
}
