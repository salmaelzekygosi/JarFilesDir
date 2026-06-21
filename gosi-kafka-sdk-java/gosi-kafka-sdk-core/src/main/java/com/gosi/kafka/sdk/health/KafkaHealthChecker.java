package com.gosi.kafka.sdk.health;

import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Provides proactive connection health checks for the Kafka cluster.
 * <p>
 * Uses the Kafka {@link AdminClient} to ping the cluster and verify connectivity.
 * This is designed to be wired into framework-specific health endpoints
 * (e.g., Spring Boot Actuator, Quarkus SmallRye Health).
 * </p>
 */
public class KafkaHealthChecker {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaHealthChecker.class);

    private final Map<String, Object> adminProps;
    private final Duration defaultTimeout;

    public KafkaHealthChecker(GosiKafkaClientConfig clientConfig) {
        this(clientConfig, Duration.ofSeconds(5));
    }

    public KafkaHealthChecker(GosiKafkaClientConfig clientConfig, Duration defaultTimeout) {
        // AdminClient uses the same base properties as producers/consumers
        // (bootstrap servers, security protocol, SASL mechanism, etc.)
        this.adminProps = clientConfig.buildProducerProperties();
        this.defaultTimeout = defaultTimeout;
    }

    /**
     * Pings the Kafka cluster to check connectivity.
     *
     * @return true if the cluster is reachable, false otherwise.
     */
    public boolean isHealthy() {
        return checkHealth().isUp();
    }

    /**
     * Performs a detailed health check, returning status and latency.
     *
     * @return HealthStatus containing UP/DOWN state, latency, and any error message.
     */
    public HealthStatus checkHealth() {
        long start = System.currentTimeMillis();
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            DescribeClusterResult result = adminClient.describeCluster();
            
            // Getting the cluster ID requires an actual round-trip to the broker
            String clusterId = result.clusterId().get(defaultTimeout.toMillis(), TimeUnit.MILLISECONDS);
            int nodeCount = result.nodes().get(defaultTimeout.toMillis(), TimeUnit.MILLISECONDS).size();
            
            long latency = System.currentTimeMillis() - start;
            
            LOG.debug("Kafka health check passed | clusterId={} | nodes={} | latency={}ms", 
                    clusterId, nodeCount, latency);
                    
            return HealthStatus.up(latency, "Connected to cluster: " + clusterId + " with " + nodeCount + " nodes");
            
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - start;
            LOG.warn("Kafka health check failed | latency={}ms | error={}", latency, e.getMessage());
            return HealthStatus.down(latency, e.getMessage());
        }
    }

    /**
     * Represents the result of a health check ping.
     */
    public static class HealthStatus {
        private final boolean up;
        private final long latencyMs;
        private final String details;

        private HealthStatus(boolean up, long latencyMs, String details) {
            this.up = up;
            this.latencyMs = latencyMs;
            this.details = details;
        }

        public boolean isUp() { return up; }
        public long getLatencyMs() { return latencyMs; }
        public String getDetails() { return details; }

        public static HealthStatus up(long latencyMs, String details) {
            return new HealthStatus(true, latencyMs, details);
        }

        public static HealthStatus down(long latencyMs, String details) {
            return new HealthStatus(false, latencyMs, details);
        }
    }
}
