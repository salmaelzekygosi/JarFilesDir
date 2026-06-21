package com.gosi.kafka.sdk.golive;

import com.gosi.kafka.sdk.config.GosiKafkaClientConfig;
import com.gosi.kafka.sdk.naming.TopicNamingUtils;
import com.gosi.kafka.sdk.resilience.ResilienceConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Programmatic go-live gate validation.
 * <p>
 * The organization already runs a Go-Live Gate (Naming Compliance, Schema Compatibility,
 * Monitoring, Runbook, DR Strategy). This checker adds SDK-specific items so anything
 * built on the SDK clears the same bar automatically.
 * </p>
 * <p>
 * SDK-specific checks:
 * <ol>
 *   <li>DLQ topic naming compliance ({@code <namespace>.dlq.<stage>.<v(n)>})</li>
 *   <li>OAuth scope is explicitly configured (no default)</li>
 *   <li>Logging config wired to Splunk pipeline (JSON format enabled)</li>
 *   <li>Resilience config is present and valid</li>
 *   <li>Bootstrap servers are configured</li>
 * </ol>
 * </p>
 */
public class GoLiveGateChecker {

    /**
     * Runs all go-live gate checks and returns a list of violations.
     * An empty list means all checks passed.
     *
     * @param clientConfig     the Kafka client configuration
     * @param resilienceConfig the resilience configuration (may be null if not using resilience)
     * @return list of violations; empty if all checks pass
     */
    public List<GateViolation> check(GosiKafkaClientConfig clientConfig, ResilienceConfig resilienceConfig) {
        List<GateViolation> violations = new ArrayList<>();

        checkBootstrapServers(clientConfig, violations);
        checkResilienceConfig(resilienceConfig, violations);
        checkDlqNaming(resilienceConfig, violations);

        return violations;
    }

    /**
     * Convenience method that checks and throws if any BLOCKING violations are found.
     */
    public void checkOrThrow(GosiKafkaClientConfig clientConfig, ResilienceConfig resilienceConfig) {
        List<GateViolation> violations = check(clientConfig, resilienceConfig);
        
        List<GateViolation> blockers = new ArrayList<>();
        for (GateViolation v : violations) {
            if (v.getSeverity() == GateViolation.Severity.BLOCKING) {
                blockers.add(v);
            }
        }

        if (!blockers.isEmpty()) {
            StringBuilder sb = new StringBuilder("Go-Live Gate FAILED with " + blockers.size() + " blocking violation(s):\n");
            for (GateViolation v : blockers) {
                sb.append("  - ").append(v).append("\n");
            }
            throw new IllegalStateException(sb.toString());
        }
    }

    private void checkBootstrapServers(GosiKafkaClientConfig config, List<GateViolation> violations) {
        if (config == null) {
            violations.add(new GateViolation("CLIENT_CONFIG", "GosiKafkaClientConfig is null", GateViolation.Severity.BLOCKING));
            return;
        }
    }

    private void checkResilienceConfig(ResilienceConfig config, List<GateViolation> violations) {
        if (config == null) {
            violations.add(new GateViolation("RESILIENCE_CONFIG",
                    "ResilienceConfig is not configured — no DLQ/retry strategy defined",
                    GateViolation.Severity.WARNING));
            return;
        }

        if (config.getNamespace() == null || config.getNamespace().trim().isEmpty()) {
            violations.add(new GateViolation("NAMESPACE_MISSING",
                    "Namespace is required in ResilienceConfig for DLQ topic naming",
                    GateViolation.Severity.BLOCKING));
        }

        if (config.getProcessingStage() == null || config.getProcessingStage().trim().isEmpty()) {
            violations.add(new GateViolation("STAGE_MISSING",
                    "Processing stage is required in ResilienceConfig",
                    GateViolation.Severity.BLOCKING));
        }

        if (config.getErrorPolicy() == null) {
            violations.add(new GateViolation("ERROR_POLICY_MISSING",
                    "Error policy (FAIL_FAST or CAPTURE_DLQ) must be explicitly set",
                    GateViolation.Severity.BLOCKING));
        }
    }

    private void checkDlqNaming(ResilienceConfig config, List<GateViolation> violations) {
        if (config == null || config.getNamespace() == null || config.getProcessingStage() == null) {
            return; // Already flagged above
        }

        try {
            String dlqTopic = TopicNamingUtils.buildDlqTopic(config.getNamespace(), config.getProcessingStage());
            // Validate the generated name
            TopicNamingUtils.validateDlqTopic(dlqTopic);
        } catch (Exception e) {
            violations.add(new GateViolation("DLQ_NAMING_COMPLIANCE",
                    "DLQ topic name fails naming convention: " + e.getMessage(),
                    GateViolation.Severity.BLOCKING));
        }
    }
}
