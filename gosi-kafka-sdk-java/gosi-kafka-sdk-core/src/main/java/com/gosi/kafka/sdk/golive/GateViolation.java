package com.gosi.kafka.sdk.golive;

/**
 * Represents a single go-live gate violation detected by the {@link GoLiveGateChecker}.
 */
public class GateViolation {

    public enum Severity {
        /** Must be fixed before go-live. */
        BLOCKING,
        /** Should be fixed but not a hard blocker. */
        WARNING
    }

    private final String checkName;
    private final String description;
    private final Severity severity;

    public GateViolation(String checkName, String description, Severity severity) {
        this.checkName = checkName;
        this.description = description;
        this.severity = severity;
    }

    /** Name of the check that failed (e.g. "DLQ_NAMING_COMPLIANCE"). */
    public String getCheckName() { return checkName; }

    /** Human-readable description of the violation. */
    public String getDescription() { return description; }

    /** Severity of the violation. */
    public Severity getSeverity() { return severity; }

    @Override
    public String toString() {
        return "[" + severity + "] " + checkName + ": " + description;
    }
}
