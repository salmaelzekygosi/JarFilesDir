package com.gosi.kafka.sdk.naming;

/**
 * Thrown when a topic name violates the organizational naming convention.
 * <p>
 * Standard topic naming: {@code <namespace>.<topic_name>.<v(n)>}<br>
 * DLQ topic naming: {@code <namespace>.dlq.<stage>.<v(n)>}<br>
 * All lowercase, dot-separated, max 200 characters, never starting with {@code __}.
 * </p>
 */
public class TopicNamingException extends RuntimeException {

    private final String topicName;
    private final String violationReason;

    public TopicNamingException(String topicName, String violationReason) {
        super("Topic naming violation for '" + topicName + "': " + violationReason);
        this.topicName = topicName;
        this.violationReason = violationReason;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getViolationReason() {
        return violationReason;
    }
}
