package com.gosi.kafka.sdk.naming;

import java.util.regex.Pattern;

/**
 * Validates and builds topic names according to the organizational naming convention.
 * <p>
 * Standard topics: {@code <namespace>.<topic_name>.<v(n)>}<br>
 * DLQ topics: {@code <namespace>.dlq.<stage>.<v(n)>}<br>
 * <ul>
 *   <li>All lowercase</li>
 *   <li>Dot-separated</li>
 *   <li>Max 200 characters</li>
 *   <li>Never starts with {@code __} (reserved for internal topics)</li>
 *   <li>Only alphanumeric characters, dots, hyphens, and underscores allowed</li>
 * </ul>
 * </p>
 */
public final class TopicNamingUtils {

    private static final int MAX_TOPIC_LENGTH = 200;
    private static final Pattern VALID_CHARS = Pattern.compile("^[a-z0-9][a-z0-9._-]*$");
    private static final Pattern STANDARD_TOPIC = Pattern.compile("^[a-z][a-z0-9-]*\\.[a-z][a-z0-9._-]*\\.v[0-9]+$");
    private static final Pattern DLQ_TOPIC = Pattern.compile("^[a-z][a-z0-9-]*\\.dlq\\.[a-z][a-z0-9-]*\\.v[0-9]+$");

    private TopicNamingUtils() {
        // Utility class
    }

    /**
     * Validates a standard topic name against the organizational naming convention.
     * Format: {@code <namespace>.<topic_name>.<v(n)>}
     *
     * @param topicName the full topic name to validate
     * @throws TopicNamingException if the name violates the convention
     */
    public static void validateTopicName(String topicName) {
        validateCommon(topicName);

        if (!STANDARD_TOPIC.matcher(topicName).matches()) {
            throw new TopicNamingException(topicName,
                    "Must follow pattern <namespace>.<topic_name>.<v(n)>, e.g. 'hrsd.employee-events.v1'");
        }
    }

    /**
     * Validates a DLQ topic name against the organizational naming convention.
     * Format: {@code <namespace>.dlq.<stage>.<v(n)>}
     *
     * @param dlqTopicName the DLQ topic name to validate
     * @throws TopicNamingException if the name violates the convention
     */
    public static void validateDlqTopic(String dlqTopicName) {
        validateCommon(dlqTopicName);

        if (!DLQ_TOPIC.matcher(dlqTopicName).matches()) {
            throw new TopicNamingException(dlqTopicName,
                    "Must follow pattern <namespace>.dlq.<stage>.<v(n)>, e.g. 'hrsd.dlq.enrichment.v1'");
        }
    }

    /**
     * Builds a DLQ topic name from namespace and processing stage.
     * Result: {@code <namespace>.dlq.<stage>.v1}
     *
     * @param namespace the organizational namespace (e.g. "hrsd", "medallia")
     * @param stage     the processing stage (e.g. "ingestion", "enrichment", "extraction")
     * @return the formatted DLQ topic name
     * @throws TopicNamingException if the resulting name violates conventions
     */
    public static String buildDlqTopic(String namespace, String stage) {
        return buildDlqTopic(namespace, stage, 1);
    }

    /**
     * Builds a DLQ topic name from namespace, processing stage, and version.
     * Result: {@code <namespace>.dlq.<stage>.v<version>}
     *
     * @param namespace the organizational namespace
     * @param stage     the processing stage
     * @param version   the topic version number
     * @return the formatted DLQ topic name
     * @throws TopicNamingException if the resulting name violates conventions
     */
    public static String buildDlqTopic(String namespace, String stage, int version) {
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace cannot be null or empty");
        }
        if (stage == null || stage.trim().isEmpty()) {
            throw new IllegalArgumentException("Stage cannot be null or empty");
        }
        if (version < 1) {
            throw new IllegalArgumentException("Version must be >= 1");
        }

        String topicName = namespace.toLowerCase() + ".dlq." + stage.toLowerCase() + ".v" + version;
        validateDlqTopic(topicName);
        return topicName;
    }

    /**
     * Builds a standard topic name from namespace, name, and version.
     * Result: {@code <namespace>.<topic_name>.v<version>}
     *
     * @param namespace the organizational namespace
     * @param name      the topic name
     * @param version   the topic version number
     * @return the formatted topic name
     * @throws TopicNamingException if the resulting name violates conventions
     */
    public static String buildTopicName(String namespace, String name, int version) {
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("Namespace cannot be null or empty");
        }
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (version < 1) {
            throw new IllegalArgumentException("Version must be >= 1");
        }

        String topicName = namespace.toLowerCase() + "." + name.toLowerCase() + ".v" + version;
        validateTopicName(topicName);
        return topicName;
    }

    /**
     * Checks if a topic name is valid without throwing an exception.
     *
     * @param topicName the topic name to check
     * @return true if valid, false otherwise
     */
    public static boolean isValidTopicName(String topicName) {
        try {
            validateTopicName(topicName);
            return true;
        } catch (TopicNamingException | IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Checks if a DLQ topic name is valid without throwing an exception.
     *
     * @param dlqTopicName the DLQ topic name to check
     * @return true if valid, false otherwise
     */
    public static boolean isValidDlqTopic(String dlqTopicName) {
        try {
            validateDlqTopic(dlqTopicName);
            return true;
        } catch (TopicNamingException | IllegalArgumentException e) {
            return false;
        }
    }

    private static void validateCommon(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new TopicNamingException(topicName != null ? topicName : "<null>", "Topic name cannot be null or empty");
        }

        if (topicName.length() > MAX_TOPIC_LENGTH) {
            throw new TopicNamingException(topicName,
                    "Topic name exceeds maximum length of " + MAX_TOPIC_LENGTH + " characters (actual: " + topicName.length() + ")");
        }

        if (topicName.startsWith("__")) {
            throw new TopicNamingException(topicName,
                    "Topic name must not start with '__' (reserved for Kafka internal topics)");
        }

        if (!topicName.equals(topicName.toLowerCase())) {
            throw new TopicNamingException(topicName, "Topic name must be all lowercase");
        }

        if (!VALID_CHARS.matcher(topicName).matches()) {
            throw new TopicNamingException(topicName,
                    "Topic name contains invalid characters. Only lowercase alphanumeric, dots, hyphens, and underscores are allowed");
        }
    }
}
