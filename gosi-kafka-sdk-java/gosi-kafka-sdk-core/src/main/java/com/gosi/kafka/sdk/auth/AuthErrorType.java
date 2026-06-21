package com.gosi.kafka.sdk.auth;

/**
 * Classifies Kafka security errors into distinct failure classes.
 * The Enterprise Event Streaming Framework requires that authorization denials
 * are surfaced distinctly from authentication failures — they are different
 * failure classes with different remediation paths.
 */
public enum AuthErrorType {

    /**
     * SASL handshake or JWT validation failed.
     * Root cause: invalid/expired credentials, IDP unreachable, token signature mismatch.
     */
    AUTHENTICATION_FAILURE,

    /**
     * Broker-side ACL or RBAC check denied the operation.
     * Root cause: missing ACL entry, insufficient RBAC role via MDS.
     */
    AUTHORIZATION_DENIED,

    /**
     * Transient network/broker error that may resolve on retry.
     */
    TRANSIENT_ERROR,

    /**
     * Error that could not be classified into the above categories.
     */
    UNKNOWN
}
