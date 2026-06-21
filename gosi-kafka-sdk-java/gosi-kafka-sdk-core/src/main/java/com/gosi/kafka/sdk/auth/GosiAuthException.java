package com.gosi.kafka.sdk.auth;

/**
 * Typed exception that wraps a Kafka security error with a classified {@link AuthErrorType}.
 * Enables consumers and producers to handle authentication vs. authorization failures
 * with distinct remediation logic.
 */
public class GosiAuthException extends RuntimeException {

    private final AuthErrorType errorType;

    public GosiAuthException(AuthErrorType errorType, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
    }

    public GosiAuthException(AuthErrorType errorType, String message) {
        super(message);
        this.errorType = errorType;
    }

    /**
     * Returns the classified error type.
     */
    public AuthErrorType getErrorType() {
        return errorType;
    }

    /**
     * Returns true if this is an authorization denial (ACL/RBAC miss) rather than
     * an authentication failure (bad credentials/expired token).
     */
    public boolean isAuthorizationDenied() {
        return errorType == AuthErrorType.AUTHORIZATION_DENIED;
    }

    /**
     * Returns true if this is an authentication failure (SASL handshake, JWT validation).
     */
    public boolean isAuthenticationFailure() {
        return errorType == AuthErrorType.AUTHENTICATION_FAILURE;
    }
}
