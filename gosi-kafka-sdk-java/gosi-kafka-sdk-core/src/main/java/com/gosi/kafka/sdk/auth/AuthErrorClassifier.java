package com.gosi.kafka.sdk.auth;

import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.DelegationTokenAuthorizationException;

/**
 * Classifies Kafka exceptions into distinct auth error types.
 * <p>
 * The Enterprise Event Streaming Framework requires that the SDK surface
 * authorization denials distinctly from authentication failures. Broker-side
 * authorization is two-tier (Kafka ACLs first, RBAC via Metadata Service on a miss)
 * — the SDK does not implement this, but must report the failure class correctly.
 * </p>
 */
public final class AuthErrorClassifier {

    private AuthErrorClassifier() {
        // Utility class
    }

    /**
     * Classifies a Throwable into an {@link AuthErrorType}.
     *
     * @param throwable the exception to classify
     * @return the classified error type
     */
    public static AuthErrorType classify(Throwable throwable) {
        if (throwable == null) {
            return AuthErrorType.UNKNOWN;
        }

        // Direct match
        AuthErrorType type = classifyDirect(throwable);
        if (type != AuthErrorType.UNKNOWN) {
            return type;
        }

        // Check cause chain
        Throwable cause = throwable.getCause();
        int depth = 0;
        while (cause != null && depth < 10) {
            type = classifyDirect(cause);
            if (type != AuthErrorType.UNKNOWN) {
                return type;
            }
            cause = cause.getCause();
            depth++;
        }

        return AuthErrorType.UNKNOWN;
    }

    /**
     * Wraps the given throwable as a {@link GosiAuthException} with the classified type.
     *
     * @param throwable the original exception
     * @return a GosiAuthException wrapping the original error
     */
    public static GosiAuthException classifyAndWrap(Throwable throwable) {
        AuthErrorType type = classify(throwable);
        String message = buildMessage(type, throwable);
        return new GosiAuthException(type, message, throwable);
    }

    /**
     * Returns true if the given throwable represents a security-related error
     * (either authentication or authorization).
     */
    public static boolean isSecurityError(Throwable throwable) {
        AuthErrorType type = classify(throwable);
        return type == AuthErrorType.AUTHENTICATION_FAILURE || type == AuthErrorType.AUTHORIZATION_DENIED;
    }

    private static AuthErrorType classifyDirect(Throwable throwable) {
        // Authentication failures — SASL handshake, JWT validation
        if (throwable instanceof SaslAuthenticationException) {
            return AuthErrorType.AUTHENTICATION_FAILURE;
        }

        // Authorization denials — ACL/RBAC evaluated by broker
        if (throwable instanceof TopicAuthorizationException) {
            return AuthErrorType.AUTHORIZATION_DENIED;
        }
        if (throwable instanceof GroupAuthorizationException) {
            return AuthErrorType.AUTHORIZATION_DENIED;
        }
        if (throwable instanceof ClusterAuthorizationException) {
            return AuthErrorType.AUTHORIZATION_DENIED;
        }
        if (throwable instanceof TransactionalIdAuthorizationException) {
            return AuthErrorType.AUTHORIZATION_DENIED;
        }
        if (throwable instanceof DelegationTokenAuthorizationException) {
            return AuthErrorType.AUTHORIZATION_DENIED;
        }

        // Heuristic: check error message for auth-related keywords
        String message = throwable.getMessage();
        if (message != null) {
            String lowerMessage = message.toLowerCase();
            if (lowerMessage.contains("authentication failed") || lowerMessage.contains("sasl authentication")) {
                return AuthErrorType.AUTHENTICATION_FAILURE;
            }
            if (lowerMessage.contains("not authorized") || lowerMessage.contains("authorization failed")) {
                return AuthErrorType.AUTHORIZATION_DENIED;
            }
        }

        return AuthErrorType.UNKNOWN;
    }

    private static String buildMessage(AuthErrorType type, Throwable throwable) {
        String originalMessage = throwable != null ? throwable.getMessage() : "unknown";
        switch (type) {
            case AUTHENTICATION_FAILURE:
                return "Authentication failure: " + originalMessage;
            case AUTHORIZATION_DENIED:
                return "Authorization denied (ACL/RBAC): " + originalMessage;
            case TRANSIENT_ERROR:
                return "Transient error: " + originalMessage;
            default:
                return "Unclassified error: " + originalMessage;
        }
    }
}
