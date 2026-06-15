package com.gosi.kafka.sdk.tracing;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Manages trace_id propagation between Kafka headers and SLF4J MDC.
 */
public class TraceContext {

    public static final String TRACE_ID_KEY = "trace_id";
    // Legacy support for firebase/nabaa connectors that used camelCase
    public static final String TRACE_ID_KEY_LEGACY = "traceId";

    /**
     * Initializes the MDC context from Kafka headers.
     * Generates a new UUID if no trace_id is present.
     * @return The resolved trace ID.
     */
    public static String initFromHeaders(Headers headers) {
        String traceId = extractFromHeaders(headers);
        if (traceId == null || traceId.trim().isEmpty()) {
            traceId = UUID.randomUUID().toString();
        }
        MDC.put(TRACE_ID_KEY, traceId);
        return traceId;
    }

    /**
     * Injects the current MDC trace_id into Kafka headers.
     * Generates and injects a new one if MDC is empty.
     */
    public static String injectIntoHeaders(Headers headers) {
        String traceId = MDC.get(TRACE_ID_KEY);
        if (traceId == null || traceId.trim().isEmpty()) {
            traceId = UUID.randomUUID().toString();
            MDC.put(TRACE_ID_KEY, traceId);
        }
        
        // Remove any existing trace_id headers to avoid duplicates
        headers.remove(TRACE_ID_KEY);
        headers.remove(TRACE_ID_KEY_LEGACY);
        
        // Always inject using the unified standard key
        headers.add(TRACE_ID_KEY, traceId.getBytes(StandardCharsets.UTF_8));
        return traceId;
    }

    /**
     * Attempts to find a trace ID in the given headers, checking both standard and legacy keys.
     */
    public static String extractFromHeaders(Headers headers) {
        if (headers == null) {
            return null;
        }
        
        Header standardHeader = headers.lastHeader(TRACE_ID_KEY);
        if (standardHeader != null && standardHeader.value() != null) {
            return new String(standardHeader.value(), StandardCharsets.UTF_8);
        }
        
        Header legacyHeader = headers.lastHeader(TRACE_ID_KEY_LEGACY);
        if (legacyHeader != null && legacyHeader.value() != null) {
            return new String(legacyHeader.value(), StandardCharsets.UTF_8);
        }
        
        return null;
    }

    /**
     * Returns the current trace ID from MDC.
     */
    public static String current() {
        return MDC.get(TRACE_ID_KEY);
    }

    /**
     * Clears the MDC context.
     */
    public static void clear() {
        MDC.remove(TRACE_ID_KEY);
    }
}
