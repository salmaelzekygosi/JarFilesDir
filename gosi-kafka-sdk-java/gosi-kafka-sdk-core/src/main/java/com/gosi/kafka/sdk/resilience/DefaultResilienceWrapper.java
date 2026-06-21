package com.gosi.kafka.sdk.resilience;

import com.gosi.kafka.sdk.consumer.GosiRecord;
import com.gosi.kafka.sdk.consumer.RecordHandler;
import com.gosi.kafka.sdk.naming.TopicNamingUtils;
import com.gosi.kafka.sdk.producer.GosiKafkaProducer;
import com.gosi.kafka.sdk.telemetry.GosiTelemetryReporter;
import com.gosi.kafka.sdk.tracing.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of {@link ResilienceWrapper}.
 * <p>
 * Implements per-stage DLQ routing, configurable retry with backoff,
 * restart-loop detection, and metrics emission matching existing dashboard expectations.
 * </p>
 * <p>
 * DLQ message headers are standardized as: {@code stack_trace}, {@code error_code},
 * {@code trace_id} — matching the fields existing Splunk dashboards/alerts use.
 * Additional enrichment headers: {@code processing_stage}, {@code original_topic},
 * {@code original_offset}, {@code retry_count}, {@code failure_timestamp}.
 * </p>
 *
 * @param <K> Kafka record key type
 * @param <V> Kafka record value type
 */
public class DefaultResilienceWrapper<K, V> implements ResilienceWrapper<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultResilienceWrapper.class);

    private final ResilienceConfig config;
    private final GosiKafkaProducer<K, V> dlqProducer;
    private final GosiTelemetryReporter telemetryReporter;
    private final String dlqTopicName;

    // Metrics tracking
    private final AtomicLong dlqVolume = new AtomicLong(0);
    private final AtomicLong mainTopicVolume = new AtomicLong(0);
    private final AtomicLong retryExhaustionCount = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> errorPatterns = new ConcurrentHashMap<>();

    // Restart-loop detection: sliding window of restart timestamps
    private final Deque<Long> restartTimestamps = new ConcurrentLinkedDeque<>();

    public DefaultResilienceWrapper(ResilienceConfig config,
                                    GosiKafkaProducer<K, V> dlqProducer,
                                    GosiTelemetryReporter telemetryReporter) {
        this.config = config;
        this.dlqProducer = dlqProducer;
        this.telemetryReporter = telemetryReporter;
        this.dlqTopicName = TopicNamingUtils.buildDlqTopic(config.getNamespace(), config.getProcessingStage());
        
        LOG.info("Initialized ResilienceWrapper | stage={} | policy={} | dlqTopic={} | maxRetries={}",
                config.getProcessingStage(), config.getErrorPolicy(), dlqTopicName, config.getMaxRetries());
    }

    @Override
    public void process(GosiRecord<K, V> record, RecordHandler<K, V> handler) throws Exception {
        mainTopicVolume.incrementAndGet();

        if (config.getErrorPolicy() == ErrorPolicy.FAIL_FAST) {
            processFailFast(record, handler);
        } else {
            processWithRetryAndDlq(record, handler);
        }
    }

    private void processFailFast(GosiRecord<K, V> record, RecordHandler<K, V> handler) throws Exception {
        try {
            handler.handle(record);
        } catch (Exception e) {
            LOG.error("FAIL_FAST: Processing error — stopping immediately | stage={} | trace_id={} | topic={}",
                    config.getProcessingStage(), record.getTraceId(), record.getTopic(), e);
            trackErrorPattern(e);
            throw e;
        }
    }

    private void processWithRetryAndDlq(GosiRecord<K, V> record, RecordHandler<K, V> handler) throws Exception {
        Exception lastException = null;
        int attempt = 0;

        while (attempt <= config.getMaxRetries()) {
            try {
                handler.handle(record);
                return; // Success
            } catch (Exception e) {
                lastException = e;
                attempt++;

                if (attempt <= config.getMaxRetries()) {
                    LOG.warn("Retry {}/{} | stage={} | trace_id={} | error={}",
                            attempt, config.getMaxRetries(), config.getProcessingStage(),
                            record.getTraceId(), e.getMessage());
                    
                    try {
                        Thread.sleep(config.getRetryBackoffMs() * attempt); // Linear backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            }
        }

        // All retries exhausted — route to DLQ
        retryExhaustionCount.incrementAndGet();
        trackErrorPattern(lastException);
        routeToDlq(record, lastException, attempt - 1);

        telemetryReporter.onRetryExhaustion(
                record.getTopic(), config.getProcessingStage(),
                record.getTraceId(), attempt - 1, lastException);

        // Check DLQ accumulation alert threshold
        long currentDlqVolume = dlqVolume.get();
        if (currentDlqVolume > 0 && currentDlqVolume % config.getDlqAccumulationAlertThreshold() == 0) {
            double ratio = calculateDlqRatio();
            telemetryReporter.onDlqAccumulation(dlqTopicName, currentDlqVolume, ratio);
        }
    }

    private void routeToDlq(GosiRecord<K, V> record, Exception cause, int retryCount) {
        dlqVolume.incrementAndGet();

        // Clear any existing error headers to avoid duplicate/stale metadata
        record.getHeaders().remove("error_code");
        record.getHeaders().remove("stack_trace");
        record.getHeaders().remove("processing_stage");
        record.getHeaders().remove("original_topic");
        record.getHeaders().remove("original_offset");
        record.getHeaders().remove("retry_count");
        record.getHeaders().remove("failure_timestamp");

        // Standardized DLQ headers matching existing Splunk dashboards
        String errorCode = deriveErrorCode(cause);
        record.getHeaders().add("error_code", errorCode.getBytes(StandardCharsets.UTF_8));
        record.getHeaders().add("stack_trace", getStackTrace(cause).getBytes(StandardCharsets.UTF_8));
        
        // Ensure trace_id is preserved
        TraceContext.injectIntoHeaders(record.getHeaders());

        // Enrichment headers
        record.getHeaders().add("processing_stage", config.getProcessingStage().getBytes(StandardCharsets.UTF_8));
        record.getHeaders().add("original_topic", record.getTopic().getBytes(StandardCharsets.UTF_8));
        record.getHeaders().add("original_offset", String.valueOf(record.getOffset()).getBytes(StandardCharsets.UTF_8));
        record.getHeaders().add("retry_count", String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8));
        record.getHeaders().add("failure_timestamp", String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));

        try {
            dlqProducer.sendAsync(dlqTopicName, record.getKey(), record.getValue(), record.getHeaders());
            telemetryReporter.onDlqReroute(record.getTopic(), dlqTopicName, record.getTraceId(), cause);
            
            LOG.warn("Routed to DLQ | dlq={} | stage={} | trace_id={} | error_code={} | retries={}",
                    dlqTopicName, config.getProcessingStage(), record.getTraceId(), errorCode, retryCount);
        } catch (Exception dlqError) {
            LOG.error("CRITICAL: Failed to route to DLQ — potential data loss | dlq={} | trace_id={}",
                    dlqTopicName, record.getTraceId(), dlqError);
            throw new RuntimeException("DLQ routing failed for trace_id=" + record.getTraceId(), dlqError);
        }
    }

    @Override
    public ResilienceMetrics getMetrics() {
        return new ResilienceMetrics(
                dlqVolume.get(),
                mainTopicVolume.get(),
                calculateDlqRatio(),
                retryExhaustionCount.get(),
                getErrorPatternSnapshot(),
                getActiveRestartCount(),
                getWindowStartMs(),
                config.getProcessingStage()
        );
    }

    @Override
    public boolean isInRestartLoop() {
        return getActiveRestartCount() >= config.getRestartLoopThreshold();
    }

    @Override
    public void recordRestart() {
        long now = System.currentTimeMillis();
        restartTimestamps.addLast(now);
        pruneOldRestarts(now);

        int count = getActiveRestartCount();
        if (count >= config.getRestartLoopThreshold()) {
            LOG.error("RESTART LOOP DETECTED | restarts={} in {}ms window | stage={} | threshold={}",
                    count, config.getRestartLoopWindowMs(), config.getProcessingStage(),
                    config.getRestartLoopThreshold());
            telemetryReporter.onRestartLoopDetected(
                    config.getNamespace() + "-" + config.getProcessingStage(),
                    count, config.getRestartLoopWindowMs());
        }
    }

    @Override
    public String getDlqTopicName() {
        return dlqTopicName;
    }

    // --- Private helpers ---

    private double calculateDlqRatio() {
        long main = mainTopicVolume.get();
        if (main == 0) return 0.0;
        return (double) dlqVolume.get() / main;
    }

    private Map<String, Long> getErrorPatternSnapshot() {
        Map<String, Long> snapshot = new HashMap<>();
        errorPatterns.forEach((k, v) -> snapshot.put(k, v.get()));
        return snapshot;
    }

    private void trackErrorPattern(Exception e) {
        String errorCode = deriveErrorCode(e);
        errorPatterns.computeIfAbsent(errorCode, k -> new AtomicLong(0)).incrementAndGet();
    }

    private int getActiveRestartCount() {
        long now = System.currentTimeMillis();
        pruneOldRestarts(now);
        return restartTimestamps.size();
    }

    private void pruneOldRestarts(long now) {
        long cutoff = now - config.getRestartLoopWindowMs();
        while (!restartTimestamps.isEmpty() && restartTimestamps.peekFirst() < cutoff) {
            restartTimestamps.pollFirst();
        }
    }

    private long getWindowStartMs() {
        if (restartTimestamps.isEmpty()) {
            return System.currentTimeMillis();
        }
        return restartTimestamps.peekFirst();
    }

    private String deriveErrorCode(Exception e) {
        if (e == null) return "UNKNOWN";
        String className = e.getClass().getSimpleName();
        // Map common exceptions to meaningful error codes
        if (className.contains("Timeout")) return "TIMEOUT";
        if (className.contains("Serialization")) return "SERIALIZATION_ERROR";
        if (className.contains("Authorization")) return "AUTHORIZATION_DENIED";
        if (className.contains("Authentication")) return "AUTHENTICATION_FAILURE";
        if (className.contains("NullPointer")) return "NULL_POINTER";
        if (className.contains("IllegalArgument")) return "INVALID_ARGUMENT";
        return "PROCESSING_ERROR";
    }

    private String getStackTrace(Throwable throwable) {
        if (throwable == null) return "";
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        throwable.printStackTrace(pw);
        String fullTrace = sw.toString();
        // Truncate to prevent oversized headers (max 4KB)
        if (fullTrace.length() > 4096) {
            return fullTrace.substring(0, 4093) + "...";
        }
        return fullTrace;
    }
}
