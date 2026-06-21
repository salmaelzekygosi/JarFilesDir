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
    public void process(GosiRecord<K, V> gosiRecord, RecordHandler<K, V> handler) throws Exception {
        mainTopicVolume.incrementAndGet();

        if (config.getErrorPolicy() == ErrorPolicy.FAIL_FAST) {
            processFailFast(gosiRecord, handler);
        } else {
            processWithRetryAndDlq(gosiRecord, handler);
        }
    }

    private void processFailFast(GosiRecord<K, V> gosiRecord, RecordHandler<K, V> handler) throws Exception {
        try {
            handler.handle(gosiRecord);
        } catch (Exception e) {
            LOG.error("FAIL_FAST: Processing error — stopping immediately | stage={} | trace_id={} | topic={}",
                    config.getProcessingStage(), gosiRecord.getTraceId(), gosiRecord.getTopic(), e);
            trackErrorPattern(e);
            throw new org.apache.kafka.common.KafkaException("Fail fast processing error", e);
        }
    }

    private void processWithRetryAndDlq(GosiRecord<K, V> gosiRecord, RecordHandler<K, V> handler) throws Exception {
        Exception lastException = null;
        int attempt = 0;

        while (attempt <= config.getMaxRetries()) {
            try {
                handler.handle(gosiRecord);
                return; // Success
            } catch (Exception e) {
                lastException = e;
                attempt++;

                if (attempt <= config.getMaxRetries()) {
                    LOG.warn("Retry {}/{} | stage={} | trace_id={} | error={}",
                            attempt, config.getMaxRetries(), config.getProcessingStage(),
                            gosiRecord.getTraceId(), e.getMessage());
                    
                    try {
                        Thread.sleep(config.getRetryBackoffMs() * attempt); // Linear backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new org.apache.kafka.common.KafkaException("Retry interrupted", ie);
                    }
                }
            }
        }

        // All retries exhausted — route to DLQ
        retryExhaustionCount.incrementAndGet();
        trackErrorPattern(lastException);
        routeToDlq(gosiRecord, lastException, attempt - 1);

        telemetryReporter.onRetryExhaustion(
                gosiRecord.getTopic(), config.getProcessingStage(),
                gosiRecord.getTraceId(), attempt - 1, lastException);

        // Check DLQ accumulation alert threshold
        long currentDlqVolume = dlqVolume.get();
        if (currentDlqVolume > 0 && currentDlqVolume % config.getDlqAccumulationAlertThreshold() == 0) {
            double ratio = calculateDlqRatio();
            telemetryReporter.onDlqAccumulation(dlqTopicName, currentDlqVolume, ratio);
        }
    }

    private void routeToDlq(GosiRecord<K, V> gosiRecord, Exception cause, int retryCount) {
        dlqVolume.incrementAndGet();

        // Clear any existing error headers to avoid duplicate/stale metadata
        gosiRecord.getHeaders().remove("error_code");
        gosiRecord.getHeaders().remove("stack_trace");
        gosiRecord.getHeaders().remove("processing_stage");
        gosiRecord.getHeaders().remove("original_topic");
        gosiRecord.getHeaders().remove("original_offset");
        gosiRecord.getHeaders().remove("retry_count");
        gosiRecord.getHeaders().remove("failure_timestamp");

        // Standardized DLQ headers matching existing Splunk dashboards
        String errorCode = deriveErrorCode(cause);
        gosiRecord.getHeaders().add("error_code", errorCode.getBytes(StandardCharsets.UTF_8));
        gosiRecord.getHeaders().add("stack_trace", getStackTrace(cause).getBytes(StandardCharsets.UTF_8));
        
        // Ensure trace_id is preserved
        TraceContext.injectIntoHeaders(gosiRecord.getHeaders());

        // Enrichment headers
        gosiRecord.getHeaders().add("processing_stage", config.getProcessingStage().getBytes(StandardCharsets.UTF_8));
        gosiRecord.getHeaders().add("original_topic", gosiRecord.getTopic().getBytes(StandardCharsets.UTF_8));
        gosiRecord.getHeaders().add("original_offset", String.valueOf(gosiRecord.getOffset()).getBytes(StandardCharsets.UTF_8));
        gosiRecord.getHeaders().add("retry_count", String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8));
        gosiRecord.getHeaders().add("failure_timestamp", String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));

        try {
            dlqProducer.sendAsync(dlqTopicName, gosiRecord.getKey(), gosiRecord.getValue(), gosiRecord.getHeaders());
            telemetryReporter.onDlqReroute(gosiRecord.getTopic(), dlqTopicName, gosiRecord.getTraceId(), cause);
            
            LOG.warn("Routed to DLQ | dlq={} | stage={} | trace_id={} | error_code={} | retries={}",
                    dlqTopicName, config.getProcessingStage(), gosiRecord.getTraceId(), errorCode, retryCount);
        } catch (Exception dlqError) {
            LOG.error("CRITICAL: Failed to route to DLQ — potential data loss | dlq={} | trace_id={}",
                    dlqTopicName, gosiRecord.getTraceId(), dlqError);
            throw new org.apache.kafka.common.KafkaException("DLQ routing failed for trace_id=" + gosiRecord.getTraceId(), dlqError);
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
