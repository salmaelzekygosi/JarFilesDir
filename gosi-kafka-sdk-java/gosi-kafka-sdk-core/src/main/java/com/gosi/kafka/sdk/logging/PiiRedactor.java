package com.gosi.kafka.sdk.logging;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Dynamic PII/sensitive-field redactor.
 * <p>
 * Teams configure the exact fields, regex patterns, and replacement strings they want to use.
 * This allows highly dynamic masking (e.g. replacing IBANs completely, but only partially masking emails).
 * </p>
 */
public class PiiRedactor {

    private final Map<String, RedactionRule> rules;

    /**
     * Creates a PiiRedactor with custom rules.
     *
     * @param rules map of field name (lowercase) to its redaction rule
     */
    public PiiRedactor(Map<String, RedactionRule> rules) {
        if (rules == null || rules.isEmpty()) {
            this.rules = Collections.emptyMap();
        } else {
            this.rules = Collections.unmodifiableMap(new HashMap<>(rules));
        }
    }

    /**
     * Creates a PiiRedactor with no rules (no-op).
     */
    public PiiRedactor() {
        this(null);
    }

    /**
     * Parses a semicolon-separated list of configuration rules.
     * Format: {@code fieldName=regex=replacement;fieldName2=regex2=replacement2}
     * <br>Example: {@code email=^.*@.*$=[REDACTED];iban=^SA[0-9]{22}$=****}
     *
     * @param config the configuration string
     */
    public static PiiRedactor fromConfig(String config) {
        if (config == null || config.trim().isEmpty()) {
            return new PiiRedactor();
        }

        Map<String, RedactionRule> parsedRules = new HashMap<>();
        for (String ruleStr : config.split(";")) {
            String trimmed = ruleStr.trim();
            if (trimmed.isEmpty()) continue;

            // Split into exactly 3 parts: field, regex, replacement
            String[] parts = trimmed.split("=", 3);
            if (parts.length == 3) {
                String fieldName = parts[0].trim().toLowerCase();
                String regex = parts[1].trim();
                String replacement = parts[2].trim();
                try {
                    Pattern pattern = Pattern.compile(regex);
                    parsedRules.put(fieldName, new RedactionRule(pattern, replacement));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid regex in PII config for field " + fieldName + ": " + regex, e);
                }
            } else {
                throw new IllegalArgumentException("Invalid PII config format: " + trimmed + ". Expected fieldName=regex=replacement");
            }
        }
        return new PiiRedactor(parsedRules);
    }

    /**
     * Redacts a value if a rule exists for the field name.
     *
     * @param fieldName the name of the field
     * @param value     the field value
     * @return the redacted value if a rule matched, original value otherwise
     */
    public String redact(String fieldName, String value) {
        if (value == null || value.isEmpty() || fieldName == null) {
            return value;
        }

        RedactionRule rule = rules.get(fieldName.toLowerCase());
        if (rule != null) {
            Matcher matcher = rule.getPattern().matcher(value);
            if (matcher.find()) {
                // If it matches the pattern, replace all matches with the configured replacement
                return matcher.replaceAll(rule.getReplacement());
            }
        }
        return value;
    }

    /**
     * Redacts sensitive header values in a Kafka Headers collection.
     *
     * @param headers the Kafka headers
     * @return map of header name → redacted string value
     */
    public Map<String, String> redactHeaders(Headers headers) {
        Map<String, String> result = new HashMap<>();
        if (headers == null) return result;
        
        for (Header header : headers) {
            String value = header.value() != null 
                    ? new String(header.value(), java.nio.charset.StandardCharsets.UTF_8) 
                    : null;
            result.put(header.key(), redact(header.key(), value));
        }
        return result;
    }

    /**
     * Batch-redacts a map of field name → value pairs.
     *
     * @param fields the fields to redact
     * @return new map with sensitive values masked
     */
    public Map<String, String> redactMap(Map<String, String> fields) {
        if (fields == null) return Collections.emptyMap();
        
        Map<String, String> result = new HashMap<>(fields.size());
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            result.put(entry.getKey(), redact(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    /**
     * Gets the configured rules.
     */
    public Map<String, RedactionRule> getRules() {
        return rules;
    }

    public static class RedactionRule {
        private final Pattern pattern;
        private final String replacement;

        public RedactionRule(Pattern pattern, String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }

        public Pattern getPattern() { return pattern; }
        public String getReplacement() { return replacement; }
    }
}
