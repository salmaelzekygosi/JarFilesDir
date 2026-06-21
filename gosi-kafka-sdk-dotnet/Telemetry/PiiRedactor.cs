namespace Gosi.Kafka.Sdk.Telemetry;

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Confluent.Kafka;

public class PiiRedactor
{
    private readonly Dictionary<string, RedactionRule> _rules;

    public PiiRedactor(Dictionary<string, RedactionRule> rules)
    {
        _rules = rules ?? new Dictionary<string, RedactionRule>();
    }

    public PiiRedactor() : this(new Dictionary<string, RedactionRule>()) { }

    /// <summary>
    /// Parses a semicolon-separated list of configuration rules.
    /// Format: fieldName=regex=replacement;fieldName2=regex2=replacement2
    /// Example: email=^.*@.*$=[REDACTED];iban=^SA[0-9]{22}$=****
    /// </summary>
    public static PiiRedactor FromConfig(string config)
    {
        if (string.IsNullOrWhiteSpace(config))
        {
            return new PiiRedactor();
        }

        var rules = new Dictionary<string, RedactionRule>(StringComparer.OrdinalIgnoreCase);
        
        foreach (var ruleStr in config.Split(';'))
        {
            var trimmed = ruleStr.Trim();
            if (string.IsNullOrEmpty(trimmed)) continue;

            var parts = trimmed.Split(new[] { '=' }, 3);
            if (parts.Length == 3)
            {
                var fieldName = parts[0].Trim();
                var regexPattern = parts[1].Trim();
                var replacement = parts[2].Trim();
                
                try
                {
                    var regex = new Regex(regexPattern, RegexOptions.Compiled);
                    rules[fieldName] = new RedactionRule(regex, replacement);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Invalid regex in PII config for field {fieldName}: {regexPattern}", e);
                }
            }
            else
            {
                throw new ArgumentException($"Invalid PII config format: {trimmed}. Expected fieldName=regex=replacement");
            }
        }

        return new PiiRedactor(rules);
    }

    public string? Redact(string fieldName, string? value)
    {
        if (string.IsNullOrEmpty(value) || string.IsNullOrEmpty(fieldName)) return value;

        if (_rules.TryGetValue(fieldName, out var rule))
        {
            return rule.Regex.Replace(value, rule.Replacement);
        }

        return value;
    }

    public Dictionary<string, string?> RedactHeaders(Headers headers)
    {
        var result = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        if (headers == null) return result;

        foreach (var header in headers)
        {
            var valueStr = header.GetValueBytes() != null 
                ? System.Text.Encoding.UTF8.GetString(header.GetValueBytes()) 
                : null;
            
            result[header.Key] = Redact(header.Key, valueStr);
        }

        return result;
    }

    public Dictionary<string, string?> RedactDictionary(IDictionary<string, string?> fields)
    {
        var result = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        if (fields == null) return result;

        foreach (var kvp in fields)
        {
            result[kvp.Key] = Redact(kvp.Key, kvp.Value);
        }

        return result;
    }
}

public class RedactionRule
{
    public Regex Regex { get; }
    public string Replacement { get; }

    public RedactionRule(Regex regex, string replacement)
    {
        Regex = regex;
        Replacement = replacement;
    }
}
