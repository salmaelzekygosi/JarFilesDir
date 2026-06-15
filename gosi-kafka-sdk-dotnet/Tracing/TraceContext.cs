namespace Gosi.Kafka.Sdk.Tracing;

using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

/// <summary>
/// Manages trace_id propagation between Kafka headers and ILogger scope in .NET.
/// </summary>
public static class TraceContext
{
    public const string TraceIdKey = "trace_id";
    public const string TraceIdKeyLegacy = "traceId";

    /// <summary>
    /// Initializes the ILogger scope from Kafka headers.
    /// Generates a new UUID if no trace_id is present.
    /// </summary>
    public static IDisposable? InitFromHeaders(ILogger logger, Headers headers, out string traceId)
    {
        var extracted = ExtractFromHeaders(headers);
        traceId = string.IsNullOrWhiteSpace(extracted) ? Guid.NewGuid().ToString() : extracted;

        var scopeState = new Dictionary<string, object>
        {
            { TraceIdKey, traceId }
        };

        return logger.BeginScope(scopeState);
    }

    /// <summary>
    /// Injects a trace_id into Kafka headers.
    /// If traceId is null, generates a new one.
    /// </summary>
    public static string InjectIntoHeaders(Headers headers, string? currentTraceId = null)
    {
        var traceId = currentTraceId;
        if (string.IsNullOrWhiteSpace(traceId))
        {
            traceId = Guid.NewGuid().ToString();
        }

        headers.Remove(TraceIdKey);
        headers.Remove(TraceIdKeyLegacy);

        headers.Add(TraceIdKey, Encoding.UTF8.GetBytes(traceId));
        return traceId;
    }

    public static string? ExtractFromHeaders(Headers headers)
    {
        if (headers == null) return null;

        if (headers.TryGetLastBytes(TraceIdKey, out var standardBytes))
        {
            return Encoding.UTF8.GetString(standardBytes);
        }

        if (headers.TryGetLastBytes(TraceIdKeyLegacy, out var legacyBytes))
        {
            return Encoding.UTF8.GetString(legacyBytes);
        }

        return null;
    }
}
