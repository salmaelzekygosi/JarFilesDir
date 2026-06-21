namespace Gosi.Kafka.Sdk.GoLive;

using System;
using System.Collections.Generic;
using Gosi.Kafka.Sdk.Config;
using Gosi.Kafka.Sdk.Naming;

public class GateViolation
{
    public string Component { get; }
    public string Issue { get; }
    public bool IsBlocking { get; }

    public GateViolation(string component, string issue, bool isBlocking)
    {
        Component = component;
        Issue = issue;
        IsBlocking = isBlocking;
    }
}

public class GoLiveGateChecker
{
    public static List<GateViolation> VerifyCompliance(GosiKafkaClientConfig config, string? topicToProduce = null)
    {
        var violations = new List<GateViolation>();

        // Rule 1: Topic Naming Standard
        if (!string.IsNullOrWhiteSpace(topicToProduce))
        {
            if (!TopicNamingUtils.IsValid(topicToProduce))
            {
                violations.Add(new GateViolation(
                    "TopicNaming",
                    $"Topic '{topicToProduce}' does not match org standard <namespace>.<entity>.<type>.v(n)",
                    true));
            }
        }

        // Rule 2: Resilience must be enabled
        if (config.ResilienceConfig == null)
        {
            violations.Add(new GateViolation(
                "Resilience",
                "ResilienceWrapper config is missing. All consumers must implement CrashLoopBackOff and DLQ routing.",
                true));
        }
        else
        {
            if (config.ResilienceConfig.ErrorPolicy == Gosi.Kafka.Sdk.Resilience.ErrorPolicy.CAPTURE_DLQ)
            {
                if (string.IsNullOrWhiteSpace(config.ResilienceConfig.Namespace) || 
                    string.IsNullOrWhiteSpace(config.ResilienceConfig.Stage))
                {
                    violations.Add(new GateViolation(
                        "Resilience",
                        "Namespace and Stage MUST be provided when ErrorPolicy=CAPTURE_DLQ to enforce DLQ naming.",
                        true));
                }
            }
        }

        // Rule 3: OAuth Scopes
        if (config.AuthenticationHandler is Gosi.Kafka.Sdk.Auth.OAuthBearerAuthHandler)
        {
            // By constructor design, scope is now required
        }

        return violations;
    }
    
    public static void Enforce(GosiKafkaClientConfig config, string? topicToProduce = null)
    {
        var violations = VerifyCompliance(config, topicToProduce);
        var blocking = violations.FindAll(v => v.IsBlocking);
        
        if (blocking.Count > 0)
        {
            var msg = "Deployment blocked by GOSI Go-Live Gates:\n";
            foreach (var v in blocking)
            {
                msg += $"- [{v.Component}]: {v.Issue}\n";
            }
            throw new Exception(msg);
        }
    }
}
