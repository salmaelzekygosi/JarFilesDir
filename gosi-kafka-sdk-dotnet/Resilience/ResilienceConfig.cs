namespace Gosi.Kafka.Sdk.Resilience;

public enum ErrorPolicy
{
    FAIL_FAST,
    CAPTURE_DLQ
}

public class ResilienceConfig
{
    public string? Namespace { get; set; }
    public string? Stage { get; set; }
    public string? SourceTopicName { get; set; }
    public ErrorPolicy ErrorPolicy { get; set; } = ErrorPolicy.CAPTURE_DLQ;
    public int MaxRetries { get; set; } = 3;
    public long RetryBackoffMs { get; set; } = 1000;
    public long DlqAccumulationAlertThreshold { get; set; } = 100;
    public int RestartLoopThreshold { get; set; } = 3;
    public long RestartLoopWindowMs { get; set; } = 600000;

    public void Validate()
    {
        if (ErrorPolicy == ErrorPolicy.CAPTURE_DLQ)
        {
            if (string.IsNullOrWhiteSpace(Namespace)) throw new System.ArgumentException("Namespace must be configured when ErrorPolicy is CAPTURE_DLQ");
            if (string.IsNullOrWhiteSpace(Stage)) throw new System.ArgumentException("Stage must be configured when ErrorPolicy is CAPTURE_DLQ");
            if (string.IsNullOrWhiteSpace(SourceTopicName)) throw new System.ArgumentException("SourceTopicName must be configured");
        }
    }

    public string GetDlqTopicName()
    {
        if (string.IsNullOrWhiteSpace(Namespace) || string.IsNullOrWhiteSpace(Stage))
        {
            return $"{SourceTopicName}.dlq";
        }
        return $"{Namespace}.dlq.{Stage}.v1".ToLower();
    }
}
