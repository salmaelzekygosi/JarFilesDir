namespace Gosi.Kafka.Sdk.Resilience;

using System;
using System.Threading.Tasks;
using Gosi.Kafka.Sdk.Consumer;

public interface IResilienceWrapper<TKey, TValue> 
    where TKey : class
    where TValue : class
{
    /// <summary>
    /// Processes a record with retries, DLQ routing, and telemetry.
    /// </summary>
    Task ProcessAsync(GosiRecord<TKey, TValue> record, Func<GosiRecord<TKey, TValue>, Task> processor);

    /// <summary>
    /// Records a consumer restart to track CrashLoopBackOff states.
    /// </summary>
    void RecordRestart();

    /// <summary>
    /// Checks if the consumer is currently in a restart loop.
    /// </summary>
    bool IsInRestartLoop();
}
