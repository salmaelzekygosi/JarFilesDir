namespace Gosi.Kafka.Sdk.Auth;

using System;
using Confluent.Kafka;

public enum AuthErrorType
{
    AUTHENTICATION_FAILURE,
    AUTHORIZATION_DENIED,
    UNKNOWN_ERROR
}

public static class AuthErrorClassifier
{
    public static AuthErrorType Classify(Exception exception)
    {
        if (exception == null) return AuthErrorType.UNKNOWN_ERROR;

        // Confluent.Kafka surfaces auth errors via ConsumeException/ProduceException which wrap KafkaException
        // We look at the Error.Code to distinguish Authentication vs Authorization

        if (exception is KafkaException kafkaException)
        {
            if (kafkaException.Error.Code == ErrorCode.Local_Authentication ||
                kafkaException.Error.Code == ErrorCode.SaslAuthenticationFailed)
            {
                return AuthErrorType.AUTHENTICATION_FAILURE;
            }

            if (kafkaException.Error.Code == ErrorCode.TopicAuthorizationFailed ||
                kafkaException.Error.Code == ErrorCode.GroupAuthorizationFailed ||
                kafkaException.Error.Code == ErrorCode.ClusterAuthorizationFailed)
            {
                return AuthErrorType.AUTHORIZATION_DENIED;
            }
        }
        
        // Unwrap AggregateException if present
        if (exception is AggregateException aggregateException && aggregateException.InnerException != null)
        {
            return Classify(aggregateException.InnerException);
        }

        // Specifically check for InnerExceptions that might be KafkaException
        if (exception.InnerException != null)
        {
            return Classify(exception.InnerException);
        }

        return AuthErrorType.UNKNOWN_ERROR;
    }

    public static Exception ClassifyAndWrap(Exception exception)
    {
        var type = Classify(exception);
        if (type == AuthErrorType.UNKNOWN_ERROR)
        {
            return exception;
        }

        return new GosiAuthException(type, exception.Message, exception);
    }
}

public class GosiAuthException : Exception
{
    public AuthErrorType ErrorType { get; }

    public GosiAuthException(AuthErrorType errorType, string message, Exception innerException) 
        : base($"[{errorType}] {message}", innerException)
    {
        ErrorType = errorType;
    }
}
