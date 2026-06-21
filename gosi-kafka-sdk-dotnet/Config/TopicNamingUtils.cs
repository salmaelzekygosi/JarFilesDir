namespace Gosi.Kafka.Sdk.Naming;

using System.Text.RegularExpressions;

public static class TopicNamingUtils
{
    private static readonly Regex OrgStandardTopicRegex = new(@"^[a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9-]+\.v[0-9]+$", RegexOptions.Compiled);

    public static bool IsValid(string topicName)
    {
        if (string.IsNullOrWhiteSpace(topicName)) return false;
        if (topicName.Length > 200) return false;

        return OrgStandardTopicRegex.IsMatch(topicName);
    }

    public static void Validate(string topicName)
    {
        if (!IsValid(topicName))
        {
            throw new TopicNamingException(
                $"Topic '{topicName}' violates org standards. Must follow format '<namespace>.<entity>.<type>.v(n)' in lowercase, dot-separated, max 200 chars. Example: hrsd.employee.events.v1");
        }
    }
}

public class TopicNamingException : System.Exception
{
    public TopicNamingException(string message) : base(message) { }
}
