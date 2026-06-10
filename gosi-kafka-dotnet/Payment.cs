namespace Gosi.Kafka
{
    public class Payment
    {
        public string? Id { get; set; }
        public double Amount { get; set; }
        public string? Currency { get; set; }
        public string? TraceId { get; set; }

        public override string ToString()
        {
            return $"Payment{{Id='{Id}', Amount={Amount}, Currency='{Currency}', TraceId='{TraceId}'}}";
        }
    }
}
