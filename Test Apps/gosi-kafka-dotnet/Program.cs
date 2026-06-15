using Gosi.Kafka.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();

// Register Kafka services
builder.Services.AddSingleton<KafkaProducerService>();
builder.Services.AddHostedService<KafkaConsumerService>();

var app = builder.Build();

app.UseAuthorization();
app.MapControllers();

app.Run();
