using Gosi.Kafka.Services;
using Gosi.Kafka.Sdk.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();

// Register GOSI Kafka SDK natively
builder.Services.AddGosiKafka(builder.Configuration);

// Register application Kafka services
builder.Services.AddSingleton<KafkaProducerService>();
builder.Services.AddHostedService<KafkaConsumerService>();

var app = builder.Build();

app.UseAuthorization();
app.MapControllers();

app.Run();
