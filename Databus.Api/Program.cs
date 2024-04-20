using Databus.Api.Registry;
using Databus.Api.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddGrpcReflection();
builder.Services.AddSingleton<IProducerRegistry, ProducerRegistry>();
builder.Services.AddScoped<IConsumerService, ConsumerService>();
builder.Services.AddScoped<IProducerService, ProducerService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcReflectionService();
app.MapGrpcService<GrpcApiService>();

app.Run();