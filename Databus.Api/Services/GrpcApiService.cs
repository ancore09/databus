using Confluent.Kafka;
using Databus.Api.Registry;
using Google.Protobuf;
using Grpc.Core;

namespace Databus.Api.Services;

public class GrpcApiService: Api.ApiBase
{
    private readonly ILogger<GrpcApiService> _logger;
    private readonly IProducerRegistry _registry;
    private readonly IConsumerService _consumerService;
    private readonly IProducerService _producerService;

    public GrpcApiService(ILogger<GrpcApiService> logger, IProducerRegistry registry, IConsumerService consumerService, IProducerService producerService)
    {
        _logger = logger;
        _registry = registry;
        _consumerService = consumerService;
        _producerService = producerService;
    }

    public override async Task Subscribe(IAsyncStreamReader<SubscribeRequest> requestStream, IServerStreamWriter<SubscribeResponse> responseStream, ServerCallContext context)
    {
        var topic = context.RequestHeaders.GetValue("topic")!;
        var consumerGroup = context.RequestHeaders.GetValue("consumerGroup")!;

        var consumer = await _registry.GetConsumer(topic, consumerGroup);
        
        _logger.LogInformation("Starting consumer for topic: {topic} and group {consumerGroup}", topic, consumerGroup);

        await _consumerService.ConsumeLoop(consumer, async response => await responseStream.WriteAsync(response), async () =>
        {
            var next = await requestStream.MoveNext(context.CancellationToken);

            if (next)
                return requestStream.Current;

            throw new Exception("Error during advancing stream");
        }, context.CancellationToken);
    }

    public override async Task Produce(IAsyncStreamReader<ProduceRequest> requestStream, IServerStreamWriter<ProduceResponse> responseStream, ServerCallContext context)
    {
        var topic = context.RequestHeaders.GetValue("topic")!;

        var producer = await _registry.GetProducer(topic);

        await _producerService.ProduceLoop(producer, requestStream.ReadAllAsync(context.CancellationToken),
            async response => await responseStream.WriteAsync(response), context.CancellationToken);
    }
}