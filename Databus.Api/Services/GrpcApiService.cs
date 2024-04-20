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


        // long failedAcks = 0;
        //
        // try
        // {
        //     while (true)
        //     {
        //         ConsumeResult<string, string>? result = null;
        //
        //         result = consumer.Consume(context.CancellationToken);
        //
        //         if (context.CancellationToken.IsCancellationRequested)
        //             break;
        //
        //         var message = result!.Message.Value;
        //
        //         await responseStream.WriteAsync(new SubscribeResponse()
        //         {
        //             Key = result.Message.Key,
        //             Message = message,
        //             Headers =
        //             {
        //                 result.Message.Headers?.Select(x => new Header()
        //                     {Key = x.Key, Payload = ByteString.CopyFrom(x.GetValueBytes())})
        //             }
        //         });
        //
        //         var readNext = await requestStream.MoveNext();
        //
        //         if (readNext)
        //         {
        //             var ack = requestStream.Current;
        //
        //             if (ack.Ack is true)
        //             {
        //                 consumer.Commit(result);
        //                 consumer.StoreOffset(result);
        //             }
        //             else
        //             {
        //                 failedAcks++;
        //                 consumer.Seek(result);
        //                 _logger.LogInformation("Failed acks: {failedAcks}", failedAcks);
        //             }
        //         }
        //         else
        //         {
        //             throw new Exception("Exception during advancing stream reader");
        //         }
        //     }
        // }
        // finally
        // {
        //     _logger.LogInformation("Closing consumer for {topic}", topic);
        //     consumer.Dispose();
        // }
    }

    public override async Task Produce(IAsyncStreamReader<ProduceRequest> requestStream, IServerStreamWriter<ProduceResponse> responseStream, ServerCallContext context)
    {
        var topic = context.RequestHeaders.GetValue("topic")!;

        var producer = await _registry.GetProducer(topic);

        await _producerService.ProduceLoop(producer, requestStream.ReadAllAsync(context.CancellationToken),
            async response => await responseStream.WriteAsync(response), context.CancellationToken);
        
    //     await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken))
    //     {
    //         var headers = new Headers();
    //         var headersList = request.Headers?.Select(x => new Confluent.Kafka.Header(x.Key, x.Payload.ToByteArray()));
    //
    //         if (headersList != null)
    //             foreach (var header in headersList)
    //                 headers.Add(header);
    //
    //         var message = new Message<string, string>()
    //         {
    //             Key = request.Key,
    //             Value = request.Message,
    //             Headers = headers
    //         };
    //
    //         try
    //         {
    //             await producer.ProduceAsync(message, context.CancellationToken);
    //
    //             await responseStream.WriteAsync(new ProduceResponse
    //             {
    //                 Ack = true
    //             });
    //         }
    //         catch
    //         {
    //             await responseStream.WriteAsync(new ProduceResponse
    //             {
    //                 Ack = false
    //             });
    //         }
    //     }
    //
    //     _logger.LogInformation("Leaving producer");
    //     await producer.Leave();
    }
}