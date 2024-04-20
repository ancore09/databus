using Confluent.Kafka;
using Databus.Api.Kafka;
using Google.Protobuf;

namespace Databus.Api.Services;

public interface IConsumerService
{
    Task ConsumeLoop(WrappedConsumer consumer, Func<SubscribeResponse, Task> sendMessage,
        Func<Task<SubscribeRequest?>> getAck, CancellationToken cancellationToken);
}

public class ConsumerService: IConsumerService
{
    private readonly ILogger<ConsumerService> _logger;

    public ConsumerService(ILogger<ConsumerService> logger)
    {
        _logger = logger;
    }

    public async Task ConsumeLoop(WrappedConsumer consumer, Func<SubscribeResponse, Task> sendMessage,
        Func<Task<SubscribeRequest?>> getAck, CancellationToken cancellationToken)
    {
        long failedAcks = 0;

        try
        {
            while (true)
            {
                ConsumeResult<string, string>? result = null;

                result = consumer.Consume(cancellationToken);

                if (cancellationToken.IsCancellationRequested)
                    break;

                var message = result!.Message.Value;

                var response = new SubscribeResponse()
                {
                    Key = result.Message.Key,
                    Message = message,
                    Headers =
                    {
                        result.Message.Headers?.Select(x => new Header()
                            {Key = x.Key, Payload = ByteString.CopyFrom(x.GetValueBytes())})
                    }
                };

                await sendMessage(response);

                var ack = await getAck();

                if (ack.Ack is true)
                {
                    consumer.Commit(result);
                    consumer.StoreOffset(result);
                }
                else
                {
                    failedAcks++;
                    consumer.Seek(result);
                    _logger.LogInformation("Failed acks: {failedAcks}", failedAcks);
                }
            }
        }
        catch (OperationCanceledException e)
        {
            
        }
        finally
        {
            _logger.LogInformation("Closing consumer");
            consumer.Dispose();
            _logger.LogInformation("Consumer closed");
        }
    }
}