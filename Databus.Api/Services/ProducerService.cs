using Confluent.Kafka;
using Databus.Api.Kafka;

namespace Databus.Api.Services;

public interface IProducerService
{
    Task ProduceLoop(WrappedProducer producer, IAsyncEnumerable<ProduceRequest> requests,
        Func<ProduceResponse, Task> ack, CancellationToken cancellationToken);
}

public class ProducerService: IProducerService
{
    private readonly ILogger<ProducerService> _logger;

    public ProducerService(ILogger<ProducerService> logger)
    {
        _logger = logger;
    }

    public async Task ProduceLoop(WrappedProducer producer, IAsyncEnumerable<ProduceRequest> requests,
        Func<ProduceResponse, Task> ack, CancellationToken cancellationToken)
    {
        await foreach (var request in requests)
        {
            var headers = new Headers();
            var headersList = request.Headers?.Select(x => new Confluent.Kafka.Header(x.Key, x.Payload.ToByteArray()));

            if (headersList != null)
                foreach (var header in headersList)
                    headers.Add(header);

            var message = new Message<string, string>()
            {
                Key = request.Key,
                Value = request.Message,
                Headers = headers
            };

            try
            {
                await producer.ProduceAsync(message, cancellationToken);

                var response = new ProduceResponse
                {
                    Ack = true
                };

                await ack(response);
            }
            catch
            {
                var response = new ProduceResponse
                {
                    Ack = false
                };

                await ack(response);
            }
        }

        _logger.LogInformation("Leaving producer");
        await producer.Leave();
    }
}