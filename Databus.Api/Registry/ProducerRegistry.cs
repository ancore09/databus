using System.Collections.Concurrent;
using Confluent.Kafka;
using Databus.Api.Kafka;

namespace Databus.Api.Registry;

public interface IProducerRegistry
{
    Task<WrappedProducer> GetProducer(string topic);
    Task<WrappedConsumer> GetConsumer(string topic, string consumerGroup);
}

public class ProducerRegistry: IProducerRegistry
{
    private readonly ConcurrentDictionary<string, WrappedProducer> _producers = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _slims = new();
    private readonly ILogger<ProducerRegistry> _logger;

    public ProducerRegistry(ILogger<ProducerRegistry> logger)
    {
        _logger = logger;
    }

    public async Task<WrappedProducer> GetProducer(string topic)
    {
        SemaphoreSlim? slim = null;

        try
        {
            slim = _slims.GetOrAdd(topic, _ => new SemaphoreSlim(1, 1));
            await slim.WaitAsync();

            if (_producers.TryGetValue(topic, out var producer))
            {
                await producer.Join();
                return producer;
            }

            producer = Creator.CreateProducer(topic, async () =>
            {
                _producers.TryRemove(topic, out var producer);
                producer.Dispose();
                _slims.TryRemove(topic, out _);
                _logger.LogInformation("Removed producer for {topic}", topic);
            });
            _producers.TryAdd(topic, producer);

            await producer.Join();

            return producer;
        }
        finally
        {
            slim?.Release();
        }
    }

    public async Task<WrappedConsumer> GetConsumer(string topic, string consumerGroup)
    {
        await Task.CompletedTask;
        
        var consumer = Creator.CreateConsumer(topic, consumerGroup);

        return consumer;
    }
}