using Confluent.Kafka;

namespace Databus.Api.Kafka;

public class WrappedProducer
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;
    private readonly SemaphoreSlim _slim;
    private long _clientCount;
    private Func<Task> _close;

    public WrappedProducer(IProducer<string, string> producer, string topic, Func<Task> close)
    {
        _producer = producer;
        _topic = topic;
        _slim = new SemaphoreSlim(1);
        _close = close;
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(Message<string, string> message, CancellationToken cancellationToken)
    {
        return await _producer.ProduceAsync(_topic, message, cancellationToken);
    }

    public async Task Join()
    {
        await _slim.WaitAsync();

        _clientCount++;

        _slim.Release();
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
    
    public async Task Leave()
    {
        await _slim.WaitAsync();

        _clientCount--;
        if (_clientCount == 0)
            await _close();

        _slim.Release();
    }
}