using Confluent.Kafka;

namespace Databus.Api.Kafka;

public class WrappedConsumer: IDisposable
{
    private readonly IConsumer<string, string> _consumer;

    public WrappedConsumer(IConsumer<string, string> consumer)
    {
        _consumer = consumer;
    }

    public ConsumeResult<string, string>? Consume(int timeout)
    {
        return _consumer.Consume(timeout);
    }
    
    public ConsumeResult<string, string>? Consume(CancellationToken cancellationToken)
    {
        return _consumer.Consume(cancellationToken);
    }

    public void Commit(ConsumeResult<string, string> result)
    {
        _consumer.Commit(result);
    }

    public void Close()
    {
        _consumer.Close();
    }
    
    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }

    public void StoreOffset(ConsumeResult<string, string> result)
    {
        _consumer.StoreOffset(result);
    }
    
    public void Assign(ConsumeResult<string, string> result)
    {
        _consumer.Assign(result.TopicPartitionOffset);
    }
    
    public void Seek(ConsumeResult<string, string> result)
    {
        _consumer.Seek(result.TopicPartitionOffset);
    }
}