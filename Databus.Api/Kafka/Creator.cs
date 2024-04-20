using Confluent.Kafka;

namespace Databus.Api.Kafka;

public static class Creator
{
    public static WrappedConsumer CreateConsumer(string topic, string consumerGroup)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            GroupId = consumerGroup
        };

        var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        return new WrappedConsumer(consumer);
    }

    public static WrappedProducer CreateProducer(string topic, Func<Task> close)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9092"
        };

        var producer = new ProducerBuilder<string, string>(config).Build();

        return new WrappedProducer(producer, topic, close);
    }
}