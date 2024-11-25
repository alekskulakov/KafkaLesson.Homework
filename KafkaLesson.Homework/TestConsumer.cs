using Dodo.Kafka.Consumer;

namespace KafkaLesson.Homework;

public class TestConsumer : IConsumer<TestEvent>
{
    public async Task Consume(TestEvent ev, CancellationToken ct)
    {
        await Task.CompletedTask;

        EventStorage.Instance.AddConsumedEvent(ev);

        var processedEvents = EventStorage.Instance.GetProcessedEvent();
        var existingEventWithHigherOrSameVersion = processedEvents.Find(@event => @event.Id == ev.Id && @event.Version >= ev.Version);
        if (existingEventWithHigherOrSameVersion == null)
            EventStorage.Instance.AddProcessedEvent(ev);
    }
}