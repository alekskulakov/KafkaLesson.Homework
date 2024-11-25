namespace KafkaLesson.Homework;

public class EventStorage
{
    private static readonly Lazy<EventStorage> Lazy = new(() => new EventStorage());
    public static EventStorage Instance => Lazy.Value;
    private EventStorage() { }
    
    private readonly List<TestEvent> _processedEvents = [];
    private readonly List<TestEvent> _allConsumedEvents = [];

    public void AddProcessedEvent(TestEvent ev)
    {
        _processedEvents.Add(ev);
    }

    public List<TestEvent> GetProcessedEvent()
    {
        return _processedEvents.ToList();
    }
    
    public void AddConsumedEvent(TestEvent ev)
    {
        _allConsumedEvents.Add(ev);
    }
    
    public List<TestEvent> GetAllConsumedEvents()
    {
        return _allConsumedEvents.ToList();
    }
}