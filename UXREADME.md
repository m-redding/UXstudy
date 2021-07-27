# Streaming Producer
Publishing events using the producer client is optimized for high and consistent throughput scenarios, allowing applications to collect a set of events as a batch and publish in a single operation.  In order to maximize flexibility, developers are expected to build and manage batches according to the needs of their application, allowing them to prioritize trade-offs between ensuring batch density, enforcing strict ordering of events, and publishing on a consistent and predictable schedule.

The primary goal of the streaming producer is to provide developers the ability to queue individual events for publishing without the need to explicitly manage batch construction, population, or service operations.  Events are collected as they are queued, organized into batches, and published by the streaming producer as batches become full or a certain amount of time has elapsed.  When queuing events, developers may request automatic routing to a partition or explicitly control the partition in the same manner supported by the `ProducerClient`, with the streaming producer managing the details of grouping events into the appropriate batches for publication.

## Key concepts

- Each event queued for publishing is considered individual; there is no support for bundling events and forcing them to be batched together. 
- Similar to other Event Hubs client types, the streaming producer has a single connection with the Event Hubs service.
- The Streaming producer has support for the same set of constructors as the `EventHubProducerClient` and attempts to create continuity of concepts.
- After events are queued for publishing, sending occurs in the background. Event handlers for the successful or failed send of a given batch should be used in the typical .NET event handler pattern. If the events failed to send due to a transient failure, the fail handler is only called after applying the retry policy and the application can attempt the retry policy again. 


## Client lifetime
Each of the Event Hubs client types is safe to cache and use as a singleton for the lifetime of the application, which is best practice when events are being published or read regularly. The clients are responsible for efficient management of network, CPU, and memory use, working to keep usage low during periods of inactivity. Calling either `CloseAsync` or `DisposeAsync` on a client is required to ensure that network resources and other unmanaged objects are properly cleaned up.

## Usage examples

The streaming producer supports the same set of constructors that are allowed by the `EventHubProducerClient`.
### Creating a default streaming producer

```csharp
var connectionString = "<< CONNECTION STRING >>";
var eventHubName = "<< EVENT HUB NAME >>";

// Create the streaming producer with default options
var producer = new StreamingProducer(connectionString, eventHubName);
```

### Creating the client with custom options

```csharp  
var connectionString = "<< CONNECTION STRING >>";
var eventHubName = "<< EVENT HUB NAME >>";

// Create the streaming producer
var producer = new StreamingProducer(connectionString, eventHubName, new StreamingProducerOptions
{
    Identifier = "Streaming Producer with custom client",
    MaximumWaitTime = TimeSpan.FromMilliseconds(500),
    MaximumQueuedEventCount = 500
    RetryOptions = new EventHubsRetryOptions { TryTimeout = TimeSpan.FromMinutes(5) }
});    
```

### Creating the client with a fully qualified namespace and credential

```csharp
TokenCredential credential = new DefaultAzureCredential();
var fullyQualifiedNamespace = "<< NAMESPACE (likely similar to {your-namespace}.eventhub.windows.net) >>";
var eventHubName = "<< NAME OF THE EVENT HUB >>";

// Create the streaming producer with default options
var producer = new StreamingProducer(fullyQualifiedNamespace, eventHubName, credential);
```

### Creating client to ignore event ordering

When the ignore ordering flag is turned on then the order in which events are queued doesn't matter. By ignoring event ordering the producer can build and publish many batches at the same time since it doesn't matter if batch #1 fails and batch #2 succeeds because it can just resend number #1 without worrying about the order of delivery. This is useful when events are independent and can be processed in parallel without worrying about which was processed first. This functionality cannot be used at the same time as idempotency however, since idempotency depends on event ordering. 

```csharp 
var connectionString = "<< CONNECTION STRING >>";
var eventHubName = "<< EVENT HUB NAME >>";

// Create the streaming producer

var producer = new StreamingProducer(connectionString, eventHubName, new StreamingProducerOptions
{
    MaximumConcurrentSend = 4; 
});    
```

### Publish events using the Streaming Producer

```csharp
// Define handlers for the streaming events
    
async Task PublishSuccessfulHandlerAsync(PublishSucceededEventArgs args)
{
    foreach (var eventData in args.Events)
    {
        var partitionId = args.PartitionId
        Console.WriteLine($"Event: { eventData.EventBody } was published by partition { partitionId }.");
    }
}

protected async Task PublishFailedHandlerAsync(PublishFailedEventArgs args)
{
    Console.WriteLine("Publishing FAILED");
}

var connectionString = "<< CONNECTION STRING >>";
var eventHubName = "<< EVENT HUB NAME >>";

// Create the streaming producer with default options
var producer = new StreamingProducer(connectionString, eventHubName);

producer.PublishSucceeded += PublishSuccessfulHandlerAsync;    
producer.PublishFailed += PublishFailedHandlerAsync;

try
{
    while (TryGetNextEvent(out var eventData))
    {
        await producer.QueueForSendAsync(eventData);
        Console.WriteLine($"There are { producer.TotalQueuedEventCount } events queued for publishing.");
    }
}
finally
{   
    await producer.DisposeAsync();
}
```

## API
### `Azure.Messaging.EventHubs.Producer`

```csharp
// These are used by the producer to pass information to the application
// on the successful publish of a batch of events. 
public class PublishSucceededEventArgs : EventArgs
{
    public IEnumerable<EventData> Events { get; init; }
    public int PartitionId {get; init ; }
    public PublishSucceededEventArgs(IEnumerable<EventData> events);
}

public class PublishFailedEventArgs : EventArgs
{
    public IEnumerable<EventData> Events { get; init; }
    public Exception Exception { get; init; }
    public int PartitionId { get; init; }
    // This method signals that the given set of events should be resent upon a transient failure. This method should be called
    // inside of the user defined fail handler. Attempts to resend the events are done in the background independent of the
    // return of this call.
    public void ForceResend()


    // True if the application would like to force the producer to try to send an event again
    public PublishFailedEventArgs(IEnumerable<EventData> events, Exception ex, int partitionId);
}

public class StreamingProducerOptions : EventHubProducerClientOptions
{
    public TimeSpan? MaximumWaitTime { get; set; }
    public int MaximumQueuedEventCount { get; set; }   
    public boolean EnableIdempotentRetries { get; set; }   
    public boolean MaximumConcurrentSendsPerPartition { get; set; }
}

public class StreamingProducer : IAsyncDisposable
{
    public event Action<StreamingProducer, PublishSucceededEventArgs> PublishSucceeded;
    public event Action<StreamingProducer, PublishFailedEventArgs> PublishFailed;

    public string FullyQualifiedNamespace { get; }
    public string EventHubName { get; }
    public string Identifier { get; }
    public int TotalQueuedEventCount { get; }
    public bool IsClosed { get; protected set; }
    
    public StreamingProducer(string connectionString, string eventHubName = default , StreamingProducerOptions streamingOptions = default);
    public StreamingProducer(string fullyQualifiedNamespace, string eventHubName, AzureNamedKeyCredential credential, StreamingProducerOptions streamingOptions = default);
    public StreamingProducer(string fullyQualifiedNamespace, string eventHubName, AzureSasCredential credential, StreamingProducerOptions streamingOptions = default);
    public StreamingProducer(string fullyQualifiedNamespace, string eventHubName, TokenCredential credential, StreamingProducerOptions streamingOptions = default);
    public StreamingProducer(EventHubConnection connection, EventHubProducerClientOptions clientOptions = default);
    
    public int GetPartitionQueuedEventCount(string partition = default);

    // Upon the first call to queue, the producer is finalized and cannot 
    // be altered 
    // The queue is bounded and has limited capacity to prevent unchecked 
    // growth.  When the queue is full, this call waits for room.
    // Sending will take place in the background, transparent to the caller.
    public virtual Task QueueForSendAsync(EventData eventData, CancellationToken cancellationToken, SendEventOptions options = default);
    
    
    // This call tries to queue an event, but if the queue is full 
    // it returns a boolean indicating whether adding to the queue was successful
    public virtual Boolean TryQueueForSend(EventData eventData, CancellationToken cancellationToken, SendEventOptions options = default);

    // This flushes the queue by attempting to send all events 
    // 
    public virtual Task FlushAsync(CancellationToken cancellationToken);

    // Performs the tasks needed to clean up all the resources used by the producer, it also 
    // flushes the queue, sending any events that are still pending.
    public virtual ValueTask CloseAsync(CancellationToken cancellationToken);

    // Performs the tasks needed to clean up all the resources used by the producer, it also 
    // flushes the queue, sending any events that are still pending.
    public virtual ValueTask DisposeAsync(CancellationToken cancellationToken);
    
    // This method cancels any active sending and clears everything 
    // that is currently in the queue. 
    public virtual Task Clear(CancellationToken cancellationToken);

    protected virtual void OnSendSucceededAsync(IEnumerable<EventData> events);
    protected virtual void OnSendFailedAsync(IEnumerable<EventData> events, Exception ex, int partitionId);
}
```