# Streaming Producer
## Overview
The primary goal of the **streaming producer** is to provide developers the ability to queue individual events for publishing without the need to explicitly manage batch construction, population, or service operations.  

Events are collected as they are queued, organized into batches, and sent by the streaming producer in the following two cases:
1. When the streaming producer has a full batch to send to a particular partition.
2. The streaming producer has not received an event for a particular partition in a given amount of time, by default this is 250 milliseconds, but this value is customizable.

When queuing events, developers may request automatic routing to a partition or explicitly control the partition in the same manner supported by the `ProducerClient`, with the streaming producer managing the details of grouping events into the appropriate batches for publication.

## Key concepts

- Each event queued for publishing is considered individual, it is not possible to require distinct events to be published in the same batch.
- The Streaming producer has support for the same set of constructors as the `EventHubProducerClient` and attempts to create continuity of concepts.
- After events are queued for sending, the actual sending occurs in the **background**. 
- Event handlers for the successful or failed send of a given batch should be used in the typical .NET event handler pattern. If the events failed to send due to a transient failure, the fail handler is only called after applying the retry policy and the application can attempt the retry policy again. 


## Client lifetime
Each of the Event Hubs client types is safe to cache and use as a singleton for the lifetime of the application, which is best practice when events are being published or read regularly. The clients are responsible for efficient management of network, CPU, and memory use, working to keep usage low during periods of inactivity. Calling either `CloseAsync` or `DisposeAsync` on a client is required to ensure that network resources and other unmanaged objects are properly cleaned up.

## Usage examples

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
    MaximumQueuedEventLimit = 500
    RetryOptions = new EventHubsRetryOptions { TryTimeout = TimeSpan.FromMinutes(5) }
});    
```


### Pattern for publishing events using the Streaming Producer

```csharp
// Accessing the Event Hub
    var connectionString = "<< CONNECTION STRING >>";
    var eventHubName = "<< EVENT HUB NAME >>";

    // Create the streaming producer
    var producer = new StreamingProducer(connectionString, hub);

    // Define the Handlers
    Task SendEventBatchSuccessHandler(SendEventBatchSuccessEventArgs args)
    {
        Console.WriteLine($"The batch was published by { args.PartitionId }:");
        foreach (var eventData in args.EventBatch)
        {
            Console.WriteLine($"Event: { eventData.EventBody }");
        }

        return Task.CompletedTask;
    }

    Task SendEventBatchFailHandler(SendEventBatchFailedEventArgs args)
    {
        Console.WriteLine($"Publishing FAILED due to { args.Exception.Message } in partition { args.PartitionId }");

        return Task.CompletedTask;
    }

    // Add the handlers to the producer
    producer.SendEventBatchSuccessAsync += SendEventBatchSuccessHandler;
    producer.SendEventBatchFailedAsync += SendEventBatchFailHandler;

    try
    {
        // Queue a set of events, this call waits until there is room on the queue, but the events are actually sent in the background
        for (var eventNum = 0; eventNum < 10; eventNum++)
        {
            await producer.EnqueueEventAsync(new EventData($"Event #{ eventNum }"));
        }
    }
    finally
    {
        // Close sends all pending queued events and then shuts down the producer
        await producer.CloseAsync();
    }
```

### Pattern for publishing events using the Streaming Producer

```csharp
// Accessing the Event Hub
    var connectionString = "<< CONNECTION STRING >>";
    var eventHubName = "<< EVENT HUB NAME >>";

    // Create the streaming producer
    var producer = new StreamingProducer(connectionString, hub);

    // Define the Handlers
    Task SendEventBatchSuccessHandler(SendEventBatchSuccessEventArgs args)
    {
        Console.WriteLine($"The batch was published by { args.PartitionId }:");
        foreach (var eventData in args.EventBatch)
        {
            Console.WriteLine($"Event: { eventData.EventBody }");
        }

        return Task.CompletedTask;
    }

    Task SendEventBatchFailHandler(SendEventBatchFailedEventArgs args)
    {
        Console.WriteLine($"Publishing FAILED due to { args.Exception.Message } in partition { args.PartitionId }");

        return Task.CompletedTask;
    }

    // Add the handlers to the producer
    producer.SendEventBatchSuccessAsync += SendEventBatchSuccessHandler;
    producer.SendEventBatchFailedAsync += SendEventBatchFailHandler;

    try
    {
        // Queue a set of events, this call only queues if there is space on the queue, the events are still sent in the background
        for (var eventNum = 0; eventNum < 10; eventNum++)
        {
            var added = producer.TryEnqueueEventWithoutWaiting(new EventData($"Event #{ eventNum }"));
            
            if (!added)
            {
                // The event could not be added.
                throw new Exception($"Event { eventNum } couldn't be enqueued because the queue was full.");
            }
        }
    }
    finally
    {
        // Close sends all pending queued events and then shuts down the producer
        await producer.CloseAsync();
    }
```