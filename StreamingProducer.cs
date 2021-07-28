  // Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;

namespace Azure.Messaging.EventHubs.Producer
{
    /// <summary>
    /// The streaming producer is responsible for sending <see cref="EventData"/> instances to a
    /// given Event Hub. The instances are added to queue and then batched and sent in the background.
    /// When adding events to the queue, options can be specified to determine which partition the
    /// event gets sent to.
    /// </summary>
    ///
    /// <remarks>
    /// <para>
    /// The difference between the <see cref="StreamingProducer"/> and the <see cref="EventHubProducerClient"/>
    /// is that the <see cref="StreamingProducer"/> batches events from a queue, rather than requiring the
    /// application to batch events before sending. The <see cref="StreamingProducer"/> utilizes timeouts.
    /// </para>
    ///
    /// <para>
    /// The <see cref="StreamingProducer"/> is safe to cache and use for the lifetime of an application,
    /// which is the recommended approach.
    /// </para>
    /// </remarks>
    public class StreamingProducer : IAsyncDisposable
    {
        private EventHubProducerClient Producer;
        private ConcurrentDictionary<string, ConcurrentQueue<EventData>> PartitionQueues;
        private ConcurrentDictionary<string, EventDataBatch> PendingBatches;
        private ConcurrentQueue<EventData> GeneralQueue;
        private EventDataBatch GeneralPendingBatch;
        private StreamingProducerOptions _options;
        private bool IsStarted;
        private event Action<SendSucceededEventArgs> _sendSucceeded;
        private event Action<SendFailedEventArgs> _sendFailed;

        /// <summary>
        /// A handler for event batches that have been sent successfully. Control is passed to this method after events have successfully
        /// been sent to the consumer, along with an instance of <see cref="SendSucceededEventArgs"/> containing data about the events.
        /// </summary>
        public event Action<SendSucceededEventArgs> SendSucceeded
        {
            add
            {
                Argument.AssertNotNull(value, nameof(SendSucceeded));

                if (_sendSucceeded != default)
                {
                    throw new NotSupportedException(Resources.HandlerHasAlreadyBeenAssigned);
                }
                _sendSucceeded = value;
            }

            remove
            {
                Argument.AssertNotNull(value, nameof(SendSucceeded));

                if (_sendSucceeded != value)
                {
                    throw new ArgumentException(Resources.HandlerHasNotBeenAssigned);
                }
                _sendSucceeded = default;
            }
        }

        /// <summary>
        /// A handler for event batches that have failed to successfully send. If a transient failure occured during send,
        /// this method will only be called after applying the retry policy. However, control is passed to this method on the event that a batch
        /// has failed to be sent to the consumer for any reason, transient or permanent, along with an instance of
        /// <see cref="SendFailedEventArgs"/> containing the the batch of events that failed and the exception.
        /// </summary>
        public event Action<SendFailedEventArgs> SendFailed
        {
            add
            {
                Argument.AssertNotNull(value, nameof(SendFailed));

                if (_sendFailed != default)
                {
                    throw new NotSupportedException(Resources.HandlerHasAlreadyBeenAssigned);
                }
                _sendFailed = value;
            }

            remove
            {
                Argument.AssertNotNull(value, nameof(SendFailed));

                if (_sendFailed != value)
                {
                    throw new ArgumentException(Resources.HandlerHasNotBeenAssigned);
                }
                _sendFailed = default;
            }
        }

        /// <summary>
        /// The fully qualified Event Hubs namespace that this producer is currently associated with, which will likely be similar
        /// to <c>{yournamespace}.servicebus.windows.net</c>.
        /// </summary>
        public string FullyQualifiedNamespace => Producer.FullyQualifiedNamespace;

        /// <summary>
        /// The name of the Event Hub that this producer is connected to, specific to the Event Hubs namespace that contains it.
        /// </summary>
        public string EventHubName => Producer.EventHubName;

        /// <summary>
        /// A unique name to identify the streaming producer.
        /// </summary>
        //public string Identifier => Producer.Identifier;
        public string Identifier { get; }

        /// <summary>
        /// The total number of events that are currently in the queue waiting to be sent, across all partitions.
        /// </summary>
        public virtual int TotalPendingEventCount { get; protected set; }

        /// <summary>
        /// <c>true</c> if the streaming producer has been closed <c>false</c> otherwise.
        /// </summary>
        public bool IsClosed => Producer.IsClosed;

        /// <summary>
        /// Returns the number of events present in the queue waiting to be sent for a given partition.
        /// </summary>
        /// <param name="partition">The id of the partition.</param>
        public int GetPartitionQueuedEventCount(string partition) => PartitionQueues[partition].Count;
        // TODO: is partition key a string? how to deal with hashing?

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamingProducer" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string to use for connecting to the Event Hubs namespace;
        /// it is expected that the Event Hub name and the shared key properties are contained in this connection string.</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the producer with.</param>
        /// <param name="clientOptions">A set of <see cref="StreamingProducerOptions"/> to apply when configuring the streaming producer.</param>
        public StreamingProducer(string connectionString, string eventHubName = default, StreamingProducerOptions clientOptions = default) : this(clientOptions)
        {
            Producer = new EventHubProducerClient(connectionString, eventHubName, clientOptions);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamingProducer" /> class.
        /// </summary>
        /// <param name="fullyQualifiedNamespace">The fully qualified Event Hubs namespace to connect to.  This is likely to be similar to <c>{yournamespace}.servicebus.windows.net</c>.</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the producer with.</param>
        /// <param name="credential">The shared access key credential to use for authorization.  Access controls may be specified by the Event Hubs namespace or the requested Event Hub, depending on Azure configuration.</param>
        /// <param name="clientOptions">A set of <see cref="StreamingProducerOptions"/> to apply when configuring the streaming producer.</param>
        public StreamingProducer(string fullyQualifiedNamespace, string eventHubName, AzureNamedKeyCredential credential, StreamingProducerOptions clientOptions = default) : this(clientOptions)
        {
            Producer = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, credential, clientOptions);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamingProducer" /> class.
        /// </summary>
        /// <param name="fullyQualifiedNamespace">The fully qualified Event Hubs namespace to connect to.  This is likely to be similar to <c>{yournamespace}.servicebus.windows.net</c>.</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the producer with.</param>
        /// <param name="credential">The shared access key credential to use for authorization.  Access controls may be specified by the Event Hubs namespace or the requested Event Hub, depending on Azure configuration.</param>
        /// <param name="clientOptions">A set of <see cref="StreamingProducerOptions"/> to apply when configuring the streaming producer.</param>
        public StreamingProducer(string fullyQualifiedNamespace, string eventHubName, AzureSasCredential credential, StreamingProducerOptions clientOptions = default) : this(clientOptions)
        {
            Producer = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, credential, clientOptions);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamingProducer" /> class.
        /// </summary>
        /// <param name="fullyQualifiedNamespace">The fully qualified Event Hubs namespace to connect to.  This is likely to be similar to <c>{yournamespace}.servicebus.windows.net</c>.</param>
        /// <param name="eventHubName">The name of the specific Event Hub to associate the producer with.</param>
        /// <param name="credential">The shared access key credential to use for authorization.  Access controls may be specified by the Event Hubs namespace or the requested Event Hub, depending on Azure configuration.</param>
        /// <param name="clientOptions">A set of <see cref="StreamingProducerOptions"/> to apply when configuring the streaming producer.</param>
        public StreamingProducer(string fullyQualifiedNamespace, string eventHubName, TokenCredential credential, StreamingProducerOptions clientOptions = default) : this(clientOptions)
        {
            Producer = new EventHubProducerClient(fullyQualifiedNamespace, eventHubName, credential, clientOptions);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamingProducer" /> class.
        /// </summary>
        /// <param name="connection">The <see cref="EventHubConnection" /> connection to use for communication with the Event Hubs service.</param>
        /// <param name="clientOptions">A set of <see cref="StreamingProducerOptions"/> to apply when configuring the streaming producer.</param>
        public StreamingProducer(EventHubConnection connection, StreamingProducerOptions clientOptions = default): this(clientOptions)
        {
            Producer = new EventHubProducerClient(connection, clientOptions);
        }

        /// <summary>
        /// An internal method to set up options and create data structures.
        /// </summary>
        /// <param name="options"></param>
        private StreamingProducer(StreamingProducerOptions options)
        {
            if (options != null)
            {
                _options = options.Clone();
            }
            else
            {
                _options = new StreamingProducerOptions();
            }
            PartitionQueues = new ConcurrentDictionary<string, ConcurrentQueue<EventData>>();
            GeneralQueue = new ConcurrentQueue<EventData>();
            PendingBatches = new ConcurrentDictionary<string, EventDataBatch>();
        }

        /// <summary>
        /// Asynchronously adds an <see cref="EventData"/> instance to the queue to be sent to the consumer.
        /// </summary>
        /// <param name="eventData"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="options"></param>
        /// <remarks>
        /// <para>
        /// Upon the first call to queue, the producer is finalized and cannot be altered, e.g. the send success and failures
        /// handlers cannot be changed. The queue is bounded and has limited capacity to prevent unchecked growth. When the queue
        /// is full, this call waits for room.
        /// </para>
        ///
        /// <para>
        /// When this call returns, the event will have been added to the queue, but may not have been sent yet. Sending the event
        /// occurs in the background. After attempting to send, the appropriate handler will be called, either <see cref="SendSucceeded"/>
        /// if the events are sent successfully, or <see cref="SendFailed"/> if the events fail to send.
        /// </para>
        /// </remarks>
        public async Task QueueForSendAsync(EventData eventData, SendEventOptions options = default, CancellationToken cancellationToken = default)
        {
            if (!IsStarted)
            {
                StartInternal();
            }

            CreateBatchOptions batchOptions = options != null ? new CreateBatchOptions
            {
                PartitionId = options.PartitionId,
                PartitionKey = options.PartitionKey
            } : null;

            string partition = options?.PartitionId;
            string key = options?.PartitionKey;
            if (key != default)
            {
                partition = default;
            }
            ConcurrentQueue<EventData> queue = GeneralQueue;
            if (partition != null)
            {
                var exists = PartitionQueues.TryGetValue(partition, out queue);
                if (!exists)
                {
                    queue = new ConcurrentQueue<EventData>();
                    PartitionQueues.TryAdd(partition, queue);
                }
            }
            await QueueForSendInternal(queue, eventData, cancellationToken).ConfigureAwait(false);
            await BatchForSendInternalAsync(queue, cancellationToken, partition, batchOptions).ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronously add an <see cref="EventData"/> instance to the queue to be sent to the consumer.
        /// </summary>
        /// <param name="eventData">The individual <see cref="EventData"/> instance to be sent to the consumer.</param>
        /// <param name="cancellationToken">An optional <see cref="CancellationToken" /> instance to signal the request to cancel the operation.</param>
        /// <param name="options">A set of <see cref="SendEventOptions"/> to configure the send to the consumer.</param>
        /// <remarks>
        /// Upon the first call to queue, the producer is finalized and cannot be altered. The queue is bounded and has limited
        /// capacity to prevent unchecked growth. When the queue is full, this call returns false. Sending will take place in the
        /// background, transparent to the caller.
        /// </remarks>
        /// <returns><c>true</c> if queuing the event was successful <c>false</c> otherwise.</returns>
        public virtual Boolean TryQueueForSend(EventData eventData, SendEventOptions options = default, CancellationToken cancellationToken = default)
        {
            if (!IsStarted)
            {
                StartInternal();
            }
            string partition = options?.PartitionId;
            string key = options?.PartitionKey;
            if (key != default)
            {
                partition = default;
            }
            ConcurrentQueue<EventData> queue = GeneralQueue;
            if (partition != null)
            {
                var exists = PartitionQueues.TryGetValue(partition, out queue);
                if (!exists)
                {
                    queue = new ConcurrentQueue<EventData>();
                    PartitionQueues.TryAdd(partition, queue);
                }
            }
            //var queuedSuccessful = TryQueueForSendInternal(queue, eventData);
            var toAdd = new List<EventData>();
            toAdd.Add(eventData);
            OnSendSucceededAsync(toAdd, default);
            return true;
        }

        /// <summary>
        /// This method flushes the queue by attempting to send all events that are currently in the queue. Each send applies the retry policy
        /// when necessary, and on successful or failed send, the appropriate <see cref="SendSucceeded"/> or <see cref="SendFailed"/> handler
        /// will be called. Upon completion of this method, all events will have been removed from the queues and either sent or dealt with according
        /// to the <see cref="SendFailed"/> handler.
        /// </summary>
        /// <param name="cancellationToken">An optional <see cref="CancellationToken" /> instance to signal the request to cancel the operation.</param>
        public async virtual Task FlushAsync(CancellationToken cancellationToken = default)
        {
            // Batch all of the events in the partition queues
            foreach (var queue in PartitionQueues)
            {
                while (!queue.Value.IsEmpty)
                {
                    await BatchForSendInternalAsync(queue.Value, cancellationToken).ConfigureAwait(false);
                }
            }

            // Batch all of the events in the general queue
            while (!GeneralQueue.IsEmpty)
            {
                await BatchForSendInternalAsync(GeneralQueue, cancellationToken).ConfigureAwait(false);
            }

            // Try to send all of the partition batches if they haven't been sent already
            foreach (var batch in PendingBatches)
            {
                if (batch.Value != null && batch.Value.Count > 0)
                {
                    try
                    {
                        await Producer.SendAsync(batch.Value, cancellationToken).ConfigureAwait(false);
                        OnSendSucceededAsync(batch.Value.AsEnumerable<EventData>(), default);
                    }
                    catch (Exception ex)
                    {
                        await OnSendFailedAsync(batch.Value.AsEnumerable<EventData>(), ex).ConfigureAwait(false);
                    }
                }
            }

            // Try to send the general batch if it hasn't been sent already
            if (GeneralPendingBatch != null && GeneralPendingBatch.Count > 0)
            {
                try
                {
                    await Producer.SendAsync(GeneralPendingBatch, cancellationToken).ConfigureAwait(false);
                    OnSendSucceededAsync(GeneralPendingBatch.AsEnumerable<EventData>(), default);
                }
                catch (Exception ex)
                {
                   await OnSendFailedAsync(GeneralPendingBatch.AsEnumerable<EventData>(), ex).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Performs the tasks needed to clean up all the resources used by the <see cref="StreamingProducer"/>, including ensuring that
        /// the streaming producer itself has been closed.
        /// </summary>
        /// <remarks>
        /// Calling this method will also <see cref="Clear(CancellationToken)"/> the queue, which will clear any events that are still pending,
        /// and stop any active sending.
        /// </remarks>
        /// <param name="cancellationToken">An optional <see cref="CancellationToken" /> instance to signal the request to cancel the operation.</param>
        public virtual ValueTask CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return DisposeAsync();
        }

        /// <summary>
        /// This method cancels any active sending and clears all events that are currently waiting to be sent in the queue.
        /// </summary>
        /// <param name="cancellationToken">An optional <see cref="CancellationToken" /> instance to signal the request to cancel the operation.</param>
        public virtual Task Clear(CancellationToken cancellationToken = default)
        {
            PendingBatches.Clear();
            GeneralPendingBatch?.Clear();
            PartitionQueues.Clear();
            GeneralQueue = new ConcurrentQueue<EventData>();

            return Task.CompletedTask;
        }

        /// <summary>
        /// This method is called upon the successful send of a batch of events. It calls the <see cref="SendSucceeded"/> event handler.
        /// </summary>
        /// <param name="events">An <see cref="IEnumerable{EventData}"/> instance holding all of the <see cref="EventData"/> instances within the
        /// batch that was successfully sent.</param>
        /// <param name="partitionId">The partition that the batch of events was sent to.</param>
        protected virtual void OnSendSucceededAsync(IEnumerable<EventData> events, string partitionId)
        {
            TotalPendingEventCount -= events.Count<EventData>();
            SendSucceededEventArgs args = new SendSucceededEventArgs
            {
                Events = events,
                PartitionId = partitionId
            };
            if (_sendSucceeded != null)
            {
                _sendSucceeded(args);
            }
        }

        /// <summary>
        /// This method is called upon the failed send of a batch of events, after applying the retry policy if there was a transient failure.
        /// It calls the <see cref="SendFailed"/> event handler.
        /// </summary>
        /// <param name="events">An <see cref="IEnumerable{EventData}"/> instance holding all of the <see cref="EventData"/> instances within the
        /// batch that was successfully sent</param>
        /// <param name="ex">The <see cref="Exception"/> that was raised when the events failed to send.</param>
        /// <param name="partitionId">The partition that the batch of events was sent to.</param>
        protected virtual async Task OnSendFailedAsync(IEnumerable<EventData> events, Exception ex, string partitionId = default)
        {
            TotalPendingEventCount -= events.Count<EventData>();
            SendFailedEventArgs args = new SendFailedEventArgs
            {
                Events = events,
                Exception = ex,
                PartitionId = partitionId,
            };
            _sendFailed(args);
            if (args.RetrySend)
            {
                try
                {
                    await Producer.SendAsync(events).ConfigureAwait(false);
                    OnSendSucceededAsync(events, default);
                }
                catch (Exception exc)
                {
                    await OnSendFailedAsync(events, exc, partitionId).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Performs the tasks needed to clean up all the resources used by the <see cref="StreamingProducer"/>, including ensuring that
        /// the streaming producer itself has been closed.
        /// </summary>
        /// <remarks>
        /// Calling this method will also clear the queue, which will drop any events that are still pending.
        /// </remarks>
        public ValueTask DisposeAsync()
        {
            Clear();
            GeneralPendingBatch?.Dispose();
            foreach (var batch in PendingBatches)
            {
                batch.Value.Dispose();
            }
            GC.SuppressFinalize(this);
            return Producer.DisposeAsync();
        }

        private async Task QueueForSendInternal(ConcurrentQueue<EventData> theQueue, EventData theEvent, CancellationToken cancellationToken)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                await Task.Run(() =>
                {
                    var sent = false;
                    while (!sent)
                    {
                        sent = TryQueueForSendInternal(theQueue, theEvent);
                        return;
                    };
                }, cancellationToken).ConfigureAwait(false);
                return;
            }
        }

        private bool TryQueueForSendInternal(ConcurrentQueue<EventData> theQueue, EventData theEvent)
        {
            if (theQueue.Count < _options.MaximumQueuedEventCount)
            {
                theQueue.Enqueue(theEvent);
                TotalPendingEventCount++;
                return true;
            }
            return false;
        }

        private void StartInternal()
        {
            IsStarted = true;
            if (_sendFailed == default)
            {
                throw new Exception("Fail Handler must be defined");
            }
        }

        private async Task BatchForSendInternalAsync(ConcurrentQueue<EventData> newestQueue, CancellationToken cancellationToken, string partition = null, CreateBatchOptions batchOptions = default)
        {
            var added = false;
            var batch = GeneralPendingBatch;
            if (partition != null)
            {
                PendingBatches.TryGetValue(partition, out batch);
            }
            batch ??= await Producer.CreateBatchAsync(batchOptions, cancellationToken).ConfigureAwait(false);
            newestQueue.TryDequeue(out EventData result);
            try
            {
                added = (result == null) ? true : batch.TryAdd(result);
                if (!added && batch.Count > 0)
                {
                    //if too large and batch not empty, then send the batch
                    await Producer.SendAsync(batch, cancellationToken).ConfigureAwait(false);
                    OnSendSucceededAsync(batch.AsEnumerable<EventData>(), default);
                    //the first event has been dequeued so need to try again, start by making a new batch
                    using var newbatch = await Producer.CreateBatchAsync(batchOptions, cancellationToken).ConfigureAwait(false);
                    if (partition == null)
                    {
                        GeneralPendingBatch ??= newbatch;
                    }
                    else
                    {
                        PendingBatches[partition] ??= newbatch;
                    }
                    added = newbatch.TryAdd(result);
                }
                else
                {
                    if (partition == null)
                    {
                        GeneralPendingBatch = batch;
                    }
                    else
                    {
                        PendingBatches[partition] = batch;
                    }
                }

                if (!added && batch.Count == 0)
                {
                    throw new Exception(); // TODO - likely event is too large to fit in a batch
                }
            }
            catch (Exception ex)
            {
                await OnSendFailedAsync(batch.AsEnumerable<EventData>(), ex).ConfigureAwait(false);
            }
        }
    }
}
