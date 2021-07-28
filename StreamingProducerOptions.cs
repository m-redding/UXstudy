// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;

namespace Azure.Messaging.EventHubs.Producer
{
    /// <summary>
    /// This class allows the application to customize the creation of the streaming producer.
    /// It extends <see cref="EventHubProducerClientOptions"/>.
    /// </summary>
    public class StreamingProducerOptions : EventHubProducerClientOptions
    {
        private int _maxConcurrentSends;

        /// <summary>
        /// MaximumWaitTime is the amount of time to wait for new events before sending a partially
        /// full batch. If <c>null</c> or empty the default of 250 milliseconds will be used.
        /// </summary>
        public TimeSpan? MaximumWaitTime { get; set; }

        /// <summary>
        /// MaximumQueuedEventCount is the maxiumum total number of events that can be queued across all
        /// partition queues. If not specified the default value of 200 events will be used.
        /// </summary>
        public int MaximumQueuedEventCount { get; set; }

        /// <summary>
        /// Indicates whether the application wants the streaming producer
        /// to retry sending idempotently, rather than the default guarantee of at least once delivery.
        /// If not specified the default value of <c>false</c> will be used. This value cannot be <c>true</c>
        /// if <see cref="MaximumConcurrentSendsPerPartition"/> is larger than 1.
        /// </summary>
        public Boolean EnableIdempotentRetries { get; set; }

        /// <summary>
        /// Indicates whether the application wants to allow the streaming
        /// producer to send multiple event batches concurrently within a single partition, rather than waiting for
        /// batches to be sent in order. If not specified the default value of <c>1</c> will
        /// be used. This value cannot be larger than 1 if <see cref="EnableIdempotentRetries"/>
        /// is true.
        /// </summary>
        public int MaximumConcurrentSendsPerPartition
        {
            get => _maxConcurrentSends;
            set
            {
                if (value < 1 || value > 100 || (EnableIdempotentRetries && value > 1))
                {
                    throw new Exception("MaximumConcurrentSendsPerPartition must be between 1 and 100, and cannot be larger than 1 if Idempotent Retries are enabled");
                }
                _maxConcurrentSends = value;
            }
        }

        /// <summary>
        /// This method creates a new <see cref="StreamingProducerOptions"/> with the default values.
        /// </summary>
        /// <returns>A new <see cref="StreamingProducerOptions"/> instance holding default values</returns>
        public StreamingProducerOptions()
        {
            MaximumWaitTime = TimeSpan.FromMilliseconds(250);
            MaximumQueuedEventCount = 200;
            EnableIdempotentRetries = false;
            MaximumConcurrentSendsPerPartition = 1;
        }

        internal new StreamingProducerOptions Clone()
        {
            var baseOptions = base.Clone();
            var copiedOptions = new StreamingProducerOptions
            {
                MaximumWaitTime = MaximumWaitTime,
                MaximumQueuedEventCount = MaximumQueuedEventCount,
                EnableIdempotentRetries = EnableIdempotentRetries,
                MaximumConcurrentSendsPerPartition = MaximumConcurrentSendsPerPartition,
                EnableIdempotentPartitions = EnableIdempotentPartitions,
            };

            foreach (var pair in PartitionOptions)
            {
                copiedOptions.PartitionOptions.Add(pair.Key, pair.Value.Clone());
            }

            return copiedOptions;
        }
    }
}
