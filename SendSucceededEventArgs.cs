// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Azure.Messaging.EventHubs;

namespace Azure.Messaging.EventHubs.Producer
{
    /// <summary>
    /// This class allows the application and the producer to pass contextual information
    /// back and forth on the event of a batch after being sent to the consumer successfully.
    /// </summary>
    public class SendSucceededEventArgs : EventArgs
    {
        /// <summary>
        /// Events is an IEnumerable of the events in the EventDataBatch that were sent.
        /// </summary>
        public IEnumerable<EventData> Events { get; set; }

        /// <summary>
        /// PartitionId is the id of the partition that the EventDataBatch was sent to.
        /// </summary>
        public string PartitionId { get; set; }
    }
}
