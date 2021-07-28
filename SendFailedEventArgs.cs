// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;

namespace Azure.Messaging.EventHubs.Producer
{
    /// <summary>
    /// This class allows the application and the producer to pass contextual information
    /// back and forth on the event of a batch failing to be sent to the consumer successfully.
    /// </summary>
    public class SendFailedEventArgs : EventArgs
    {
        /// <summary>
        /// Events is an IEnumerable of the events in the EventDataBatch that failed to send.
        /// </summary>
        public IEnumerable<EventData> Events { get; set; }

        /// <summary>
        /// Exception is the Exception that was thrown when the EventDataBatch failed to send.
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// PartitionId is the id of the partition that the EventDataBatch was being sent to when it
        /// failed.
        /// </summary>
        public string PartitionId { get; set; }

        /// <summary>
        /// This method signals that the given set of events should be resent upon a transient failure. This method should be called
        /// inside of the user defined fail handler. Attempts to resend the events are done in the background independent of the
        /// return of this call.
        /// </summary>
        public void ForceResend()
        {
            RetrySend = true;
        }

        internal Boolean RetrySend { get; set; }
    }
}
