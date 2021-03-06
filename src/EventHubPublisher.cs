namespace Observables.Azure
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Azure.EventHubs;

    /// <summary>
    /// Exposes Event Hub publications as a sink.
    /// </summary>
    public sealed class EventHubPublisher : IObserver<EventData>, IDisposable
    {
        private EventHubClient client;

        /// <summary>
        /// Creates a new instance of the EventHubsPublisher.
        /// </summary>
        /// <param name="eventHubConnectionString">The connection string to be used.</param>
        /// <param name="eventHubName">The Event Hub entity path.</param>
        /// <exception cref="ArgumentNullException">Thrown if any of the parameters are null.</exception>
        public EventHubPublisher(
            string eventHubConnectionString,
            string eventHubName)
        {
            if (string.IsNullOrWhiteSpace(eventHubConnectionString))
            {
                throw new ArgumentNullException(nameof(eventHubConnectionString));
            }

            if (string.IsNullOrWhiteSpace(eventHubName))
            {
                throw new ArgumentNullException(nameof(eventHubName));
            }

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(eventHubConnectionString)
            {
                EntityPath = eventHubName
            };

            this.client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
        }

        /// <summary>
        /// Provides the observer with new data.
        /// </summary>
        /// <param name="value">The current notification information.</param>
        public void OnNext(EventData value)
        {
            this.OnNextAsync(value).Wait();
        }

        /// <summary>
        /// Notifies the observer that the provider has experienced an error condition.
        /// </summary>
        /// <param name="error">An object that provides additional information about the error.</param>
        public void OnError(Exception error)
        {
            this.Dispose();
        }

        /// <summary>
        /// Notifies the observer that the provider has finished sending push-based notifications.
        /// </summary>
        public void OnCompleted()
        {
            this.Dispose();
        }

        /// <summary>
        /// Provides the observer with new data.
        /// </summary>
        /// <param name="value">
        /// The newly produced data.
        /// </param>
        /// <returns>
        /// A Task that completes when the observer has finished processing <paramref name="value"/>.
        /// </returns>
        public Task OnNextAsync(EventData value)
        {
            return this.client.SendAsync(value);
        }

        public Task OnErrorAsync(Exception error)
        {
            this.Dispose();
            return Task.FromResult(1);
        }

        public Task OnCompletedAsync()
        {
            this.Dispose();
            return Task.FromResult(1);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            var clientToDispose = Interlocked.Exchange(ref this.client, null);
            if (clientToDispose != null)
            {
                try
                {
                    clientToDispose.Close();
                }
                catch
                {
                    // ignored
                }
            }
        }
    }
}