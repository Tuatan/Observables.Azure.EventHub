namespace Observables.Azure
{
    using System;
    using System.Linq;
    using System.Reactive.Disposables;
    using System.Reactive.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// This type that exposes IoT Event Hub instance as an observable source.
    /// </summary>
    public sealed class EventHubListener : IObservable<EventData>
    {
        private readonly string eventHubName;

        private readonly string eventHubConnectionString;

        private readonly string consumerGroupName;

        private readonly TimeSpan period;

        private EventHubConsumerGroup consumerGroup;

        private readonly IObservable<EventData> source;

        /// <summary>
        /// Creates a new instance of the EventHubsListener using the specified connection string.
        /// </summary>
        /// 
        /// <returns>
        /// The newly created <see cref="T:Ecs.EventHubs.EventHubsListener"/> object.
        /// </returns>
        /// <param name="eventHubName">The path to the Event Hub from which to start receiving event data.</param>   
        /// <param name="eventHubConnectionString">The connection string for the Event Hub.</param>
        /// <param name="consumerGroupName">The consumer group name.</param>
        /// <param name="pollingPeriod">Polling interval.</param>
        public EventHubListener(
            string eventHubName,
            string eventHubConnectionString,
            string consumerGroupName,
            TimeSpan pollingPeriod)
        {
            if (eventHubName == null)
            {
                throw new ArgumentNullException(nameof(eventHubName));
            }
            if (eventHubConnectionString == null)
            {
                throw new ArgumentNullException(nameof(eventHubConnectionString));
            }

            this.eventHubName = eventHubName;
            this.eventHubConnectionString = eventHubConnectionString;
            this.consumerGroupName = consumerGroupName;
            this.period = pollingPeriod;

            this.source = Observable.Create((Func<IObserver<EventData>, CancellationToken, Task<IDisposable>>)this.CreateStream)
               .Publish()
               .RefCount();
        }

        /// <summary>
        /// Notifies the provider that an observer is to receive notifications.
        /// </summary>
        /// 
        /// <returns>
        /// A reference to an interface that allows observers to stop receiving notifications before the provider has finished sending them.
        /// </returns>
        /// <param name="observer">The object that is to receive notifications.</param>
        public IDisposable Subscribe(IObserver<EventData> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            return this.source.Subscribe(observer);
        }

        private async Task<IDisposable> CreateStream(IObserver<EventData> observer, CancellationToken cancellationToken)
        {
            var eventHubClient = EventHubClient.CreateFromConnectionString(
                this.eventHubConnectionString,
                this.eventHubName);

            this.consumerGroup = string.IsNullOrEmpty(this.consumerGroupName)
                ? eventHubClient.GetDefaultConsumerGroup()
                : eventHubClient.GetConsumerGroup(this.consumerGroupName);

            var receiver = await this.consumerGroup.CreateReceiverAsync("7").ConfigureAwait(false);

            while (!cancellationToken.IsCancellationRequested)
            {
                var items = await receiver.ReceiveAsync(10).ConfigureAwait(false);

                var eventDatas = items as EventData[] ?? items.ToArray();

                if (eventDatas.Length != 0)
                {
                    foreach (var item in eventDatas)
                    {
                        observer.OnNext(item);
                    }
                }
                else
                {
                    await Task.Delay(this.period, cancellationToken).ConfigureAwait(false);
                }
            }

            return Disposable.Create(receiver.Close);
        }
    }
}
