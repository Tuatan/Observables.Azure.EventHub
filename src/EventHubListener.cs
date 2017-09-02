namespace Observables.Azure
{
    using System;
    using System.Linq;
    using System.Reactive.Disposables;
    using System.Reactive.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Azure.EventHubs;

    /// <summary>
    /// This type that exposes Azure Event Hub instance as an observable source.
    /// </summary>
    public sealed class EventHubListener : IObservable<EventData>
    {
        private readonly string eventHubName;

        private readonly string eventHubConnectionString;

        private readonly string consumerGroupName;

        private readonly TimeSpan period;

        private readonly int batchSize;

        private readonly string partitionId;

        private PartitionReceiver receiver;

        private readonly IObservable<EventData> source;

        public static IObservable<EventData> Create(
            string eventHubName,
            string eventHubConnectionString,
            string partitionId)
        {
            return new EventHubListener(
                eventHubName,
                eventHubConnectionString,
                PartitionReceiver.DefaultConsumerGroupName,
                partitionId,
                TimeSpan.FromSeconds(30),
                10
            );
        }

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
            string partitionId,
            TimeSpan pollingPeriod,
            uint batchSize)
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
            this.consumerGroupName = string.IsNullOrEmpty(consumerGroupName)
                ? PartitionReceiver.DefaultConsumerGroupName
                : consumerGroupName;

            this.period = pollingPeriod;
            this.batchSize = checked((int)batchSize);
            this.partitionId = partitionId;

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
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(this.eventHubConnectionString)
            {
                EntityPath = this.eventHubName
            };

            var client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            var runTimeInformation = await client.GetRuntimeInformationAsync().ConfigureAwait(false);

            if (!runTimeInformation.PartitionIds.Contains(this.partitionId))
            {
                observer.OnError(new ArgumentException($"Provided partition ID ({this.partitionId}) does not exist on server")); 
                return Task.FromResult(Disposable.Empty);
            }

            this.receiver = client.CreateReceiver(this.consumerGroupName, partitionId, PartitionReceiver.EndOfStream);

            while (!cancellationToken.IsCancellationRequested)
            {
                var items = await this.receiver.ReceiveAsync(this.batchSize).ConfigureAwait(false);

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

            return new CompositeDisposable(
                Disposable.Create(this.receiver.Close),
                Disposable.Create(client.Close));
        }
    }
}