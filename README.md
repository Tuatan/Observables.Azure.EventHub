[![Nuget](https://img.shields.io/nuget/vpre/Observables.Azure.EventHub.svg)](https://www.nuget.org/packages/Observables.Azure.EventHub/)

# Observable Azure EventHub

That is an observable implementation of Azure EventHub based transport.

# Usage

Exposing an Azure Event Hub instance as a push source:

```csharp
new EventHubListener("myEventHub", "connectionString", string.Empty, TimeSpan.FromSeconds(15)).Dump();
```

Publish to an Azure Event Hub instance:
```csharp
var publisher = new EventHubPublisher("connectionString", "myEventHub");
publisher.OnNext(new EventData());
```
