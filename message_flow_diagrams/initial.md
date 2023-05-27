```mermaid
sequenceDiagram
    actor Producer
    participant Producer Connection Accepter
    participant Producer Connection Handler
    participant Exchange
    participant Topic Handler
    participant Consumer Connection Handler
    participant Consumer Connection Accepter
    actor Consumer

    Consumer->>Consumer Connection Accepter: Connect
    Consumer->>Consumer Connection Handler: Subscribe for Topic as User
    Consumer Connection Handler->>Topic Handler: Subscribe for Topic
    Producer->>Producer Connection Accepter: Connect
    Producer->>Producer Connection Handler: Publish Topic & Message
    Producer Connection Handler->>Exchange: Message & Topic
    Exchange->>Topic Handler: Categorized Message for Topic
    Topic Handler->>Consumer Connection Handler: Message for the Subscribed Topic
    Consumer Connection Handler->>Consumer: Message for the Subscribed Topic  
```