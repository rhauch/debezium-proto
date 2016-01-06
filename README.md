Copyright 2014 Red Hat
Licensed under the Apache License, Version 2.0.

This is a prototype for a demonstration system that uses Kafka, stream processing, and multiple distributed services to store data for theoretical mobile applications. A summary of the initial version that uses [Apache Samza](http://samza.incubator.apache.org) for stream processing is [here](Results), while the code for that version is (available [as an archive](https://github.com/rhauch/debezium-proto/releases/tag/debezium-with-samza) or [navigable code](https://github.com/rhauch/debezium-proto/tree/7c3e7b2ee980a7226f75e350800487defcfd0394)).

The current state of the codebase is incomplete and replaces Samza (and the requirement for YARN) with the proposed and much lighter-weight [Kakfa Streams](https://cwiki.apache.org/confluence/display/KAFKA/KIP-28+-+Add+a+processor+client), which has not yet been released and which has changed since work on this prototype has stopped (see https://github.com/rhauch/debezium).

## Debezium

This prototype demonstrates an easy-to-use, fast, and durable distributed storage service for mobile application data. With Debezium, apps have to do less work coordinating changes to shared state, and that saves mobile app developers time and money and keeps mobile apps less complicated. Debezium uses an event streaming approach to record each incoming request in a durable, partitioned, and replicated log, and Debezium's share-nothing services asynchronously process these requests and produce output to other logs. This architecture ensures that no data is lost: if any of the services should fail, when they are restarted they will simply continue where the previous service left off.

### Technology

Debezium uses [Apache Kafka](http://kafka.apache.org) to read and write the durable, partitioned, and replicated logs. Kafka is very powerful and has features that are essential for Debezium:

* All messages are persisted on disk in a very fast and efficient way.
* Message topics are partitioned and replicated across multiple machines, with automatic failover. Kafka keeps track of how many replicas are in sync, and if that number falls below some threshold you can specify whether the broker becomes unavailable or to continue producing messages and risk losing data. The only thing shared amongst replicas is the network connection.
* Messages written with the same key will always be sent to the same partition, and all messages within a single partition have total ordering.
* Consumers read topic partitions and control where in the log they are. If needed, they can even re-read old messages.Consumers can use either [queuing or pub-sub](http://kafka.apache.org/documentation.html#intro_consumers).

With this architecture, Kafka can handle tremendous amounts of information and be configured to never lose any data. And because consumers are in control of where in the log they read, more options are available to Debezium than with traditional AMQP or JMS messaging systems.

## Future plans

No more work on this prototype will be done. Instead, it has been superseded by [another prototype](https://github.com/rhauch/debezium) that is focusing not on data storage for mobile apps but Change Data Capture. The architecture is quite similar, and the success of this prototype's use of Kafka shows the value of using Kafka as a replicated, partitioned, and append-only transaction log while using stream processing services to easily consume the totally ordered events within the logs.
