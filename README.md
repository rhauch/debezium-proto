Copyright 2014 Red Hat
Licensed under the Eclipse Public License, Version 1.0.

This is a work in progress. Some of the functionality is implemented, but there is still much to do. 

## Debezium

Debezium is a prototype for a distributed storage service for mobile application data. It is designed to be resilient and to never lose data. The Debezium architecture is based upon event streaming: all requests are funneled through durable, partitioned, and replicated logs and processed asynchronously by multiple share-nothing services. If any of the services should fail, when they are restarted they will simply continue where the previous service left off, ensuring that once requests enter the first log no data or requests are ever lost or missed. 

## Technology

Debezium uses [Apache Kafka](http://kafka.apache.org) to read and write the durable, partitioned, and replicated logs. Kafka is very powerful, and has some features that are essential for Debezium:

* All messages are persisted on disk in a very fast and efficient way.
* Topics are replicated across multiple machines, with automatic failover. Kafka keeps track of how many replicas are in sync, and if that number falls below some threshold you can specify whether the broker becomes unavailable or to continue producing messages and risk losing data. The only thing shared amongst replicas is the network connection.
* Messages written to topics are partitioned; messages with the same key will always go to the same partition. Messages within a partition will have total ordering.
* Consumers read topic partitions, and they control where in the log they are. If needed, they can even re-read old messages. Consumers can use either [queuing or pub-sub](http://kafka.apache.org/documentation.html#intro_consumers).

With this architecture, Kafka can handle tremendous amounts of information and be configured to never lose any data. And because consumers are in control of where in the log they read, more options are available to Debezium than with traditional AMPQ or JMS messaging systems.

One goal of Debezium will be easy deployment on [OpenShift](https://www.openshift.com) and in the cloud, and therefore support for [Docker](https://www.docker.com) and [Kubernetes](http://kubernetes.io) will be very important. More important in the short term, however, is verifying the concepts and event-processing architecture can be used for data storage. This prototype uses several other (potentially competing) technologies in the interest of getting something working fast.

The prototype's services are built on top of [Apache Samza](http://samza.incubator.apache.org), a distributed stream processing library that makes it easy to write services that work with Kafka streams. Samza offers a few benefits that make prototyping easier and faster, but it also brings with it a dependency upon [Apache Hadoop's YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html). At this point, it is not clear whether Samza can be extended to instead use Kubernetes instead of YARN; if not, then Debezium will likely replace Samza and the limited capabilities it brings.

## Build

Debezium is written in Java 8 and uses the Maven build system. Use Git to obtain a local clone of this repository, and make sure that you have Java 8 and Maven 3.1.x (or later) installed. Then, run the following from the top-level directory of your local repository:

    $ mvn clean install


## Run

The codebase contains a [sample application](blob/master/debezium-example-app/src/main/java/org/debezium/example/SampleApp.java) that uses the Debezium client library to provision and connect to a new database, read the database's schema, upload entities into the database, and to re-read the databases potentially updated schema.

The codebase also contains a script that will install and run Debezium on a local grid. This script:
* downloads, installs, and run Zookeeper, Kafka, and YARN;
* installs and starts each of the Debezium services as separate Samza jobs via YARN;
* optionally start log consumers to show the recorded operations; and
* stop all running components, including Zookeepr, Kafka, and YARN

### Starting up the grid

To run the sample, first build the software locally. Then, open a shell in the top-level directory of your local Debezium git repository and run this command:

    $ bin/grid bootstrap

This downloads, installs, and runs Zookeeper, Kafka, and YARN, and then installs and starts (via YARN) each of the Debezium services as separate Samza jobs. Then go to [http://localhost:8088/cluster]() in a browser to view the status of the YARN services; wait until you see all 4 services in the "`RUNNING`" state before proceeding.

### Watching the topics (optional)

If you want to monitor the Kafka topics that Debezium uses internally, you can use the same `grid` comamnd list or to watch one or more of Debezium's Kafka topics. To see a list of the available topics, use:

    $ bin/grid watch

To watch a specific topic, use this command in a separate shell:

    $ bin/grid watch <topicName>

where `<topicName` is one of the following:

* `entities-batches` - The topic that records all batch requests to read and/or change multiple entities. The Debezium client submits each batch request to this topic. One of the Debezium services consumes this topic, splits out the patches for each entity, and submits each patch request to `entities-patches`.
* `entities-patches` - The topic that records all patch requests to read or change entities. One of the Debezium services consumes this topic and, for each patch, attempts to apply the patches and submits the results to `partial-responses`. Any changes to the entities are also written to `entity-updates`.
* `entity-updates` - The topic that records all changes to all entities.
* `partial-responses` - The topic that records the results of all read and change requests for entities, schemas, etc. Each Debezium client independently consumes this topic and forwards responses to response handlers the the application provided when calling client methods.
* `schema-patches` - The topic that records all requests to read or change schemas. One of the Debezium services consumes this topic and for each patch attempts to apply the patches and submit the results to `partial-responses`. Any changes to the schemas are also written to `schema-updates'.
* `schema-updates` - The topic that records all changes to schemas.
* `schema-learning` - The topic onto which all changes to entities and changes to schemas are aggregated. This topic is consumed by Debezium's schema learning service.

### Run the example application

Once all Debezium services are running, you can then run the client application:

    $ mvn exec:java -Dexec.mainClass="org.debezium.example.SampleApp"

This application will output information as it uses Debezium to provision (or connect to) a database, get schema information, and upload contacts. You can run the application more than once.

### Shut down the grid

Finally, run the following command to shut down all of the Debezium, Zookeeper, Kafka, and YARN processes:

    $ bin/grid stop all

All data is written to the `deploy` directory and to the `/tmp` directory. Remove these to start before running the demo again.