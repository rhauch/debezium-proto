Copyright 2014 Red Hat
Licensed under the Eclipse Public License, Version 1.0.

This is a work in progress. Some of the functionality is implemented, but there is still much to do. The results of this prototype have been summarized [here](Results).

## Debezium

Debezium is a prototype for an easy-to-use, fast, and durable distributed storage service for mobile application data. With Debezium, apps have to do less work coordinating changes to shared state, and that saves mobile app developers time and money and keeps mobile apps less complicated. Debezium uses an event streaming approach to record each incoming request in a durable, partitioned, and replicated log, and Debezium's share-nothing services asynchronously process these requests and produce output to other logs. This architecture ensures that no data is lost: if any of the services should fail, when they are restarted they will simply continue where the previous service left off.

### Technology

Debezium uses [Apache Kafka](http://kafka.apache.org) to read and write the durable, partitioned, and replicated logs. Kafka is very powerful and has features that are essential for Debezium:

* All messages are persisted on disk in a very fast and efficient way.
* Message topics are partitioned and replicated across multiple machines, with automatic failover. Kafka keeps track of how many replicas are in sync, and if that number falls below some threshold you can specify whether the broker becomes unavailable or to continue producing messages and risk losing data. The only thing shared amongst replicas is the network connection.
* Messages written with the same key will always be sent to the same partition, and all messages within a single partition have total ordering.
* Consumers read topic partitions and control where in the log they are. If needed, they can even re-read old messages.Consumers can use either [queuing or pub-sub](http://kafka.apache.org/documentation.html#intro_consumers).

With this architecture, Kafka can handle tremendous amounts of information and be configured to never lose any data. And because consumers are in control of where in the log they read, more options are available to Debezium than with traditional AMQP or JMS messaging systems.

Each of Debezium's services use [Apache Samza](http://samza.incubator.apache.org), a distributed stream processing library that makes it easy to write services that work with Kafka streams.

Debezium is deployed using [Docker](https://www.docker.com) containers, and will soon support [Kubernetes](http://kubernetes.io) to for easy deployment on [OpenShift](https://www.openshift.com). 

## Releases

We've not yet released Debezium, although we're working towards the first 0.1 release as fast as we can. Until then, the only way to run Debezium is to get the code and build it locally. This will generate the Docker images and put them into your local Docker registry, where they're ready to use. 

## Building the code

Debezium is written in Java 8, uses Maven 3.1.x for building, uses Git for version control, and packages all services using [Docker](https://www.docker.com) - all of these must be installed before you can get the code and build Debezium. Use the following commands to obtain the code:

    $ git clone https://github.com/rhauch/debezium.git
	$ cd debezium

Then make sure Docker daemon is running (or `boot2docker` is running on OS X and Windows), and run the following command to build everything:

	$ mvn clean install docker:build

This command builds JARs for all of the modules, runs all of the tests, builds Docker images for each of the services, and puts these images into your local Docker registry so they are ready to use. Once again, official Debezium releases will include pushing the official Debezium Docker images into DockerHub.

## Running Debezium

Debezium is designed with a microservices architecture to be durable, scalable, distributed, and fast, so Debezium runs best on a cluster of machines.

However, you probably want to first try it locally, so we made it easy to run a complete Debezium cluster on a single machine using [Docker Compose](https://docs.docker.com/compose/). Once you've installed Docker Compose, then starting Debezium is as easy as going to the top-level directory (same place you ran the `mvn ...` command above) and running:

    $ docker-compose up

This will start up a single instance of each Debezium service, each running in a separate Docker container. Use `Ctrl-C` to gracefully shut down all of the services.

You can either use use Debezium's Driver in one of your server applications (creating your own API service) or use the [sample application](blob/master/debezium-example-app/src/main/java/org/debezium/example/SampleApp.java) to provision and connect to a new database, read the database's schema, upload entities into the database, and to re-read the databases potentially updated schema. Debezium also provides a [load application](blob/master/debezium-example-load-app/src/main/java/org/debezium/example/LoadApp.java) that will continuously add content into Debezium.

For example, use the following commands (from within the `debezium` directory) to run the sample application against Debezium running locally:

    $ cd debeziume-example-app
    $ mvn exec:java -Dexec.mainClass="org.debezium.example.SampleApp"

Or use the following commands (from within the `debezium` directory) to run the load application against Debezium running locally:

    $ cd debeziume-example-app
    $ mvn exec:java -Dexec.mainClass="org.debezium.example.LoadApp" -Dexec.args="--verbose --threads 2 --requests-per-thread 1000000"

To shut down the local Debezium cluster:

    $ docker-compose stop

## Future plans

Debezium is still a prototype that is missing some important features, but we've been able to demonstrate the power of using event streaming to store application state in a distributed, durable, and scalable system. We're currently working on making it easy to deploy Debezium to a public or private cloud, and to make it easier for directly using Debezium from within mobile apps.
