Copyright 2014 Red Hat
Licensed under the Eclipse Public License, Version 1.0.

# Debezium

# Build

Debezium is written in Java 8 and uses the Maven build system. Use Git to obtain a local clone of this repository, and make sure that you have Java 8 and Maven 3.1.x (or later) installed. Then, run the following from the top-level directory of your local repository:

    $ mvn clean install


# Run

The codebase contains a [sample application](blob/master/debezium-example-app/src/main/java/org/debezium/example/SampleApp.java) that uses the Debezium client library to provision and connect to a new database, read the database's schema, upload entities into the database, and to re-read the databases potentially updated schema.

The codebase also contains a script that will install and run Debezium on a local grid. This script:
* downloads, installs, and run Zookeeper, Kafka, and YARN;
* installs and starts each of the Debezium services as separate Samza jobs via YARN;
* optionally start log consumers to show the recorded operations; and
* stop all running components, including Zookeepr, Kafka, and YARN

To run the sample, first build the software locally. Then, open a shell in the top-level directory of your local Debezium git repository and run this command:

    $ bin/grid bootstrap

This downloads, installs, and runs Zookeeper, Kafka, and YARN, and then installs and starts (via YARN) each of the Debezium services as separate Samza jobs. Then go to (http://localhost:8088/cluster)[] in a browser to view the status of the YARN services; wait until you see all 4 services in the "`RUNNING`" state before proceeding.

You can then run the client application:

    $ mvn exec:java -Dexec.mainClass="org.debezium.example.SampleApp"

which will output information as it uses Debezium to provision (or connect to) a database, get schema information, and upload contacts. You can run the application more than once.

Run the following command to shut down all of the Debezium, Zookeeper, Kafka, and YARN processes:

    $ bin/grid stop all

All data is written to the `deploy` directory and to the `/tmp` directory. These can be removed to start from scrath.