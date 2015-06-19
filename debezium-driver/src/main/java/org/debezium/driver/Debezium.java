/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.debezium.core.component.DatabaseId;

/**
 * @author Randall Hauch
 *
 */
public interface Debezium {

    /**
     * Obtain a fluent API to programmatically build up a configuration.
     * 
     * @return the new builder; never null
     */
    public static Configure configure() {
        return new DbzConfigurator();
    }

    /**
     * Obtain a fluent API to programmatically modify up a configuration based upon the supplied starting point.
     * 
     * @param stream the stream containing the initial JSON configuration; may not be null
     * @return the new builder; never null
     * @throws IOException if there is a problem reading the supplied stream
     */
    public static Configure configure(InputStream stream) throws IOException {
        return new DbzConfigurator(Configuration.load(stream));
    }

    /**
     * Obtain a fluent API to programmatically modify up a configuration based upon the supplied starting point.
     * 
     * @param url the URL to the initial JSON configuration; may not be null
     * @return the new builder; never null
     * @throws IOException if there is a problem reading the supplied stream
     */
    public static Configure configure(URL url) throws IOException {
        return new DbzConfigurator(Configuration.load(url));
    }

    /**
     * Obtain a fluent API to programmatically modify up a configuration based upon the supplied starting point.
     * 
     * @param file the file to the initial JSON configuration; may not be null
     * @return the new builder; never null
     * @throws IOException if there is a problem reading the supplied stream
     */
    public static Configure configure(File file) throws IOException {
        return new DbzConfigurator(Configuration.load(file));
    }

    /**
     * Connect to a Debezium cluster using the supplied configuration.
     * 
     * @param config the immutable configuration; may not be null
     * @return the client that is connected to the Debezium cluster; never null
     * @see #configure()
     */
    public static Client start(Configuration config) {
        return new DbzClient(config, Environment.create(config)).start();
    }

    /**
     * Connect to a Debezium cluster using the supplied configuration.
     * 
     * @param config the immutable configuration; may not be null
     * @param foundationFactory the factory for the foundation; may not be null
     * @return the client that is connected to the Debezium cluster; never null
     * @see #configure()
     */
    static Client start(Configuration config, Function<Supplier<Executor>, Foundation> foundationFactory) {
        return new DbzClient(config, Environment.create(foundationFactory)).start();
    }

    /**
     * Connect to a Debezium cluster using the supplied configuration.
     * 
     * @param config the immutable configuration; may not be null
     * @param env the environment to use for the client; may not be null
     * @return the client that is connected to the Debezium cluster; never null
     * @see #configure()
     */
    static Client start(Configuration config, Environment env) {
        return new DbzClient(config, env).start();
    }

    /**
     * A fluent API for building up a Debezium configuration.
     * 
     * @see Debezium#configure()
     */
    static interface Configure {

        /**
         * Set the Zookeeper connection string.
         * 
         * @param zookeeperConnectString the Zookeeper connection string, consisting of a comma separated host:port pairs,
         *            optionally followed by a chroot path
         * @return this builder instance for chaining together methods; never null
         */
        Configure withZookeeper(String zookeeperConnectString);

        /**
         * Add a broker with the given name, machine address, and port.
         * 
         * @param brokerName the name of the broker; may not be null
         * @param machine the address of the machine on which the broker is running; may be null for the local host
         * @param port the port used by the broker; must be positive
         * @return this builder instance for chaining together methods; never null
         */
        Configure withBroker(String brokerName, String machine, int port);

        /**
         * Add a broker with the given broker string in the form "{@code brokerName:machine:port}".
         * 
         * @param brokerString the broker string; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Configure withBroker(String brokerString);

        /**
         * Add a broker with the given name and port for the current machine.
         * 
         * @param brokerName the name of the broker; may not be null
         * @param port the port used by the broker; must be positive
         * @return this builder instance for chaining together methods; never null
         */
        Configure withBroker(String brokerName, int port);

        /**
         * Specify the kind of acknowledgement that's required for submitting requests within a Debezium cluster.
         * 
         * @param acknowledgement the desired acknowledgement level; may be null if the default acknowledgement is to be used
         * @return this builder instance for chaining together methods; never null
         */
        Configure acknowledgement(Acknowledgement acknowledgement);

        /**
         * Specify the amount of time that Debezium will wait for {@link #acknowledgement(Acknowledgement) acknowledgement} of
         * requests before reporting an error.
         * 
         * @param time the amount of time; must be positive
         * @param unit the time unit; must not be null
         * @return this builder instance for chaining together methods; never null
         */
        Configure requestTimeout(long time, TimeUnit unit);

        /**
         * Specify the maximum number of times that Debezium should retry a failed request.
         * Setting a non-zero value here can lead to duplicates in the case of network errors that cause a message to be
         * sent but the acknowledgement to be lost. Generally this Debezium is tolerant of at-least-once behaviors.
         * 
         * @param maximum the maximum number of times to retry a failed request; must be positive
         * @return this instance for chaining together methods; never null
         */
        Configure retryFailedRequests(int maximum);

        /**
         * Specify the amount of time to pause before retrying a request. The Debezium cluster will detect failures and
         * will elect new leaders for topic partitions. Since this may take some time, setting this value will control how
         * much time this client should wait before refreshing the cluster metadata.
         * 
         * @param time the amount of time; must be positive
         * @param unit the time unit; must not be null
         * @return this instance for chaining together methods; never null
         */
        Configure pauseBeforeRetries(long time, TimeUnit unit);

        /**
         * Specify the amount of time in between metadata refreshes. Metadata is automatically refreshed when a failure
         * is detected, but this setting can be used to periodically refresh the metadata.
         * <p>
         * The default is 10 minutes.
         * 
         * @param time the amount of time; if negative, the metadata will be refreshed only on failure; if 0, the metadata
         *            will be refreshed after every request (not recommended); if positive, the metadata will be refreshed
         *            periodically
         * @param unit the time unit; must not be null
         * @return this instance for chaining together methods; never null
         */
        Configure refreshMetadataInterval(long time, TimeUnit unit);

        /**
         * Specify the compression that should be used to persist information in the transaction logs.
         * 
         * @param compression the compression codec; may be null if {@link Compression#NONE no compression} is to be used
         * @return this builder instance for chaining together methods; never null
         */
        Configure compression(Compression compression);

        /**
         * Specify a client identifier that is sent with all messages to help trace requests through the system.
         * This should logically represent the application.
         * 
         * @param id the client identifier; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Configure clientId(String id);

        /**
         * Specify the size of the buffer used to write to a socket.
         * <p>
         * The default is 100kb.
         * 
         * @param size the number of bytes; must be positive
         * @return this instance for chaining together methods; never null
         */
        Configure socketBufferSize(int size);

        /**
         * Specify whether the producer connections should be initialized lazily. The default is 'false'.
         * 
         * @param immediately {@code true} if the initialization should be done immediately upon startup, or false if the
         * connections should be done only when needed
         * @return this instance for chaining together methods; never null
         */
        Configure initializeProducerImmediately(boolean immediately);

        /**
         * Specify how frequently (in seconds) the callback cleaner should run.
         * 
         * @param period the number of seconds between runs
         * @return this instance for chaining together methods; never null
         */
        Configure cleanerPeriodInSeconds(int period);

        /**
         * Specify the initial delay (in seconds) for the the callback cleaner.
         * 
         * @param delay the number of seconds after startup after which the cleaner should be run the first time
         * @return this instance for chaining together methods; never null
         */
        Configure cleanerDelayInSeconds(int delay);

        /**
         * Specify the number of response partitions.
         * 
         * @param count the number of separate partitions used to track the registered callbacks; must be positive
         * @return this instance for chaining together methods; never null
         */
        Configure responsePartitionCount(int count);

        /**
         * Specify the maximum backlog per partition for registered callbacks. If the number of incomplete requests
         * exceeds this number per callback partition, then any new requests will block until existing requests have
         * been completed.
         * 
         * @param count the maximum number of callbacks allowed per partition before new requests are blocked
         * @return this instance for chaining together methods; never null
         */
        Configure responseMaxBacklog(int count);

        /**
         * Create a new immutable representation of the current state of this configurator.
         * The resulting Configuration can be passed to the {@link Debezium#start(Configuration)} method to create a
         * {@link Debezium.Client Debezium Client}.
         * 
         * @return the immutable configuration.
         */
        Configuration build();
    }

    /**
     * A client connected to a Debezium cluster. Applications and services can use a client to interact with
     * Debezium.
     */
    static interface Client {

        /**
         * Connect to the specified database using the given username.
         * 
         * @param id the database identifier
         * @param username the username
         * @param device the device identifier or token
         * @param appVersion the version of the application
         * @return the database connection
         * @throws DebeziumConnectionException if there was an error connecting to the given database with the given username
         */
        Database connect(DatabaseId id, String username, String device, String appVersion);

        /**
         * Connect to the specified database using the given username.
         * 
         * @param id the database identifier
         * @param username the username
         * @param device the device identifier or token
         * @param appVersion the version of the application
         * @param timeout the maximum amount of time to wait for responses
         * @param unit the unit of time for the timeout
         * @return the database connection
         * @throws DebeziumConnectionException if there was an error connecting to the given database with the given username
         */
        Database connect(DatabaseId id, String username, String device, String appVersion, long timeout, TimeUnit unit);

        /**
         * Provision a new database with the given name.
         * 
         * @param id the database identifier
         * @param username the username
         * @param device the device identifier or token
         * @param appVersion the version of the application
         * @return the database connection
         * @throws DebeziumConnectionException if there was an error connecting to the given database with the given username
         * @throws DebeziumProvisioningException if there was an error provisioning a database with the given username
         */
        Database provision(DatabaseId id, String username, String device, String appVersion);

        /**
         * Provision a new database with the given name.
         * 
         * @param id the database identifier
         * @param username the username
         * @param device the device identifier or token
         * @param appVersion the version of the application
         * @param timeout the maximum amount of time to wait for responses
         * @param unit the unit of time for the timeout
         * @return the database connection
         * @throws DebeziumConnectionException if there was an error connecting to the given database with the given username
         * @throws DebeziumProvisioningException if there was an error provisioning a database with the given username
         */
        Database provision(DatabaseId id, String username, String device, String appVersion, long timeout, TimeUnit unit);

        /**
         * Shutdown this client and release all resources.
         * 
         * @param timeout the maximum time that this method should block before returning; must be positive
         * @param unit the time unit for {@code timeout}; may not be null
         */
        void shutdown(long timeout, TimeUnit unit);
    }

    /**
     * The level of acknowledgement that is required by Debezium before returning from an update request.
     */
    public static enum Acknowledgement {
        /**
         * Require an acknowledgement from all replicas before Debezium returns from an update request.
         * This option provides the best durability, we guarantee that no messages will be lost as long as at
         * least one in sync replica remains.
         */
        ALL("-1"),
        /**
         * Require an acknowledgement from the leader replica before Debezium returns from an update request.
         * This option provides better durability as the client waits until the server acknowledges the request
         * as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
         */
        ONE("1"),
        /**
         * Require no acknowledgement from an replicas before Debezium returns from an update request.
         * This option provides the lowest latency but the weakest durability guarantees (some data will be lost
         * when a server fails).
         */
        NONE("0");
        private final String literal;

        private Acknowledgement(String literal) {
            this.literal = literal;
        }

        public String literal() {
            return literal;
        }

        @Override
        public String toString() {
            return literal();
        }
    }

    /**
     * The codec used during compression of persisted information.
     */
    public static enum Compression {
        /**
         * Use GZIP compression.
         */
        GZIP("gzip"),
        /**
         * Use Snappy compression.
         */
        SNAPPY("snappy"),
        /**
         * Use no compression.
         */
        NONE("none");
        private final String literal;

        private Compression(String literal) {
            this.literal = literal;
        }

        public String literal() {
            return literal;
        }

        @Override
        public String toString() {
            return literal();
        }
    }

}
