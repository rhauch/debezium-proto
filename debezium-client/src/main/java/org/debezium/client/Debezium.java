/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.DocumentReader;

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
     * @param document the initial JSON configuration; may not be null
     * @return the new builder; never null
     */
    public static Configure configure(Document document) {
        return new DbzConfigurator(() -> document);
    }

    /**
     * Obtain a fluent API to programmatically modify up a configuration based upon the supplied starting point.
     * 
     * @param stream the stream containing the initial JSON configuration; may not be null
     * @return the new builder; never null
     * @throws IOException if there is a problem reading the supplied stream
     */
    public static Configure configure(InputStream stream) throws IOException {
        return new DbzConfigurator(DocumentReader.defaultReader().read(stream));
    }

    /**
     * Obtain a fluent API to programmatically modify up a configuration based upon the supplied starting point.
     * 
     * @param url the URL to the initial JSON configuration; may not be null
     * @return the new builder; never null
     * @throws IOException if there is a problem reading the supplied stream
     */
    public static Configure configure(URL url) throws IOException {
        return new DbzConfigurator(DocumentReader.defaultReader().read(url));
    }

    /**
     * Obtain a fluent API to programmatically modify up a configuration based upon the supplied starting point.
     * 
     * @param file the file to the initial JSON configuration; may not be null
     * @return the new builder; never null
     * @throws IOException if there is a problem reading the supplied stream
     */
    public static Configure configure(File file) throws IOException {
        return new DbzConfigurator(DocumentReader.defaultReader().read(file));
    }

    /**
     * Connect to a Debezium cluster using the supplied configuration.
     * 
     * @param config the immutable configuration; may not be null
     * @return the client that is connected to the Debezium cluster; never null
     * @see #configure()
     */
    static Client start(Configuration config) {
        return new DbzClient(config).start();
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
         * @param lazy {@code true} if the initialization should be lazy, or false if the connections should be established upon
         *            startupp
         * @return this instance for chaining together methods; never null
         */
        Configure lazyInitialization(boolean lazy);

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
         * @return the database connection
         * @throws DebeziumConnectionException if there was an error connecting to the given database with the given username
         */
        Database connect(DatabaseId id, String username);

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
