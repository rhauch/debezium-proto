/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Editor;

/**
 * @author Randall Hauch
 *
 */
public interface Debezium {

    /**
     * Obtain a fluent API to programmatically build up the configuration for a driver.
     * 
     * @return the new builder; never null
     */
    public static Builder driver() {
        return new DbzDriverBuilder();
    }

    /**
     * A fluent API for building up a Debezium configuration.
     * 
     * @author Randall Hauch
     */
    public static interface Builder {

        /**
         * Load the configuration properties in the file at the supplied URL, overwriting any similarly-named properties
         * already in this configuration.
         * 
         * @param url the URL at which the configuration file can be found; may not be null
         * @return this builder instance for chaining together methods; never null
         * @throws IOException if there is an error connecting to the resource at the given URL
         */
        default public Builder load(URL url) throws IOException {
            try (InputStream stream = url.openStream()) {
                return load(stream);
            }
        }

        /**
         * Load the configuration properties in the file given by the supplied path, overwriting any similarly-named properties
         * already in this configuration.
         * 
         * @param path the {@link Path} at which the configuration file can be found; may not be null
         * @return this builder instance for chaining together methods; never null
         * @throws IOException if there is an error connecting to the resource at the given URL
         */
        default public Builder load(Path path) throws IOException {
            return load(path.toFile());
        }

        /**
         * Load the configuration properties in the given file, overwriting any similarly-named properties already in this
         * configuration.
         * 
         * @param file the configuration file to be read; may not be null
         * @return this builder instance for chaining together methods; never null
         * @throws IOException if there is an error connecting to the resource at the given URL
         */
        default public Builder load(File file) throws IOException {
            try (InputStream stream = new FileInputStream(file)) {
                return load(stream);
            }
        }

        /**
         * Load the configuration properties using the given reader, overwriting any similarly-named properties already in this
         * configuration.
         * 
         * @param reader the configuration properties reader; may not be null
         * @return this builder instance for chaining together methods; never null
         * @throws IOException if there is an error connecting to the resource at the given URL
         */
        default public Builder load(Reader reader) throws IOException {
            try {
                Properties properties = new Properties();
                properties.load(reader);
                return load(properties);
            } finally {
                reader.close();
            }
        }

        /**
         * Load the configuration properties from the given stream, overwriting any similarly-named properties already in this
         * configuration.
         * 
         * @param stream the stream containing the configuration properties; may not be null
         * @return this builder instance for chaining together methods; never null
         * @throws IOException if there is an error connecting to the resource at the given URL
         */
        default public Builder load(InputStream stream) throws IOException {
            try {
                Properties properties = new Properties();
                properties.load(stream);
                return load(properties);
            } finally {
                stream.close();
            }
        }

        /**
         * Load the configuration properties, overwriting any similarly-named properties already in this
         * configuration.
         * 
         * @param properties the configuration properties; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        public Builder load(Properties properties);

        /**
         * Set the Zookeeper connection string.
         * 
         * @param zookeeperConnectString the Zookeeper connection string, consisting of a comma separated host:port pairs,
         *            optionally followed by a chroot path
         * @return this builder instance for chaining together methods; never null
         */
        Builder withZookeeper(String zookeeperConnectString);

        /**
         * Add a broker with the given name, machine address, and port.
         * 
         * @param brokerName the name of the broker; may not be null
         * @param machine the address of the machine on which the broker is running; may be null for the local host
         * @param port the port used by the broker; must be positive
         * @return this builder instance for chaining together methods; never null
         */
        Builder withBroker(String brokerName, String machine, int port);

        /**
         * Add a broker with the given broker string in the form "{@code brokerName:machine:port}".
         * 
         * @param brokerString the broker string; may not be null
         * @return this builder instance for chaining together methods; never null
         * @throws IllegalArgumentException if the string is not in the proper form
         */
        default Builder withBroker(String brokerString) {
            String[] parts = brokerString.split(":");
            if (parts.length == 3) {
                try {
                    return withBroker(parts[0], parts[1], Integer.parseInt(parts[2]));
                } catch (NumberFormatException e) {
                    String msg = "The port number field '" + parts[2] + "' in the broker string '" + brokerString + "' is not an integer";
                    throw new IllegalArgumentException(msg);
                }
            }
            String msg = "The broker string '" + brokerString
                    + "' must be of the form 'brokerName:machine:port', where 'port' is an integer";
            throw new IllegalArgumentException(msg);
        }

        /**
         * Add a broker with the given name and port for the current machine.
         * 
         * @param brokerName the name of the broker; may not be null
         * @param port the port used by the broker; must be positive
         * @return this builder instance for chaining together methods; never null
         */
        default Builder withBroker(String brokerName, int port) {
            return withBroker(brokerName, null, port);
        }

        /**
         * Specify the kind of acknowledgement that's required for submitting requests within a Debezium cluster.
         * <p>
         * The default is {@link Acknowledgement#ALL}.
         * 
         * @param acknowledgement the desired acknowledgement level; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder acknowledgement(Acknowledgement acknowledgement);

        /**
         * Specify the amount of time that Debezium will wait for {@link #acknowledgement(Acknowledgement) acknowledgement} that
         * requests were submitted before reporting an error.
         * 
         * @param time the amount of time; must be positive
         * @param unit the time unit; must not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder requestTimeout(long time, TimeUnit unit);

        /**
         * Specify the maximum number of times that Debezium should retry a failed request.
         * Setting a non-zero value here can lead to duplicates in the case of network errors that cause a message to be
         * sent but the acknowledgement to be lost. Generally this Debezium is tolerant of at-least-once behaviors.
         * 
         * @param maximum the maximum number of times to retry a failed request; must be positive
         * @return this builder instance for chaining together methods; never null
         */
        Builder retryFailedRequests(int maximum);

        /**
         * Specify the amount of time to pause before retrying a request. The Debezium cluster will detect failures and
         * will elect new leaders for topic partitions. Since this may take some time, setting this value will control how
         * much time this client should wait before refreshing the cluster metadata.
         * 
         * @param time the amount of time; must be positive
         * @param unit the time unit; must not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder pauseBeforeRetries(long time, TimeUnit unit);

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
         * @return this builder instance for chaining together methods; never null
         */
        Builder refreshMetadataInterval(long time, TimeUnit unit);

        /**
         * Specify the compression that should be used to persist information in the transaction logs.
         * <p>
         * The default is {@link Compression#NONE}.
         * 
         * @param compression the compression codec; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder compression(Compression compression);

        /**
         * Specify a client identifier that is sent with all messages to help trace requests through the system.
         * This should logically represent the application. If not specified, a random identifier will be generated.
         * 
         * @param id the client identifier; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder clientId(String id);

        /**
         * Specify the size of the buffer used to write to a socket.
         * <p>
         * The default is 100kb.
         * 
         * @param size the number of bytes; must be positive
         * @return this builder instance for chaining together methods; never null
         */
        Builder socketBufferSize(int size);

        /**
         * Specify whether the producer connections should be initialized lazily. The default is 'false'.
         * 
         * @param immediately {@code true} if the initialization should be done immediately upon startup, or false if the
         *            connections should be done only when needed (which may make debugging connectivity problems more difficult)
         * @return this builder instance for chaining together methods; never null
         */
        Builder initializeProducerImmediately(boolean immediately);

        /**
         * Specify the number of response partitions.
         * 
         * @param count the number of separate partitions used to track the registered callbacks; must be positive
         * @return this builder instance for chaining together methods; never null
         */
        Builder responsePartitionCount(int count);

        /**
         * Specify the maximum backlog per partition for registered callbacks. If the number of incomplete requests
         * exceeds this number per callback partition, then any new requests will block until existing requests have
         * been completed.
         * 
         * @param count the maximum number of callbacks allowed per partition before new requests are blocked
         * @return this builder instance for chaining together methods; never null
         */
        Builder responseMaxBacklog(int count);

        /**
         * Specify the {@link SecurityProvider} implementation that should be used.
         * 
         * @param security the function that supplies a security provider implementation; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder usingSecurity(Supplier<SecurityProvider> security);

        /**
         * Specify the {@link MessageBus} implementation that should be used.
         * 
         * @param messageBus the function that supplies the message bus implementation; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder usingBus(Function<Supplier<Executor>, MessageBus> messageBus);

        /**
         * Specify the {@link ExecutorService} implementation that should be used.
         * 
         * @param executor the function that supplies the executor service implementation; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder usingExecutor(Supplier<ExecutorService> executor);

        /**
         * Specify the {@link ScheduledExecutorService} implementation that should be used.
         * 
         * @param executor the function that supplies the scheduled executor service implementation; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder usingScheduledExecutor(Supplier<ScheduledExecutorService> executor);

        /**
         * Start the driver.
         * 
         * @return the driver; never null
         */
        Debezium start();
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

        String literal() {
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

        String literal() {
            return literal;
        }

        @Override
        public String toString() {
            return literal();
        }
    }

    /**
     * Authenticate the named user for the given database.
     * 
     * @param username the username
     * @param device the device identifier or token
     * @param appVersion the version of the application
     * @param databaseIds the identifier of the database(s) for which the user is to be authenticated
     * @return the session token for the user; never null
     * @throws DebeziumAuthorizationException if the user could not be authenticated or authorized for the database(s)
     */
    public SessionToken connect(String username, String device, String appVersion, String... databaseIds);

    /**
     * Provision a new database with the given name, blocking at most the default timeout.
     * 
     * @param adminToken a valid session token for an administrative user; may not be null
     * @param databaseId the identifier for the new database; may not be null
     * @param timeout the maximum amount of time to wait for responses
     * @param unit the unit of time for the timeout
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     * @throws DebeziumProvisioningException if there was an error provisioning a database with the given username, for
     *             example if the database already exists
     */
    public void provision(SessionToken adminToken, String databaseId, long timeout, TimeUnit unit);

    /**
     * Read the current schema for the specified database.
     * 
     * @param token a valid session token for the user; may not be null
     * @param databaseId the name of the database; may not be null
     * @param timeout the maximum amount of time to wait for responses
     * @param unit the unit of time for the timeout
     * @return the schema; never null
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public Schema readSchema(SessionToken token, String databaseId, long timeout, TimeUnit unit);

    /**
     * Read one entity from the database and return its representation, including whether or not the entity
     * {@link Entity#exists() exists}.
     * 
     * @param token a valid session token for the user; may not be null
     * @param entityId the entity's unique identifier; may not be null
     * @param timeout the amount of time to wait for the response
     * @param unit the unit of time for the timeout
     * @return a representation of the entity; never null
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public Entity readEntity(SessionToken token, EntityId entityId, long timeout, TimeUnit unit);

    /**
     * Request to apply the given patch to an entity.
     * 
     * @param token a valid session token for the user; may not be null
     * @param patch the patch; may not be null
     * @param timeout the amount of time to wait for the response
     * @param unit the unit of time for the timeout
     * @return the result of the change request; never null
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public EntityChange changeEntity(SessionToken token, Patch<EntityId> patch, long timeout, TimeUnit unit);

    /**
     * Destroy one entity from the database.
     * 
     * @param token a valid session token for the user; may not be null
     * @param entityId the entity's unique identifier within this database; may not be null
     * @param timeout the amount of time to wait for the response
     * @param unit the unit of time for the timeout
     * @return {@code true} if the entity existed and was destroyed, or {@code false} if it did not exist
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public boolean destroyEntity(SessionToken token, EntityId entityId, long timeout, TimeUnit unit);

    /**
     * Begin a batch operation. Use the resulting {@link BatchBuilder} object to assemble the requests, and then
     * {@link BatchBuilder#submit(SessionToken, long, TimeUnit)} the batch.
     * 
     * @return the builder of the batch request; never null
     */
    public BatchBuilder batch();

    /**
     * Shutdown this client and release all resources.
     * 
     * @param timeout the maximum time that this method should block before returning; must be positive
     * @param unit the time unit for {@code timeout}; may not be null
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public void shutdown(long timeout, TimeUnit unit);

    /**
     * A builder of a batch request, used to record the operations for a single database that are to be
     * {@link #submit(SessionToken, long, TimeUnit) submitted} to the server using a single network request.
     * 
     * @author Randall Hauch
     */
    public static interface BatchBuilder {
        /**
         * Add a request to this batch to read one entity from the database and return its representation, including whether or
         * not the entity {@link Entity#exists() exists}.
         * 
         * @param entityId the entity's unique identifier within this database; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        public BatchBuilder readEntity(EntityId entityId);

        /**
         * Add a request to this batch to apply the given patch to an entity.
         * 
         * @param patch the patch; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        public BatchBuilder changeEntity(Patch<EntityId> patch);

        /**
         * Add a request to this batch to destroy one entity from the database.
         * 
         * @param entityId the entity's unique identifier within this database; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        public BatchBuilder destroyEntity(EntityId entityId);

        /**
         * Add a request that changes the entity with the given ID, returning a {@link Editor patch editor} that can be used
         * to record the operations. Call {@link Editor#end()} on the patch editor to obtain this batch builder.
         * 
         * @param entityId the entity's unique identifier within this database; may not be null
         * @return the patch editor, which when {@link Editor#end()} is called returns this builder
         */
        public Patch.Editor<BatchBuilder> changeEntity(EntityId entityId);

        /**
         * Add a request that creates a new entity of the given type and with a generated ID, returning a {@link Editor patch
         * editor} that can be used to record the operations. Call {@link Editor#end()} on the patch editor to obtain this
         * batch builder.
         * 
         * @param entityType the type of entity to be created; may not be null
         * @return the patch editor, which when {@link Editor#end()} is called returns this builder
         */
        public Patch.Editor<BatchBuilder> createEntity(EntityType entityType);

        /**
         * Submit the recorded operations to the server as a single batched request, wait for the response, and return the
         * results.
         * 
         * @param token a valid session token for the user; may not be null
         * @param timeout the amount of time to wait for the response
         * @param unit the unit of time for the timeout
         * @return the results of the batched operations; never null
         * @throws DebeziumTimeoutException if the operation timed out
         */
        public BatchResult submit(SessionToken token, long timeout, TimeUnit unit);
    }

}
