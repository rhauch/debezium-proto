/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.debezium.kafka.KafkaCluster;
import org.debezium.kafka.KafkaCluster.Usage;
import org.debezium.message.Topic;
import org.debezium.service.EntityBatchService;
import org.debezium.service.EntityStorageService;
import org.debezium.service.ResponseAccumulatorService;
import org.debezium.service.SchemaLearningService;
import org.debezium.service.SchemaService;
import org.debezium.service.ServiceRunner;
import org.debezium.util.NamedThreadFactory;

/**
 * A Debezium server that runs an embedded Zookeeper, Kafka, and all Debezium services. While this can be run as a single process
 * or even embedded into other applications, doing so will not be as scalable, resilient, or as fault tolerant as running a
 * distributed cluster of multiple Zookeeper processes, multiple Kafka brokers, and Debezium services in multiple processes.
 * <p>
 * By default the server will start Zookeeper on port {@value #DEFAULT_ZOOKEEPER_PORT}, a single Kafka broker on port
 * {@value #DEFAULT_KAFKA_STARTING_PORT}, and a single instance of each Debezium service. The ports can be explicitly set prior to
 * {@link #startup()} via the {@link #withPorts(int, int)} method.
 * 
 * @author Randall Hauch
 */
public class Server {

    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    public static final int DEFAULT_KAFKA_STARTING_PORT = 9092;
    public static final boolean DEFAULT_DELETE_DATA_UPON_SHUTDOWN = true;

    private final KafkaCluster kafkaCluster = new KafkaCluster().deleteDataUponShutdown(DEFAULT_DELETE_DATA_UPON_SHUTDOWN)
                                                                .withPorts(DEFAULT_ZOOKEEPER_PORT, DEFAULT_KAFKA_STARTING_PORT);
    private final ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("debezium-thread-"));
    private final List<ServiceRunner> services = new ArrayList<>();
    private final Set<String> topicNames = new HashSet<>();
    private final ConcurrentMap<String, Properties> configs = new ConcurrentHashMap<>();
    private volatile boolean running = false;

    public Server() {
        services.add(EntityBatchService.runner());
        services.add(EntityStorageService.runner());
        services.add(ResponseAccumulatorService.runner());
        services.add(SchemaLearningService.runner());
        services.add(SchemaService.runner());

        // TODO: Get the topic names from the service runners (via their topologies' source and sink topics) ...
        topicNames.add(Topic.ENTITY_BATCHES);
        topicNames.add(Topic.ENTITY_PATCHES);
        topicNames.add(Topic.ENTITY_UPDATES);
        topicNames.add(Topic.PARTIAL_RESPONSES);
        topicNames.add(Topic.COMPLETE_RESPONSES);
        topicNames.add(Topic.ENTITY_TYPE_UPDATES);
        topicNames.add(Topic.SCHEMA_UPDATES);
    }

    /**
     * Set the configuration properties for each of the brokers. This method does nothing if the supplied properties are null or
     * empty. Any properties that deal with Zookeeper, the host name, and the broker ID will be ignored, since they are
     * set via this embedded server.
     * 
     * @param properties the Kafka configuration properties
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if this server is running
     */
    public Server withKafkaConfiguration(Properties properties) {
        kafkaCluster.withKafkaConfiguration(properties);
        return this;
    }

    /**
     * Set the port numbers for Zookeeper and the Kafka brokers. By default the
     * 
     * @param zkPort the port number that Zookeeper should use; may be -1 if an available port should be discovered
     * @param firstKafkaPort the port number for the first Kafka broker (additional brokers will use subsequent port numbers);
     *            may be -1 if available ports should be discovered
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the cluster is running
     */
    public Server withPorts(int zkPort, int firstKafkaPort) {
        if (running) throw new IllegalStateException("Unable to add a broker when the cluster is already running");
        this.kafkaCluster.withPorts(zkPort, firstKafkaPort);
        return this;
    }

    /**
     * Specify whether the data is to be deleted upon {@link #shutdown(long, TimeUnit)}.
     * 
     * @param delete true if the data is to be deleted upon shutdown, or false otherwise
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if this server is running
     */
    public Server deleteDataUponShutdown(boolean delete) {
        this.kafkaCluster.deleteDataUponShutdown(delete);
        return this;
    }

    /**
     * Add a number of new Kafka broker to the cluster. The broker IDs will be generated.
     * 
     * @param count the number of new brokers to add
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if this server is running
     */
    public Server addBrokers(int count) {
        this.kafkaCluster.addBrokers(count);
        return this;
    }

    /**
     * Set the parent directory where all data will be stored. The Kafka brokers logs, Zookeeper server logs and snapshots, and
     * all Debezium data and logs will be stored here.
     * 
     * @param dataDir the parent directory for all persisted data; may be null if a temporary directory will be used
     * @return this instance to allow chaining methods; never null
     * @throws IllegalArgumentException if the supplied file is not a directory or not writable
     * @throws IllegalStateException if this server is running
     */
    public Server usingDirectory(File dataDir) {
        this.kafkaCluster.usingDirectory(dataDir);
        return this;
    }

    /**
     * Create a number of additional topics not automatically created by this server for its services.
     * 
     * @param topics the names of the additional topics to create
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if this server is running
     */
    public Server withTopics(String... topics) {
        if (topics != null) {
            for (String topic : topics) {
                if (topic != null && !topic.trim().isEmpty()) this.topicNames.add(topic);
            }
        }
        return this;
    }

    /**
     * Determine if the server is running.
     * 
     * @return true if this server is running, or false otherwise
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Asynchronously start the embedded Zookeeper server, Kafka brokers, and Debezium services.
     * This method does nothing if the cluster is already running.
     * 
     * @return this instance to allow chaining methods; never null
     * @throws IOException if there is an error during startup
     */
    public synchronized Server startup() throws IOException {
        if (!running) {
            this.kafkaCluster.startup();
            this.topicNames.forEach(topic -> this.kafkaCluster.createTopics(topic));
            this.services.forEach(service -> {
                Properties config = generateServiceConfiguration(service.getName());
                service.run(config, executor);
            });
            running = true;
        }
        return this;
    }

    private Properties generateServiceConfiguration(String serviceName) {
        assert kafkaCluster.isRunning();
        Properties consumer = useTo().getConsumerProperties(serviceName, serviceName, OffsetResetStrategy.LATEST);
        Properties producer = useTo().getProducerProperties(serviceName);
        Properties service = configs.get(serviceName);
        Properties config = new Properties();
        if (service != null) config.putAll(service);
        config.putAll(consumer);
        config.putAll(producer);
        return config;
    }

    /**
     * Obtain the interface for consuming messages from and producing messages to Kafka topics.
     * 
     * @return the usage interface; never null
     * @throws IllegalStateException if the cluster is not running
     */
    public Usage useTo() {
        return this.kafkaCluster.useTo();
    }

    /**
     * Shutdown the embedded Zookeeper server and the Kafka servers {@link #addBrokers(int) in the cluster}.
     * This method does nothing if the cluster is not running.
     * 
     * @param timeout the maximum amount of time to wait for all services to shutdown
     * @param unit the unit of time for {@code timeout}; may not be null
     * @return this instance to allow chaining methods; never null
     */
    public synchronized Server shutdown(long timeout, TimeUnit unit) {
        if (running) {
            try {
                // Shutdown the executor by interrupting threads, which will stop all services ...
                this.executor.shutdownNow();
                this.executor.awaitTermination(timeout, unit);
                // Stop kafka and zookeeper ...
                this.kafkaCluster.shutdown();
            } catch (InterruptedException e) {
                Thread.interrupted();
                // and continue ...
            } finally {
                this.running = false;
            }
        }
        return this;
    }
}
