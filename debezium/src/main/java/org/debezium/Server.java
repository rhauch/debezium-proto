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

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.examples.WallclockTimestampExtractor;
import org.debezium.kafka.KafkaCluster;
import org.debezium.kafka.KafkaCluster.Usage;
import org.debezium.message.DocumentSerdes;
import org.debezium.service.EntityBatchService;
import org.debezium.service.EntityStorageService;
import org.debezium.service.ResponseAccumulatorService;
import org.debezium.service.SchemaLearningService;
import org.debezium.service.SchemaService;
import org.debezium.service.ServiceRunner;

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
    public static final boolean DEFAULT_DELETE_DATA_UPON_SHUTDOWN = false;
    public static final boolean DEFAULT_DELETE_DATA_PRIOR_TO_STARTUP = false;

    private final KafkaCluster kafkaCluster = new KafkaCluster().deleteDataUponShutdown(DEFAULT_DELETE_DATA_UPON_SHUTDOWN)
                                                                .deleteDataPriorToStartup(DEFAULT_DELETE_DATA_PRIOR_TO_STARTUP)
                                                                .withPorts(DEFAULT_ZOOKEEPER_PORT, DEFAULT_KAFKA_STARTING_PORT);
    private final List<ServiceRunner> services = new ArrayList<>();
    private final Set<String> topicNames = new HashSet<>();
    private final ConcurrentMap<String, Properties> configs = new ConcurrentHashMap<>();
    private volatile boolean running = false;
    private volatile File dataDir = null;

    public Server() {
        services.add(EntityBatchService.runner().setVerbose(true));
        services.add(EntityStorageService.runner().setVerbose(true));
        services.add(ResponseAccumulatorService.runner().setVerbose(true));
        services.add(SchemaLearningService.runner().setVerbose(true));
        services.add(SchemaService.runner().setVerbose(true));

        // Get the names of *all* topics used by each service ...
        topicNames.addAll(EntityBatchService.topics());
        topicNames.addAll(EntityStorageService.topics());
        topicNames.addAll(ResponseAccumulatorService.topics());
        topicNames.addAll(SchemaLearningService.topics());
        topicNames.addAll(SchemaService.topics());
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
     * Specify whether the data is to be deleted upon {@link #shutdown()}.
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
     * Specify whether the data is to be deleted prior to {@link #startup()}.
     * 
     * @param delete true if the data is to be deleted upon shutdown, or false otherwise
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the cluster is running
     */
    public Server deleteDataPriorToStartup(boolean delete) {
        this.kafkaCluster.deleteDataPriorToStartup(delete);
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
        this.dataDir = dataDir;
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
     * Set the configuration for the {@link EntityBatchService entity batch service}.
     * 
     * @param props the service configuration properties
     * @return this instance to allow chaining methods; never null
     */
    public Server withEntityBatchConfiguration(Properties props) {
        return withServiceConfiguration(EntityBatchService.SERVICE_NAME, props);
    }

    /**
     * Set the configuration for the {@link EntityStorageService entity storage service}.
     * 
     * @param props the service configuration properties
     * @return this instance to allow chaining methods; never null
     */
    public Server withEntityStorageConfiguration(Properties props) {
        return withServiceConfiguration(EntityStorageService.SERVICE_NAME, props);
    }

    /**
     * Set the configuration for the {@link ResponseAccumulatorService response accumulator service}.
     * 
     * @param props the service configuration properties
     * @return this instance to allow chaining methods; never null
     */
    public Server withResponseAccumulatorConfiguration(Properties props) {
        return withServiceConfiguration(ResponseAccumulatorService.SERVICE_NAME, props);
    }

    /**
     * Set the configuration for the {@link SchemaLearningService schema learning service}.
     * 
     * @param props the service configuration properties
     * @return this instance to allow chaining methods; never null
     */
    public Server withSchemaLearningConfiguration(Properties props) {
        return withServiceConfiguration(SchemaLearningService.SERVICE_NAME, props);
    }

    /**
     * Set the configuration for the {@link SchemaService schema service}.
     * 
     * @param props the service configuration properties
     * @return this instance to allow chaining methods; never null
     */
    public Server withSchemaConfiguration(Properties props) {
        return withServiceConfiguration(SchemaService.SERVICE_NAME, props);
    }

    protected Server withServiceConfiguration(String serviceName, Properties props) {
        if (props != null) {
            Properties copy = new Properties();
            copy.putAll(props);
            this.configs.put(serviceName, copy);
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
            this.kafkaCluster.createTopics(1, 1, this.topicNames); // 1 partition for each
            this.services.forEach(service -> {
                Properties config = generateServiceConfiguration(service.getName());
                service.run(config);
            });
            running = true;
        }
        return this;
    }

    private Properties generateServiceConfiguration(String serviceName) {
        assert kafkaCluster.isRunning();
        Properties config = new Properties();
        config.put(StreamingConfig.CLIENT_ID_CONFIG, serviceName);
        config.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.brokerList());
        config.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, DocumentSerdes.class.getName());
        config.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DocumentSerdes.class.getName());
        config.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        if ( dataDir != null ) {
            File serviceDir = new File(dataDir,serviceName);
            serviceDir.mkdirs();
            config.put(StreamingConfig.STATE_DIR_CONFIG, serviceDir.getAbsolutePath());
        }
        Properties service = configs.get(serviceName);
        if (service != null) config.putAll(service);
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
     * Shutdown the embedded Zookeeper server, the Kafka brokers {@link #addBrokers(int) in the cluster}, and the services.
     * This method does nothing if the cluster is not running.
     * 
     * @return this instance to allow chaining methods; never null
     */
    public synchronized Server shutdown() {
        if (running) {
            try {
                // Stop each of the services ...
                this.services.forEach(ServiceRunner::shutdown);
                // Stop kafka and zookeeper ...
                this.kafkaCluster.shutdown();
            } finally {
                this.running = false;
            }
        }
        return this;
    }
}
