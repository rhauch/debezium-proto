/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.debezium.api.ConnectionFailedException;
import org.debezium.api.Database;
import org.debezium.api.DatabaseId;
import org.debezium.api.Debezium;
import org.debezium.api.Debezium.Configuration;
import org.debezium.core.util.NamedThreadFactory;


/**
 * @author Randall Hauch
 *
 */
public final class DbzClient implements Debezium.Client {

    private final DbzConfiguration config;
    private final ProducerConfig producerConfig;
    private final DbzDatabases databases = new DbzDatabases();
    private final String uniqueClientId = UUID.randomUUID().toString();
    private Producer<String, byte[]> producer;
    private DbzConsumers consumers;
    private ExecutorService executor;
    
    public DbzClient( Configuration config ) {
        this.config = (DbzConfiguration)config;
        this.producerConfig = new ProducerConfig(this.config.kafkaProducerProperties());
    }
    
    public DbzClient start() {
        producer = new Producer<String,byte[]>(producerConfig);
        boolean useDaemonThreads = false;    // they will keep the VM running if not shutdown properly
        ThreadFactory threadFactory = new NamedThreadFactory("debezium", "consumer",useDaemonThreads);
        executor = Executors.newCachedThreadPool(threadFactory);
        consumers = new DbzConsumers(config,executor);
        databases.initialize(uniqueClientId,producer,consumers);
        return this;
    }
    
    @Override
    public Database connect(DatabaseId id, String username) {
        if ( databases.isActive(id)) {
            return new DatabaseConnection(databases, new ExecutionContext(id,username));
        }
        throw new ConnectionFailedException(id);
    }
    
    @Override
    public void shutdown( long timeout, TimeUnit unit ) {
        if ( producer != null ) {
            try {
                producer.close();
            } finally {
                producer = null;
            }
        }
        // Shutdown the consumers ...
        if ( consumers != null ) {
            consumers.shutdown();
        }
        if ( executor != null ) {
            try {
                executor.shutdown();
            } finally {
                try {
                    executor.awaitTermination(timeout, unit);
                } catch ( InterruptedException e ) {
                    // We were interrupted while blocking, so clear the status ...
                    Thread.interrupted();
                }
            }
        }
        if ( databases != null ) {
            databases.shutdown();
        }
    }
    
    protected ConsumerConfig createConsumerConfig( String groupId ) {
        return new ConsumerConfig(config.kafkaConsumerProperties(groupId));
    }
}
