/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.debezium.api.ConnectionFailedException;
import org.debezium.api.Database;
import org.debezium.api.Debezium;
import org.debezium.api.Debezium.Configuration;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.util.NamedThreadFactory;


/**
 * @author Randall Hauch
 *
 */
public final class DbzClient implements Debezium.Client {

    private final DbzConfiguration config;
    private final DbzNode node;
    private final DbzDatabases databases;
    private ExecutorService executor;
    
    public DbzClient( Configuration config ) {
        this.config = (DbzConfiguration)config;
        this.node = new DbzNode(this.config.kafkaProducerProperties(),this.config.kafkaConsumerProperties(),()->executor());
        this.databases = new DbzDatabases();
        this.node.add(this.databases);
    }
    
    private Executor executor() {
        return this.executor;
    }
    
    public DbzClient start() {
        boolean useDaemonThreads = false;    // they will keep the VM running if not shutdown properly
        ThreadFactory threadFactory = new NamedThreadFactory("debezium", "consumer",useDaemonThreads);
        executor = Executors.newCachedThreadPool(threadFactory);
        node.start();
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
        // Shutdown the cluster node, which shuts down all services and the service manager ...
        node.shutdown();
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
    }
}
