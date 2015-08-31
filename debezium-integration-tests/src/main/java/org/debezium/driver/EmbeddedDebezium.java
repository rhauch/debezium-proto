/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Editor;
import org.debezium.core.util.Stopwatch;
import org.debezium.core.util.Stopwatch.StopwatchSet;

/**
 * In-memory representations of a Debezium.Client and all services. Streams are implemented with in-memory queues, and all
 * stream consumers are run in separate threads. Thus, each service is run in its own thread, and all client behavior matches
 * that when the client runs against a normal Debezium installation that uses Kafka for streams.
 * <p>
 * The purpose of this class is to make it easy to test the behavior and interaction of the client and all services, using a
 * single lightweight class that tests can instantiate as needed. Since Kafka and Samza are not used, this will not test any
 * behavior or interaction with Kafka brokers. And, since this system uses a single partition for each stream, it does not
 * replicate the multi-partition behavior of a normal Debezium installation. All information is kept in memory (including the
 * key-value stores used in the services), so the information is not durable (making cleanup easy) and is limited to the available
 * memory.
 * <p>
 * Be sure to call {@link #shutdown(long, TimeUnit)} when finished with an {@link EmbeddedDebezium} instance. Doing so will
 * properly clean up all threads and resources.
 */
public class EmbeddedDebezium implements Debezium {


    private final Debezium client;
    private final EmbeddedDebeziumServices services;
    
    private final StopwatchSet connectTimer = Stopwatch.multiple();
    private final StopwatchSet provisionTimer = Stopwatch.multiple();
    private final StopwatchSet readSchemaTimer = Stopwatch.multiple();
    private final StopwatchSet readEntityTimer = Stopwatch.multiple();
    private final StopwatchSet changeEntityTimer = Stopwatch.multiple();
    private final StopwatchSet removeEntityTimer = Stopwatch.multiple();
    private final StopwatchSet batchTimer = Stopwatch.multiple();
    private final StopwatchSet shutdownTimer = Stopwatch.multiple();

    public EmbeddedDebezium() {
        this(new EmbeddedDebeziumServices());
    }
    
    public EmbeddedDebezium( EmbeddedDebeziumServices services ) {
        this.services = services;
        
        // Create the client ...
        client = Debezium.driver()
                         .clientId("client")
                         .initializeProducerImmediately(true)
                         .usingExecutor(services.supplyExecutorService())
                         .usingBus(services.supplyMessageBus())
                         .usingSecurity(PassthroughSecurityProvider::new)
                         .start();
    }

    /**
     * Signal that the {@link WindowableTask#window(MessageCollector, TaskCoordinator)} method be called (asynchronously) on all
     * {@link WindowableTask windowable service}.
     */
    public void fireWindow() {
        services.fireWindow();
    }

    @Override
    public BatchBuilder batch() {
        BatchBuilder delegate = client.batch();
        return new BatchBuilder() {
            @Override
            public BatchBuilder readEntity(EntityId entityId) {
                return delegate.readEntity(entityId);
            }
            @Override
            public BatchBuilder changeEntity(Patch<EntityId> patch) {
                return delegate.changeEntity(patch);
            }
            @Override
            public Editor<BatchBuilder> changeEntity(EntityId entityId) {
                return delegate.changeEntity(entityId);
            }
            @Override
            public Editor<BatchBuilder> createEntity(EntityType entityType) {
                return delegate.createEntity(entityType);
            }
            @Override
            public BatchBuilder destroyEntity(EntityId entityId) {
                return delegate.destroyEntity(entityId);
            }
            @Override
            public BatchResult submit(SessionToken token, long timeout, TimeUnit unit) {
                return batchTimer.time(()->delegate.submit(token, timeout, unit));
            }
        };
    }
    
    @Override
    public Configuration getConfiguration() {
        return client.getConfiguration();
    }

    @Override
    public SessionToken connect(String username, String device, String appVersion, String... databaseIds) {
        return connectTimer.time(()-> client.connect(username, device, appVersion, databaseIds));
    }

    @Override
    public void provision(SessionToken adminToken, String databaseId, long timeout, TimeUnit unit) {
        provisionTimer.time(()-> client.provision(adminToken, databaseId, timeout, unit));
    }

    @Override
    public Schema readSchema(SessionToken token, String databaseId, long timeout, TimeUnit unit) {
        return readSchemaTimer.time(()->client.readSchema(token, databaseId, timeout, unit));
    }

    @Override
    public Entity readEntity(SessionToken token, EntityId entityId, long timeout, TimeUnit unit) {
        return readEntityTimer.time(()->client.readEntity(token, entityId, timeout, unit));
    }

    @Override
    public EntityChange changeEntity(SessionToken token, Patch<EntityId> patch, long timeout, TimeUnit unit) {
        return changeEntityTimer.time(()->client.changeEntity(token, patch, timeout, unit));
    }

    @Override
    public boolean destroyEntity(SessionToken token, EntityId entityId, long timeout, TimeUnit unit) {
        return removeEntityTimer.time(()->client.destroyEntity(token, entityId, timeout, unit));
    }
    
    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        shutdownTimer.time(()->client.shutdown(timeout, unit));
    }

    public void printStatistics(String desc, Consumer<String> output ) {
        output.accept("Times to " + desc);
        output.accept("  - read schemas        " + readSchemaTimer.statistics());
        output.accept("  - read entities       " + readEntityTimer.statistics());
        output.accept("  - change entities     " + changeEntityTimer.statistics());
        output.accept("  - remove entities     " + removeEntityTimer.statistics());
        output.accept("  - provision databases " + provisionTimer.statistics());
        output.accept("  - connect sessions    " + connectTimer.statistics());
        output.accept("  - batch requests      " + batchTimer.statistics());
        output.accept("  - shutdown            " + shutdownTimer.statistics());
    }
    
    public BatchBuilder createEntities( BatchBuilder batch, int numEdits, int numRemoves, EntityType type ) {
        return services.createEntities(batch, numEdits, numRemoves, type);
    }
}
