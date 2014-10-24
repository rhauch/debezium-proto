/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import kafka.consumer.TopicFilter;

import org.debezium.api.Database.ChangeEntities;
import org.debezium.api.Database.ChangeSchemaComponents;
import org.debezium.api.Database.Outcome;
import org.debezium.api.Database.OutcomeHandler;
import org.debezium.api.Database.ReadEntities;
import org.debezium.core.DbzNode.Callable;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.SchemaComponentId;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Patch;
import org.debezium.service.Service;

/**
 * @author Randall Hauch
 *
 */
final class DbzDatabases implements Service {
    
    private static final Array EMPTY_ARRAY = Array.create();
    
    private final ConcurrentMap<DatabaseId, ActiveDatabase> activeDatabases = new ConcurrentHashMap<>();
    private final Lock databasesLock = new ReentrantLock();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private OutcomeHandlers<ChangeSchemaComponents> readSchemaHandlers;
    private DbzNode node;
    
    DbzDatabases() {
    }
    
    boolean isActive(DatabaseId id) {
        ActiveDatabase db = activeDatabases.get(id);
        return db != null && db.isActive();
    }
    
    @Override
    public void start(DbzNode node) {
        Lock writeLock = this.lock.writeLock();
        try {
            if (this.node == null) {
                this.node = node;
                this.readSchemaHandlers = new OutcomeHandlers<>(()->RequestId.create(node.id()));
                // Add a single-threaded consumer that will read the top-level topics for database & schema changes ...
                int numThreads = 1;
                String groupId = "databases-" + node.id(); // unique so that all clients see all messages
                TopicFilter topicFilter = Topic.anyOf(Topic.DATABASE_CHANGES.outputTopic(),
                                                      Topic.SCHEMA_CHANGES.outputTopic(),
                                                      Topic.DATABASES_LIST.outputTopic());
                node.subscribe(groupId, topicFilter, numThreads, (topic, partition, offset, key, msg) -> {
                    if (Topic.DATABASES_LIST.isOutputTopic(topic)) {
                        setActiveDatabases(msg.getArray("databaseNames", EMPTY_ARRAY).streamValues());
                    } else if (Topic.DATABASE_CHANGES.isOutputTopic(topic)) {
                        Patch<DatabaseId> patch = Patch.forDatabase(msg);
                        RequestId request = RequestId.from(msg.getString("requestId"));
                        if (patch.isDeletion()) {
                            
                        }
                    } else if (Topic.SCHEMA_CHANGES.isOutputTopic(topic)) {
                        Patch<EntityType> patch = Patch.forEntityType(msg);
                        
                    } else if (Topic.READ_SCHEMA.isOutputTopic(topic)) {
                        Patch<EntityType> patch = Patch.forEntityType(msg);
                        
                    }
                    return true;
                });
                
                // Produce a message to asynchronously get the names of all of the databases ...
                Document msg = Document.create("request", "read");
                node.send(Topic.DATABASES_LIST.inputTopic(), "", msg);
            }
        } finally {
            writeLock.unlock();
        }
    }
    
    @Override
    public void stop() {
        Lock writeLock = this.lock.writeLock();
        try {
            // Our registered consumers will simply terminate, so there's nothing to do except ...
            activeDatabases.clear();
            Logger logger = node.logger(getClass());
            String reason = "Client has been shutdown and is not usable";
            readSchemaHandlers.notifyAndRemoveAll(Outcome.Status.CLIENT_STOPPED, reason, (t) -> {
                logger.error("Failed to handle error in handler: {0}", t.getMessage());
            });
            readSchemaHandlers = null;
            node = null;
        } finally {
            writeLock.unlock();
        }
    }
    
    private void ifRunning(Callable function) {
        Lock readLock = this.lock.readLock();
        try {
            if (node == null) throw new IllegalStateException("Client has been shutdown and is not usable");
            function.call();
        } finally {
            readLock.unlock();
        }
    }
    
    private void setActiveDatabases(Stream<Value> databaseNames) {
        try {
            databasesLock.lock();
            Set<DatabaseId> dbIds = new HashSet<>();
            databaseNames.forEach((value) -> {
                if (value.isString()) {
                    DatabaseId dbId = Identifier.of(value.asString());
                    activeDatabases.putIfAbsent(dbId, new ActiveDatabase(dbId));
                    dbIds.add(dbId);
                }
            });
            // Remove all extras ...
            Iterator<DatabaseId> iter = activeDatabases.keySet().iterator();
            while (iter.hasNext()) {
                DatabaseId id = iter.next();
                if (!dbIds.contains(id)) iter.remove();
            }
        } finally {
            databasesLock.unlock();
        }
    }
    
    private boolean addActiveDatabase(DatabaseId id) {
        try {
            databasesLock.lock();
            return activeDatabases.putIfAbsent(id, new ActiveDatabase(id)) == null;
        } finally {
            databasesLock.unlock();
        }
    }
    
    private boolean removeActiveDatabase(DatabaseId id) {
        try {
            databasesLock.lock();
            return activeDatabases.remove(id) != null;
        } finally {
            databasesLock.unlock();
        }
    }
    
    void readSchema(ExecutionContext context, OutcomeHandler<ChangeSchemaComponents> handler) {
        if (handler != null) throw new IllegalArgumentException("A non-null handler is required to read the schema");
        final long startTime = System.currentTimeMillis();
        ifRunning(() -> {
            RequestId requestId = readSchemaHandlers.register(handler);
            Document request = Requests.readSchemaRequest(context, requestId, startTime);
            node.send(Topic.READ_SCHEMA.inputTopic(), context.databaseId().asString(), request);
        });
    }
    
    void changeSchema(ExecutionContext context, Batch<? extends SchemaComponentId> request, OutcomeHandler<ChangeSchemaComponents> handler) {
    }
    
    void readEntities(ExecutionContext context, Iterable<EntityId> entityIds, OutcomeHandler<ReadEntities> handler) {
    }
    
    void readEntity(ExecutionContext context, EntityId entityId, OutcomeHandler<Entity> handler) {
        readEntities(context, Collections.singleton(entityId), adaptOutcome(handler, (entities) -> entities.get(entityId)));
    }
    
    void changeEntities(ExecutionContext context, Batch<EntityId> request, OutcomeHandler<ChangeEntities> handler) {
    }
    
//    void destroy(ExecutionContext context, OutcomeHandler<ChangeSchema> handler) {
//        final long startTime = System.currentTimeMillis();
//        ifRunning(() -> {
//            DatabaseId dbId = context.databaseId();
//            RequestId requestId = changeSchemaHandlers.register(handler);
//            Document request = Patch.destroy(dbId).asDocument();
//            request.setString("requestId", requestId.asString());
//            request.setString("requestor", context.username());
//            request.setNumber("requestedAt", startTime);
//            node.send(Topic.DATABASE_CHANGES.inputTopic(), dbId.asString(), request);
//        });
//    }
    
    boolean disconnect(DatabaseConnection connection) {
        // Clean up any resources held for the given database connection ...
        return true;
    }
    
    void shutdown() {
    }
    
    private final class ActiveDatabase {
        private final DatabaseId id;
        private volatile Document schema;
        
        protected ActiveDatabase(DatabaseId id) {
            this.id = id;
        }
        
        void setSchema( Document document ) {
            this.schema = document;
        }
        
        public boolean isActive() {
            return true;
        }
    }
    
    protected static <T, U> OutcomeHandler<T> adaptOutcome(OutcomeHandler<U> handler, Function<T, U> adapter) {
        return new OutcomeHandler<T>() {
            @Override
            public void handle(final Outcome<T> outcome) {
                handler.handle(new Outcome<U>() {
                    @Override
                    public boolean succeeded() {
                        return outcome.succeeded();
                    }
                    
                    @Override
                    public boolean failed() {
                        return outcome.failed();
                    }
                    
                    @Override
                    public Outcome.Status status() {
                        return outcome.status();
                    }
                    
                    @Override
                    public String failureReason() {
                        return outcome.failureReason();
                    }
                    
                    @Override
                    public U result() {
                        return adapter.apply(outcome.result());
                    }
                });
            }
        };
    }
}
