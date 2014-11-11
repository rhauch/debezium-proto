/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import kafka.consumer.TopicFilter;

import org.debezium.client.Database.Outcome;
import org.debezium.client.DbzNode.Service;
import org.debezium.client.ResponseHandlers.Handlers;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.function.Callable;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Topic;

/**
 * @author Randall Hauch
 *
 */
final class DbzDatabases extends Service {
    
    private static final class ActiveDatabase {
        private final DatabaseId id;
        private final Document schema;
        
        protected ActiveDatabase(DatabaseId id, Document schema) {
            this.id = id;
            this.schema = schema;
        }
        
        public boolean isActive() {
            return true;
        }
        
        public DatabaseId id() {
            return id;
        }
        
        public Document schema() {
            return schema;
        }
    }
    
    private final ConcurrentMap<DatabaseId, ActiveDatabase> activeDatabases = new ConcurrentHashMap<>();
    private final ResponseHandlers handlers;
    
    DbzDatabases(ResponseHandlers handlers) {
        this.handlers = handlers;
    }
    
    @Override
    protected void onStart(DbzNode node) {
        // Add a single-threaded consumer that will read the "schema-updates" topic to get all database schema updates.
        // We use a unique group ID so that we get *all* the messages on this topic.
        int numThreads = 1;
        String groupId = "databases-" + node.id(); // unique so that all clients see all messages
        TopicFilter topicFilter = Topics.anyOf(Topic.SCHEMA_UPDATES);
        node.subscribe(groupId, topicFilter, numThreads, (topic, partition, offset, key, msg) -> {
            Document updatedSchema = Message.getAfter(msg);
            DatabaseId dbId = Identifier.parseDatabaseId(key);
            activeDatabases.put(dbId, new ActiveDatabase(dbId, updatedSchema));
            return true;
        });
    }
    
    @Override
    protected void beginShutdown(DbzNode node) {
    }
    
    @Override
    protected void completeShutdown(DbzNode node) {
        activeDatabases.clear();
    }
    
    DbzConnection provision(ExecutionContext context, long timeout, TimeUnit unit) {
        return whenRunning(node -> {
            DatabaseId dbId = context.databaseId();
            ActiveDatabase db = activeDatabases.get(dbId);
            if (db != null) {
                throw new DebeziumProvisioningException("Unable to provision database '" + dbId + "'");
            }
            db = handlers.requestAndWait(context, timeout, unit, submitCreateSchema(context, dbId, node),
                                         this::updateActiveDatabase, this::provisioningFailed)
                                         .orElseThrow(DebeziumConnectionException::new);
            return new DbzConnection(this, context);
        }).orElseThrow(DebeziumClientException::new);
    }
    
    DbzConnection connect(ExecutionContext context, long timeout, TimeUnit unit) {
        return whenRunning(node -> {
            activeDatabase(node,context,timeout,unit);
            return new DbzConnection(this, context);
        }).orElseThrow(DebeziumClientException::new);
    }
    
    private ActiveDatabase activeDatabase(DbzNode node, ExecutionContext context, long timeout, TimeUnit unit) {
        DatabaseId dbId = context.databaseId();
        ActiveDatabase db = activeDatabases.get(dbId);
        if (db == null) {
            db = handlers.requestAndWait(context, timeout, unit, submitReadSchema(context, dbId, node),
                                         this::updateActiveDatabase, notAvailable(dbId))
                                         .orElseThrow(DebeziumConnectionException::new);
        }
        assert db != null;
        return db;
    }
    
    private Consumer<RequestId> submitReadSchema(ExecutionContext context, DatabaseId id, DbzNode node) {
        return requestId -> {
            Document request = Patch.read(id).asDocument();
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), context.username());
            if ( !node.send(Topic.SCHEMA_PATCHES, context.databaseId().asString(), request) ) {
                throw new DebeziumClientException("Unable to send request to read schema for " + id);
            }
        };
    }
    
    private Consumer<RequestId> submitCreateSchema(ExecutionContext context, DatabaseId id, DbzNode node) {
        return requestId -> {
            Document request = Patch.create(id).asDocument();
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), context.username());
            if ( !node.send(Topic.SCHEMA_PATCHES, context.databaseId().asString(), request) ) {
                throw new DebeziumClientException("Unable to send request to create schema for " + id);
            }
        };
    }
    
    private ActiveDatabase updateActiveDatabase(Document schemaReadResponse) {
        DatabaseId dbId = Message.getDatabaseId(schemaReadResponse);
        if ( Message.isSuccess(schemaReadResponse)) {
            Document schema = Message.getAfter(schemaReadResponse);
            ActiveDatabase db = new ActiveDatabase(dbId, schema);
            activeDatabases.put(dbId, db);
            return db;
        }
        return null;
    }
    
    private BiConsumer<Outcome.Status, String> notAvailable(DatabaseId dbId) {
        return (status, reason) -> {
            throw new DebeziumConnectionException("The database '" + dbId + "' is not available");
        };
    }
    
    private void provisioningFailed( Outcome.Status status, String reason ) {
        throw new DebeziumProvisioningException(reason);
    }
    
    boolean disconnect(DbzConnection connection) {
        // Clean up any resources held for the given database connection ...
        return true;
    }
    
    void readSchema(ExecutionContext context, Handlers handlers) {
        if (handlers == null) throw new IllegalArgumentException("A non-null handler is required to read entities");
        // We should always have one locally since we're connected ...
        whenRunning(node -> {
            Document schema = activeDatabase(node,context,10,TimeUnit.SECONDS).schema();
            handlers.successHandler.ifPresent(c->c.accept(schema));
            handlers.completionHandler.ifPresent(Callable::call);
            return true;
        });
    }
    
    void readEntities(ExecutionContext context, Iterable<EntityId> entityIds, Handlers handlers) {
        if (handlers == null) throw new IllegalArgumentException("A non-null handler is required to read entities");
        whenRunning(node -> {
            RequestId requestId = this.handlers.register(context, 1, handlers).orElseThrow(DebeziumClientException::new);
            Batch<EntityId> batch = Batch.<EntityId> create().read(entityIds).build();
            Document request = batch.asDocument();
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), context.username());
            node.send(Topic.ENTITY_BATCHES, requestId.asString(), request);
            return requestId;
        }).orElseThrow(DebeziumClientException::new);
    }
    
    
    void changeEntities(ExecutionContext context, Batch<EntityId> batch, Handlers handlers) {
        if (handlers == null) throw new IllegalArgumentException("A non-null handler is required to change entities");
        whenRunning(node -> {
            RequestId requestId = this.handlers.register(context, batch.patchCount(), handlers).orElseThrow(DebeziumClientException::new);
            Document request = batch.asDocument();
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), context.username());
            node.send(Topic.ENTITY_BATCHES, requestId.asString(), request);
            return requestId;
        }).orElseThrow(DebeziumClientException::new);
    }
    
}
