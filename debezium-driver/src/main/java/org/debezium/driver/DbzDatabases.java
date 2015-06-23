/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import kafka.consumer.TopicFilter;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.function.Callable;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Topic;
import org.debezium.driver.Database.Outcome;
import org.debezium.driver.DbzNode.Service;
import org.debezium.driver.ResponseHandlers.Handlers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Randall Hauch
 *
 */
final class DbzDatabases extends Service {

    private static final class ActiveDatabase {
        private final Document schema;

        protected ActiveDatabase(DatabaseId id, Document schema) {
            this.schema = schema;
        }

        public Document schema() {
            return schema;
        }
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConcurrentMap<DatabaseId, ActiveDatabase> activeDatabases = new ConcurrentHashMap<>();
    private final ResponseHandlers handlers;

    DbzDatabases(ResponseHandlers handlers) {
        this.handlers = handlers;
    }

    @Override
    public String getName() {
        return "DbzDatabases";
    }

    @Override
    protected void onStart(DbzNode node) {
        logger.debug("Starting Databases. Subscribing to '{}'...", Topic.SCHEMA_UPDATES);
        // Add a single-threaded consumer that will read the "schema-updates" topic to get all database schema updates.
        // We use a unique group ID so that we get *all* the messages on this topic.
        int numThreads = 1;
        String groupId = "databases-" + node.id(); // unique so that all clients see all messages
        TopicFilter topicFilter = Topics.anyOf(Topic.SCHEMA_UPDATES);
        node.subscribe(groupId, topicFilter, numThreads, (topic, partition, offset, key, msg) -> {
            Document updatedSchema = Message.getAfter(msg);
            DatabaseId dbId = Identifier.parseDatabaseId(key);
            activeDatabases.put(dbId, new ActiveDatabase(dbId, updatedSchema));
            logger.debug("Cached active database '{}'...", dbId);
            return true;
        });
    }

    @Override
    protected void beginShutdown(DbzNode node) {
        logger.debug("Beginning shutdown of databases");
    }

    @Override
    protected void completeShutdown(DbzNode node) {
        activeDatabases.clear();
        logger.debug("Completed shutdown of databases");
    }

    DbzConnection provision(ExecutionContext context) {
        return provision(context, context.defaultTimeout(), context.timeoutUnit());
    }

    DbzConnection provision(ExecutionContext context, long timeout, TimeUnit unit) {
        return whenRunning(node -> {
            DatabaseId dbId = context.databaseId();
            ActiveDatabase db = activeDatabases.get(dbId);
            if (db != null) {
                throw new DebeziumProvisioningException("Database '" + dbId + "' already exists");
            }
            logger.debug("Provisioning new database '{}'", dbId);
            db = handlers.requestAndWait(context, timeout, unit, submitCreateSchema(context, dbId, node),
                                         this::updateActiveDatabase, this::provisioningFailed)
                         .orElseThrow(DebeziumConnectionException::new);
            logger.debug("Completed provisioning new database '{}'", dbId);
            return new DbzConnection(this, context);
        }).orElseThrow(DebeziumClientException::new);
    }

    DbzConnection connect(ExecutionContext context) {
        return connect(context, context.defaultTimeout(), context.timeoutUnit());
    }

    DbzConnection connect(ExecutionContext context, long timeout, TimeUnit unit) {
        return whenRunning(node -> {
            activeDatabase(node, context, timeout, unit);
            return new DbzConnection(this, context);
        }).orElseThrow(DebeziumClientException::new);
    }

    private ActiveDatabase activeDatabase(DbzNode node, ExecutionContext context, long timeout, TimeUnit unit) {
        DatabaseId dbId = context.databaseId();
        ActiveDatabase db = activeDatabases.get(dbId);
        if (db == null) {
            logger.debug("Active database info not found for '{}', so attempting to read schema", dbId);
            db = handlers.requestAndWait(context, timeout, unit, submitReadSchema(context, dbId, node),
                                         this::updateActiveDatabase, notAvailable(dbId))
                         .orElseThrow(() -> new DebeziumConnectionException("Unable to connect to " + dbId));
            logger.debug("Found and cached schema for database '{}'", dbId);
        }
        recordConnection(context, dbId, node);
        assert db != null;
        return db;
    }

    private Consumer<RequestId> submitReadSchema(ExecutionContext context, DatabaseId id, DbzNode node) {
        return requestId -> {
            logger.trace("Attempting to submit request to read schema for database '{}'", id);
            Document request = Patch.read(id).asDocument();
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), context.username());
            if (!node.send(Topic.SCHEMA_PATCHES, context.databaseId().asString(), request)) {
                throw new DebeziumClientException("Unable to send request to read schema for " + id);
            }
        };
    }

    private Consumer<RequestId> submitCreateSchema(ExecutionContext context, DatabaseId id, DbzNode node) {
        return requestId -> {
            logger.trace("Attempting to submit request to create schema for database '{}'", id);
            Document request = Patch.create(id).asDocument();
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), context.username());
            if (!node.send(Topic.SCHEMA_PATCHES, context.databaseId().asString(), request)) {
                throw new DebeziumClientException("Unable to send request to create schema for " + id);
            }
        };
    }

    private void recordConnection(ExecutionContext context, DatabaseId id, DbzNode node) {
        logger.trace("Established new connection to database '{}'", id);
        Document msg = Message.createConnectionMessage(node.id(), id, context.username(), context.device(), context.version());
        node.send(Topic.CONNECTIONS, context.username(), msg);
    }

    private ActiveDatabase updateActiveDatabase(Document schemaReadResponse) {
        DatabaseId dbId = Message.getDatabaseId(schemaReadResponse);
        if (Message.isSuccess(schemaReadResponse)) {
            Document schema = Message.getAfter(schemaReadResponse);
            ActiveDatabase db = new ActiveDatabase(dbId, schema);
            activeDatabases.put(dbId, db);
            logger.debug("Caching updated schema for database '{}'", dbId);
            return db;
        }
        return null;
    }

    private BiConsumer<Outcome.Status, String> notAvailable(DatabaseId dbId) {
        return (status, reason) -> {
            throw new DebeziumConnectionException("The database '" + dbId + "' is not available");
        };
    }

    private void provisioningFailed(Outcome.Status status, String reason) {
        throw new DebeziumProvisioningException(reason);
    }

    boolean disconnect(DbzConnection connection) {
        logger.trace("Disconnecting connection from database '{}'", connection.databaseId());
        // Clean up any resources held for the given database connection ...
        return true;
    }

    void readSchema(ExecutionContext context, Handlers handlers) {
        if (handlers == null) throw new IllegalArgumentException("A non-null handler is required to read entities");
        // We should always have one locally since we're connected ...
        whenRunning(node -> {
            logger.debug("Reading the schema for database '{}'", context.databaseId());
            Document schema = activeDatabase(node, context, context.defaultTimeout(), context.timeoutUnit()).schema();
            handlers.successHandler.ifPresent(c -> c.accept(schema));
            handlers.completionHandler.ifPresent(Callable::call);
            return true;
        });
    }

    void readEntities(ExecutionContext context, Iterable<EntityId> entityIds, Handlers handlers) {
        if (handlers == null) throw new IllegalArgumentException("A non-null handler is required to read entities");
        whenRunning(node -> {
            logger.debug("Reading entities in database '{}'", context.databaseId());
            Batch<EntityId> batch = Batch.<EntityId> create().read(entityIds).build();
            Document request = batch.asDocument();
            RequestId requestId = this.handlers.register(context, batch.patchCount(), handlers).orElseThrow(DebeziumClientException::new);
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), context.username());
            Message.addId(request, context.databaseId());
            node.send(Topic.ENTITY_BATCHES, requestId.asString(), request);
            return requestId;
        }).orElseThrow(DebeziumClientException::new);
    }

    void changeEntities(ExecutionContext context, Batch<EntityId> batch, Handlers handlers) {
        if (handlers == null) throw new IllegalArgumentException("A non-null handler is required to change entities");
        whenRunning(node -> {
            logger.debug("Submitting batch to change {} entities in database '{}'", batch.patchCount(), context.databaseId());
            RequestId requestId = this.handlers.register(context, batch.patchCount(), handlers).orElseThrow(DebeziumClientException::new);
            Document request = batch.asDocument();
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), context.username());
            Message.addId(request, context.databaseId());
            node.send(Topic.ENTITY_BATCHES, requestId.asString(), request);
            return requestId;
        }).orElseThrow(DebeziumClientException::new);
    }
}
