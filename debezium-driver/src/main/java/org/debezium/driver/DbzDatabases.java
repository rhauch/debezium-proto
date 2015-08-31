/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import kafka.consumer.TopicFilter;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Randall Hauch
 *
 */
final class DbzDatabases extends DbzNode.Service {

    private static final class ActiveDatabase {
        private final DbzSchema schema;

        protected ActiveDatabase(DatabaseId id, Document schemaDoc) {
            this.schema = new DbzSchema(id,schemaDoc);
        }

        public Schema schema() {
            return schema;
        }
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConcurrentMap<String, ActiveDatabase> activeDatabases = new ConcurrentHashMap<>();
    private final DbzPartialResponses responses;

    DbzDatabases(DbzPartialResponses responses) {
        this.responses = responses;
    }

    @Override
    public String getName() {
        return "databases";
    }

    @Override
    protected void onStart(DbzNode node) {
        logger.debug("DATABASES: Starting and subscribing to '{}'...", Topic.SCHEMA_UPDATES);
        // Add a single-threaded consumer that will read the "schema-updates" topic to get all database schema updates.
        // We use a unique group ID so that we get *all* the messages on this topic.
        int numThreads = 1;
        String groupId = "databases-" + node.id(); // unique so that all clients see all messages
        TopicFilter topicFilter = DbzTopics.anyOf(Topic.SCHEMA_UPDATES);
        node.subscribe(groupId, topicFilter, numThreads, (topic, partition, offset, key, msg) -> {
            Document updatedSchema = Message.getAfter(msg);
            DatabaseId dbId = Identifier.parseDatabaseId(key);
            activeDatabases.put(dbId.asString(), new ActiveDatabase(dbId, updatedSchema));
            logger.debug("DATABASES: Cached active database '{}'...", dbId);
            return true;
        });
    }

    @Override
    protected void beginShutdown(DbzNode node) {
        logger.debug("DATABASES: Beginning shutdown of databases");
    }

    @Override
    protected void completeShutdown(DbzNode node) {
        activeDatabases.clear();
        logger.debug("DATABASES: Completed shutdown of databases");
    }

    void provision( String username, DatabaseId dbId, long timeout, TimeUnit unit ) {
        whenRunning(node -> {
            ActiveDatabase db = activeDatabases.get(dbId);
            if (db != null) {
                throw new DebeziumProvisioningException("Database '" + dbId + "' already exists");
            }
            logger.debug("DATABASES: Provisioning new database '{}'", dbId);
            return responses.submit(Boolean.class, requestId -> {
                logger.trace("DATABASES: Attempting to submit request to create schema for database '{}'", dbId);
                Document request = Patch.create(dbId).asDocument();
                Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), username);
                if (!node.send(Topic.SCHEMA_PATCHES, dbId.asString(), request)) {
                    throw new DebeziumClientException("Unable to send request to create schema for " + dbId);
                }
            }).onResponse(timeout,unit, response -> {
                logger.debug("DATABASES: Reading schema for new database '{}'", dbId);
                Message.getFirstFailureReason(response).ifPresent(msg->{
                    throw new DebeziumProvisioningException("Unable to provision database '" + dbId + "': " + msg);
                });
                assert Message.isSuccess(response);
                Document schema = Message.getAfter(response);
                activeDatabases.put(dbId.asString(), new ActiveDatabase(dbId, schema));
                logger.debug("DATABASES: Caching updated schema for database '{}'", dbId);
                return Boolean.TRUE;
            }).onTimeout(() -> {
                throw new DebeziumProvisioningException("Timed out while waiting for provision request to be processed.");
            });
        }).orElseThrow(()->new DebeziumClientException("The Debezium driver is not running"));
    }

    Set<String> existing( String... databaseIds ) {
        Set<String> result = new HashSet<>();
        for ( String dbId : databaseIds ) {
            if ( activeDatabases.containsKey(dbId) ) result.add(dbId);
        }
        return result;
    }
    
    Schema readSchema( String dbId ) {
        return activeDatabases.get(dbId).schema();
    }
}
