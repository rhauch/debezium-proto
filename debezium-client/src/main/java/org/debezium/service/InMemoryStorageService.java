/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kafka.consumer.TopicFilter;

import org.debezium.core.DbzNode;
import org.debezium.core.Topic;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityType;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Add;

/**
 * @author Randall Hauch
 *
 */
public class InMemoryStorageService implements Service {

    private final ConcurrentMap<DatabaseId,Document> schemasByDatabaseIds = new ConcurrentHashMap<>();
    
    public InMemoryStorageService() {
    }
    
    @Override
    public void start(DbzNode node) {
        // Add a single-threaded consumer that reads inputs to database and schema topics ...
        int numThreads = 1;
        String groupId = "database-service"; // not unique so that they're only processed once ...
        TopicFilter topicFilter = Topic.anyOf(Topic.DATABASE_CHANGES.inputTopic(),
                                              Topic.SCHEMA_CHANGES.inputTopic(),
                                              Topic.DATABASES_LIST.inputTopic());
        // Create a consumer that handles each topic ...
        node.subscribe(groupId, topicFilter, numThreads, (topic, partition, offset, key, request) -> {
            // All topics use JSON documents for messages ...
            if (Topic.DATABASES_LIST.isInputTopic(topic)) {
                // Put the list of databases into the output topic ...
                Document msg = Document.create("databaseNames",databaseNames());
                node.send(Topic.DATABASES_LIST.outputTopic(),"",msg);
            } else if (Topic.DATABASE_CHANGES.isInputTopic(topic)) {
                // Read the change to the database ...
                Patch<DatabaseId> patch = Patch.forDatabase(request);
                DatabaseId dbId = patch.target();
                Optional<Value> created = patch.createdValue();
                if ( created.isPresent() ) {
                    // Request to create a new database ...
                    Document doc = created.get().asDocument();
                    if ( doc == null ) doc = Document.create();
                    schemasByDatabaseIds.putIfAbsent(dbId, doc);
                } else if ( patch.isDeletion() ) {
                    // Delete an existing database ...
                    schemasByDatabaseIds.remove(dbId);
                }
            } else if (Topic.SCHEMA_CHANGES.isOutputTopic(topic)) {
                Patch<EntityType> patch = Patch.forEntityType(request);
                EntityType type = patch.target();
                DatabaseId dbId = type.databaseId();
                Document schema = schemasByDatabaseIds.get(dbId);
                String typeName = type.entityTypeName();
                if ( schema != null ) {
                    if ( patch.isCreation() ) {
                        // Add the entity definition ...
                        patch.stream().forEach((op)->{
                            if ( op.action() == Patch.Action.ADD ) {
                                schema.setDocument(typeName,((Add)op).value().asDocument());
                            }
                        });
                    } else if ( patch.isDeletion() ) {
                        // Remove the entity definition ...
                        schema.remove(typeName);
                    } else {
                        // Apply the changes to the entity type's document ...
                        Document entity = schema.getOrCreateDocument(typeName);
                        patch.apply(entity,(failed)->{
                            // Record which operation failed ...
                        });
                    }
                }
                
            }
            return true;
        });
    }
    
    protected Array databaseNames() {
        Array dbIds = Array.create();
        schemasByDatabaseIds.keySet().forEach((dbId)->dbIds.add(dbId.asString()));
        return dbIds;
    }
    
    @Override
    public void stop() {
    }
    
}
