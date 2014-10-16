/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import kafka.consumer.TopicFilter;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.debezium.api.DatabaseId;
import org.debezium.api.EntityType;
import org.debezium.api.doc.Array;
import org.debezium.api.doc.Document;
import org.debezium.api.doc.DocumentReader;
import org.debezium.api.doc.DocumentWriter;
import org.debezium.api.message.Patch;
import org.debezium.api.message.Patch.Add;

/**
 * @author Randall Hauch
 *
 */
final class DbzServices {
    
    private final DocumentReader docReader = DocumentReader.defaultReader();
    private final DocumentWriter docWriter = DocumentWriter.defaultWriter();
    private final ConcurrentMap<DatabaseId,Document> schemasByDatabases = new ConcurrentHashMap<DatabaseId,Document>();
    
    DbzServices() {
    }
    
    void initialize(String uniqueClientId, Producer<String, byte[]> producer, DbzConsumers consumers) {
        // Add a single-threaded consumer that reads inputs to database and schema topics ...
        int numThreads = 1;
        String groupId = "database-service"; // not unique so that they're only processed once ...
        TopicFilter topicFilter = Topic.anyOf(Topic.DATABASE_CHANGES.inputTopic(),
                                              Topic.SCHEMA_CHANGES.inputTopic(),
                                              Topic.DATABASES_LIST.inputTopic());
        consumers.subscribe(groupId, topicFilter, numThreads, (topic, partition, offset, key, message) -> {
            try {
                // All topics use JSON documents for messages ...
                Document request = docReader.read(message);
                if (Topic.DATABASES_LIST.isInputTopic(topic)) {
                    // Put the list of databases into the output topic ...
                    Document msg = Document.create("databaseNames",Array.create());
                    send(producer,Topic.DATABASES_LIST.outputTopic(),msg,(e)->log(e,"Unable to send database names"));
                } else if (Topic.DATABASE_CHANGES.isInputTopic(topic)) {
                    // Read the change to the database ...
                    Patch<DatabaseId> patch = Patch.forDatabase(request);
                    DatabaseId dbId = patch.target();
                    if ( patch.isCreation() ) {
                        // Request to create a new database ...
                        schemasByDatabases.putIfAbsent(dbId, Document.create());
                    } else if ( patch.isDeletion() ) {
                        // Delete an existing database ...
                        schemasByDatabases.remove(dbId);
                    }
                } else if (Topic.SCHEMA_CHANGES.isOutputTopic(topic)) {
                    Patch<EntityType> patch = Patch.forEntityType(request);
                    EntityType type = patch.target();
                    DatabaseId dbId = type.getDatabaseId();
                    Document schema = schemasByDatabases.get(dbId);
                    String typeName = type.getEntityTypeName();
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
            } catch (IOException e) {
                // Problem reading message ...
            }
            return true;
        });
        
    }

    void shutdown() {
    }
    
    protected <T> boolean send( Producer<String,byte[]> producer, String topic, Document msg, Consumer<? super IOException> errorHandler ) {
        try {
            producer.send(new KeyedMessage<>(topic,docWriter.writeAsBytes(msg)));
            return true;
        } catch ( IOException e ) {
            errorHandler.accept(e);
            return false;
        }
    }

    protected void log( Throwable t, String msg ) {
        System.out.println(msg);
        t.printStackTrace();
    }
}
