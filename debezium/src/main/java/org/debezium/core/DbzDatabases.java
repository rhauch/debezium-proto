/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

import kafka.consumer.TopicFilter;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.debezium.api.Database.ChangeCollections;
import org.debezium.api.Database.ChangeEntities;
import org.debezium.api.Database.ChangeFailureCode;
import org.debezium.api.Database.Outcome;
import org.debezium.api.Database.OutcomeHandler;
import org.debezium.api.Database.ReadCollections;
import org.debezium.api.Database.ReadEntities;
import org.debezium.api.DatabaseId;
import org.debezium.api.Entity;
import org.debezium.api.EntityCollection;
import org.debezium.api.EntityId;
import org.debezium.api.EntityType;
import org.debezium.api.Identifier;
import org.debezium.api.doc.Array;
import org.debezium.api.doc.Document;
import org.debezium.api.doc.DocumentReader;
import org.debezium.api.doc.DocumentWriter;
import org.debezium.api.message.Batch;
import org.debezium.api.message.Patch;


/**
 * @author Randall Hauch
 *
 */
final class DbzDatabases {
    
    private static final Array EMPTY_ARRAY = Array.create();
    
    private final DocumentReader docReader = DocumentReader.defaultReader();
    private final DocumentWriter docWriter = DocumentWriter.defaultWriter();
    private final ConcurrentMap<DatabaseId, ActiveDatabase> activeDatabases = new ConcurrentHashMap<>();
    
    DbzDatabases() {
    }
    
    boolean isActive(DatabaseId id) {
        ActiveDatabase db = activeDatabases.get(id);
        return db != null && db.isActive();
    }
    
    void initialize(String uniqueClientId, Producer<String, byte[]> producer, DbzConsumers consumers) {

        // Add a single-threaded consumer that will read the top-level topics for database & schema changes ...
        int numThreads = 1;
        String groupId = uniqueClientId + "-databases"; // unique so that all clients see all messages
        TopicFilter topicFilter = Topic.anyOf(Topic.DATABASE_CHANGES.outputTopic(),
                                              Topic.SCHEMA_CHANGES.outputTopic(),
                                              Topic.DATABASES_LIST.outputTopic());
        consumers.subscribe(groupId, topicFilter, numThreads, (topic, partition, offset, key, message) -> {
            try {
                // All topics use JSON documents for messages ...
                Document msg = docReader.read(message);
                if (Topic.DATABASES_LIST.isOutputTopic(topic)) {
                    // Make sure we have an active database for all of the database names ...
                    msg.getArray("databaseNames",EMPTY_ARRAY).streamValues().forEach((name)->{
                        addDatabase(Identifier.of(name.asString()),producer);
                    });
                } else if (Topic.DATABASE_CHANGES.isOutputTopic(topic)) {
                    Patch<DatabaseId> patch = Patch.forDatabase(msg);
                    
                } else if (Topic.SCHEMA_CHANGES.isOutputTopic(topic)) {
                    Patch<EntityType> patch = Patch.forEntityType(msg);
                    
                }
            } catch (IOException e) {
                // Problem reading message ...
            }
            return true;
        });

        // Produce a message to asynchronously get the names of all of the databases ...
        Document msg = Document.create("request","read");
        send(producer,Topic.DATABASES_LIST.inputTopic(),msg,(e)->log(e,"Unable to request database names from cluster"));
    }
    
    private <T> boolean send( Producer<String,byte[]> producer, String topic, Document msg, Consumer<? super IOException> errorHandler ) {
        try {
            producer.send(new KeyedMessage<>(topic,docWriter.writeAsBytes(msg)));
            return true;
        } catch ( IOException e ) {
            errorHandler.accept(e);
            return false;
        }
    }
    
    private void log( Throwable t, String msg ) {
        System.out.println(msg);
        t.printStackTrace();
    }
    
    private void addDatabase(DatabaseId id, Producer<String, byte[]> producer) {
        if ( !activeDatabases.containsKey(id)) {
            activeDatabases.putIfAbsent(id, new ActiveDatabase(id, producer));
        }
    }
    
    void readSchema(ExecutionContext context, OutcomeHandler<ReadCollections> handler) {
    }
    
    void readSchema(ExecutionContext context, EntityType type, OutcomeHandler<EntityCollection> handler) {
        readSchema(context, adaptOutcome(handler, (collections) -> collections.get(type)));
    }
    
    void changeSchema(ExecutionContext context, Batch<EntityType> request, OutcomeHandler<ChangeCollections> handler) {
    }
    
    void readEntities(ExecutionContext context, Iterable<EntityId> entityIds, OutcomeHandler<ReadEntities> handler) {
    }
    
    void readEntity(ExecutionContext context, EntityId entityId, OutcomeHandler<Entity> handler) {
        readEntities(context, Collections.singleton(entityId), adaptOutcome(handler, (entities) -> entities.get(entityId)));
    }
    
    void changeEntities(ExecutionContext context, Batch<EntityId> request, OutcomeHandler<ChangeEntities> handler) {
    }
    
    void loadData(ExecutionContext context) {
        ActiveDatabase db = activeDatabases.get(context.databaseId());
        String topics[] = { "databases", "schemas-" + context.databaseId() };
        for (String topic : topics) {
            for (int i = 0; i != 100; ++i) {
                Document doc = Document.create("counter", i, "field2", "value2");
                try {
                    byte[] bytes = DocumentWriter.defaultWriter().writeAsBytes(doc);
                    KeyedMessage<String, byte[]> msg = new KeyedMessage<>(topic, bytes);
                    db.producer.send(msg);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    boolean disconnect(DatabaseConnection connection) {
        // Clean up any resources held for the given database connection ...
        return true;
    }
    
    void shutdown() {
    }
    
    private final class ActiveDatabase {
        private final Producer<String, byte[]> producer;
        private final DatabaseId id;
        
        protected ActiveDatabase(DatabaseId id, Producer<String, byte[]> producer) {
            this.id = id;
            this.producer = producer;
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
                    public ChangeFailureCode failureCode() {
                        return outcome.failureCode();
                    }
                    
                    @Override
                    public Throwable cause() {
                        return outcome.cause();
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
