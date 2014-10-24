/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.debezium.core.doc.Document;
import org.debezium.core.id.DatabaseId;
import org.debezium.core.id.EntityType;
import org.debezium.core.id.Identifier;
import org.debezium.core.id.SchemaComponentId;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Status;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Operation;
import org.debezium.core.message.Topics;

/**
 * A service (or task in Samza parlance) that is responsible for locally storing schema definitions in a share-nothing approach.
 * Multiple instances of this service do not share storage: each is entirely responsible for the data on the incoming partitions.
 * <p>
 * Each incoming message is a {@link Batch batch} containing one or more {@link Patch patches} on components within the schema.
 * <p>
 * This uses Samza's storage feature, which maintains a durable log of all changes and then uses an in-process database for quick
 * access. If a process containing this service fails, another can be restarted to recover all data because this service persists
 * all changes to the documents in the durable log.
 * 
 * @author Randall Hauch
 *
 */
public class SchemaStorageService implements StreamTask, InitableTask {
    
    private static final SystemStream UNCHANGED_OUTPUT = new SystemStream("kafka",Topics.RESPONSES);
    private static final SystemStream CHANGE_OUTPUT = new SystemStream("kafka",Topics.SCHEMA_UPDATES);
    
    private KeyValueStore<String, Document> store;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
      this.store = (KeyValueStore<String, Document>) context.getStore("schema-store");
    }

    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        DatabaseId dbId = Identifier.parseDatabaseId(env.getKey());
        Document request = (Document)env.getMessage();
        
        // Construct the batch from the request ...
        Batch<SchemaComponentId> batch = Batch.from(request);
        assert batch.appliesTo(dbId);
        final String key = dbId.asString();

        // Construct the response message ...
        Document response = Message.createResponseFrom(request);

        // Look up the schema in the store ...
        Document existingSchema = store.get(key);
        
        if ( batch.isReadRequest() ) {
            // This is a request to only read the schema, so just send it off to the correct output stream ...
            if ( existingSchema == null ) {
                // The schema did not exist ...
                Message.setStatus(response, Status.DOES_NOT_EXIST);
                Message.addFailureReason(response, "Database '" + dbId + "' does not exist.");
            } else {
                Message.setAfter(response, existingSchema);
            }
            collector.send(new OutgoingMessageEnvelope(UNCHANGED_OUTPUT, response));
        }
        
        // Apply each patch ...
        final Document schema = existingSchema != null ? existingSchema : Document.create();
        AtomicBoolean modified = new AtomicBoolean(false);
        batch.forEach((patch)->{
            SchemaComponentId componentId = patch.target();
            switch(componentId.type() ) {
                case ENTITY_TYPE:
                    EntityType type = (EntityType)componentId;
                    Document collections = schema.getOrCreateDocument("collections");
                    Document collection = collections.getOrCreateDocument(type.entityTypeName());
                    if ( patch.apply(collection, (failedOp)->record(failedOp,response)) ) modified.set(true);
                    break;
            }
        });
        
        if ( modified.get() && Message.isSuccess(response) ) {
            // The changes were successful and we modified the schema, so store the changes ...
            store.put(key, schema);

            // Output the result ...
            collector.send(new OutgoingMessageEnvelope(CHANGE_OUTPUT, response));
        } else {
            // Output the failed attempt ...
            collector.send(new OutgoingMessageEnvelope(UNCHANGED_OUTPUT, response));
        }
    }
    
    private void record( Operation failedOperation, Document response ) {
        Message.addFailureReason(response, failedOperation.failureDescription());
        Message.setStatus(response, Status.PATCH_FAILED);
    }
    
}
