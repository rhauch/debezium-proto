/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

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
import org.debezium.core.id.EntityId;
import org.debezium.core.id.Identifier;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Status;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Operation;
import org.debezium.core.message.Topics;

/**
 * A service (or task in Samza parlance) that is responsible for locally storing entities in a share-nothing approach. Multiple
 * instances of this service do not share storage: each is entirely responsible for the data on the incoming partitions.
 * <p>
 * Each incoming message is a {@link Patch patch} for a single entity.
 * <p>
 * This uses Samza's storage feature, which maintains a durable log of all changes and then uses an in-process database for quick
 * access. If a process containing this service fails, another can be restarted to recover all data because this service persists
 * all changes to the documents in the durable log.
 * 
 * @author Randall Hauch
 *
 */
public class EntityStorageService implements StreamTask, InitableTask {
    
    private static final SystemStream UNCHANGED_OUTPUT = new SystemStream("kafka", Topics.RESPONSES);
    private static final SystemStream CHANGE_OUTPUT = new SystemStream("kafka", Topics.ENTITY_UPDATES);
    
    private KeyValueStore<String, Document> store;
    
    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        this.store = (KeyValueStore<String, Document>) context.getStore("entity-store");
    }
    
    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        EntityId id = Identifier.parseEntityId(env.getKey());
        Document request = (Document) env.getMessage();
        
        // Construct the patch from the request ...
        Patch<EntityId> patch = Patch.from(request);
        assert patch.target().equals(id);
        final String key = id.asString();
        
        // Construct the response message ...
        Document response = Message.createResponseFrom(request);
        
        // Look up the entity in the store ...
        Document entity = store.get(key);
        
        if (patch.isReadRequest()) {
            // This is a request to only read the entity, so just send it off to the correct output stream ...
            if (entity == null) {
                // The entity did not exist ...
                Message.setStatus(response, Status.DOES_NOT_EXIST);
                Message.addFailureReason(response, "Entity '" + id + "' does not exist.");
            } else {
                Message.setAfter(response, entity);
            }
            collector.send(new OutgoingMessageEnvelope(UNCHANGED_OUTPUT, response));
        }

        // Apply the patch ...
        if ( entity == null ) {
            // The patch is expected to be a creation ...
            if ( !patch.isCreation() ) {
                // The entity did not exist ...
                Message.setStatus(response, Status.DOES_NOT_EXIST);
                Message.addFailureReason(response, "Entity '" + id + "' does not exist.");
                collector.send(new OutgoingMessageEnvelope(UNCHANGED_OUTPUT, response));
            }
            // Otherwise it was a creation, so create it ...
            entity = Document.create();
        }
        
        if ( patch.apply(entity,(failedOp)->record(failedOp,response))) {
            // The entity was successfully changed, so store the changes ...
            store.put(key, entity);
            
            // Output the result ...
            collector.send(new OutgoingMessageEnvelope(CHANGE_OUTPUT, response));
        }
        
        // Otherwise the patch failed, so just output it as unchanged ...
        collector.send(new OutgoingMessageEnvelope(UNCHANGED_OUTPUT, response));
    }
    
    private void record(Operation failedOperation, Document response) {
        Message.addFailureReason(response, failedOperation.failureDescription());
        Message.setStatus(response, Status.PATCH_FAILED);
    }
    
}
