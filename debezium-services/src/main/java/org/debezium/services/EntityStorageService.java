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
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Status;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Operation;

/**
 * A service (or task in Samza parlance) responsible for locally storing entities in a share-nothing approach. Multiple
 * instances of this service do not share storage: each is entirely responsible for the data on the incoming partitions.
 * <p>
 * This service consumes the "{@link Streams#entityPatches entity-patches}" topic, where each incoming message is a {@link Patch
 * patch} for a single entity.
 * <p>
 * This service produces messages describing the changed entities on the "{@link Streams#entityUpdates entity-updates}" topic, and
 * all read-only requests or errors on the "{@link Streams#partialResponses() partial-responses}" topic.
 * <p>
 * This service uses Samza's storage feature to maintain a durable log of all changes and then use an in-process database for
 * quick access. If this service fails, another can be restarted and can completely recover the cache from the durable log.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class EntityStorageService implements StreamTask, InitableTask {
    
    private KeyValueStore<String, Document> store;
    private boolean sendResponseUponUpdate = false;
    
    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        this.sendResponseUponUpdate = config.getBoolean("", sendResponseUponUpdate);
        this.store = (KeyValueStore<String, Document>) context.getStore("entity-store");
    }
    
    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String idStr = (String) env.getKey();
        EntityId id = Identifier.parseEntityId(idStr);
        DatabaseId dbId = id.databaseId();
        Document request = (Document) env.getMessage();
        
        // Construct the patch from the request ...
        Patch<EntityId> patch = Patch.from(request);
        assert patch.target().equals(id);
        
        // Construct the response message ...
        Document response = Message.createResponseFromRequest(request);
        
        // Look up the entity in the store ...
        Document entity = store.get(idStr);
        
        if (entity == null) {
            // The entity does not exist ...
            if (!patch.isCreation()) {
                // The entity did not exist ...
                Message.setStatus(response, Status.DOES_NOT_EXIST);
                Message.addFailureReason(response, "Entity '" + id + "' does not exist.");
                sendResponse(response, idStr, collector);
            }
            // Otherwise it was a creation, so create it ...
            entity = Document.create();
        } else if (patch.isReadRequest()) {
            // We're reading an existing entity ...
            assert entity != null;
            Message.setAfter(response, entity);
            sendResponse(response, idStr, collector);
        }
        
        // Apply the patch ...
        if (patch.apply(entity, (failedOp) -> record(failedOp, response))) {
            // The entity was successfully changed, so store the changes ...
            store.put(idStr, entity);
            
            // Output the result ...
            collector.send(new OutgoingMessageEnvelope(Streams.entityUpdates(dbId), idStr, idStr, response));

            // And (depending upon the config) also send the response to the partial responses stream ...
            if ( sendResponseUponUpdate ) sendResponse(response,idStr, collector);
        }
        
        // Otherwise the patch failed, so just output it as unchanged ...
        sendResponse(response, idStr, collector);
    }
    
    private void sendResponse(Document response, String idStr, MessageCollector collector) {
        String clientId = Message.getClient(response);
        collector.send(new OutgoingMessageEnvelope(Streams.partialResponses(), clientId, idStr, response));
    }
    
    private void record(Operation failedOperation, Document response) {
        Message.addFailureReason(response, failedOperation.failureDescription());
        Message.setStatus(response, Status.PATCH_FAILED);
    }
    
}
