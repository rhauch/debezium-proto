/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

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
import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Status;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Operation;
import org.debezium.core.message.Topic;

/**
 * A service (or task in Samza parlance) responsible for locally storing entities in a share-nothing approach. Multiple
 * instances of this service do not share storage: each is entirely responsible for the data on the incoming partitions.
 * <p>
 * This service consumes the {@value Topic#ENTITY_PATCHES} topic, where each incoming message is a {@link Patch
 * patch} for a single entity.
 * <p>
 * This service produces messages describing the changed entities on the {@value Topic#ENTITY_UPDATES} topic, and
 * all read-only requests or errors on the {@value Topic#PARTIAL_RESPONSES} topic.
 * <p>
 * This service uses Samza's storage feature to maintain a durable log of all changes and then use an in-process database for
 * quick access. If this service fails, another can be restarted and can completely recover the cache from the durable log.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class EntityStorageService implements StreamTask, InitableTask {

    private static final String SYSTEM_NAME = "kafka";
    private static final SystemStream ENTITY_UPDATES = new SystemStream(SYSTEM_NAME, Topic.ENTITY_UPDATES);
    private static final SystemStream PARTIAL_RESPONSES = new SystemStream(SYSTEM_NAME, Topic.PARTIAL_RESPONSES);

    private KeyValueStore<String, Document> store;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        this.store = (KeyValueStore<String, Document>) context.getStore("entity-store");
    }

    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        try {
            String idStr = (String) env.getKey();
            EntityId id = Identifier.parseEntityId(idStr);
            Document request = (Document) env.getMessage();

            // Construct the patch from the request ...
            Patch<EntityId> patch = Patch.from(request);
            assert patch.target().equals(id);

            // Construct the response message ...
            Document response = Message.createResponseFromRequest(request);

            // Look up the entity in the store ...
            Document entity = store.get(idStr);

            if ( patch.isReadRequest() ) {
                if ( entity == null ) {
                    // The entity does not exist ...
                    Message.setStatus(response, Status.DOES_NOT_EXIST);
                    Message.addFailureReason(response, "Entity '" + id + "' does not exist.");
                    Message.setEnded(response, System.currentTimeMillis());
                    sendResponse(response, idStr, collector);
                } else {
                    // We're reading an existing entity ...
                    assert entity != null;
                    Message.setAfter(response, entity);
                    Message.setEnded(response, System.currentTimeMillis());
                    sendResponse(response, idStr, collector);
                }
                return;
            }
            
            // Add the patch operations to the response ...
            Message.setOperations(response,request);
            
            // Make sure there is an entity document ...
            if (entity == null) {
                entity = Document.create();
            } else {
                // and if there capture it as the 'before' ...
                Message.setBefore(response, entity.clone());
            }
            
            // Apply the patch, which may create the entity ...
            if (patch.apply(entity, (failedOp) -> record(failedOp, response))) {
                // The entity was successfully changed, so store the changes ...
                store.put(idStr, entity);
                Message.setAfter(response, entity.clone());
                Message.setEnded(response, System.currentTimeMillis());

                // Output the result ...
                collector.send(new OutgoingMessageEnvelope(ENTITY_UPDATES, idStr, idStr, response));

                // And also send the response to the partial responses stream ...
                sendResponse(response, idStr, collector);
            } else {
                // Could not apply the patch, so just output it as unchanged (with the failure recorded) ...
                Message.setEnded(response, System.currentTimeMillis());
                sendResponse(response, idStr, collector);
            }

        } catch (RuntimeException t) {
            t.printStackTrace();
            throw t;
        }
    }

    private void sendResponse(Document response, String idStr, MessageCollector collector) {
        String clientId = Message.getClient(response);
        collector.send(new OutgoingMessageEnvelope(PARTIAL_RESPONSES, clientId, idStr, response));
    }

    private void record(Operation failedOperation, Document response) {
        Message.addFailureReason(response, failedOperation.failureDescription());
        Message.setStatus(response, Status.PATCH_FAILED);
    }

}
