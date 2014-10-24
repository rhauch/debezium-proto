/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.Schema;
import org.debezium.core.component.SchemaComponentId;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;

/**
 * A service (or task in Samza parlance) responsible for re-partitioning the changes to entities and schemas onto a single
 * topic (partitioned by entity type) that the {@link EntityLearningService} can consume.
 * <p>
 * This service consumes the "{@link Streams#entityUpdates entity-updates}" topic, where each incoming message is a
 * successfully-applied {@link Patch patch} for a single entity. It also consumes the "{@link Streams#schemaUpdates
 * schema-updates}" topic, where each incoming message is a successfully-updated schema (some of which might have originated from
 * this service instance, whereas others originated from {@link Patch patch} for a single entity. <em>Note: the
 * schema updates should be prioritized higher than the entity updates.</em>
 * <p>
 * This service forwards these messages on to the "{@link Streams#schemaLearning schema-learning}" topic, partitioned by entity
 * type. Entity updates are forwarded as-is, but each updated schema component is sent separately as a completed patch request.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class EntityLearningPartitioner implements StreamTask {
    
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        SystemStreamPartition stream = envelope.getSystemStreamPartition();
        if (Streams.isEntityUpdates(stream)) {
            processEntityUpdate(envelope, collector, coordinator);
        } else if (Streams.isSchemaUpdates(stream)) {
            processSchemaUpdate(envelope, collector, coordinator);
        }
    }
    
    private void processSchemaUpdate(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator)
            throws Exception {
        DatabaseId dbId = Identifier.parseDatabaseId(env.getKey());
        Document update = (Document) env.getMessage();
        if (Message.isFromClient(update, EntityLearningService.CLIENT_ID)) {
            // This schema change came from the learning service, so we can disregard it ...
            // TODO: Unless it was a failed attempt, although they won't come through this topic!!
            return;
        }
        
        // Get the complete updated schema representation ...
        Document schema = Message.getAfter(update);
        
        // Process each patch within the batch ...
        Batch<? extends SchemaComponentId> batch = Batch.from(update);
        batch.forEach((patch) -> {
            // Create a new request message for the patch ...
            Document patchRequest = Message.createPatchRequest(update, patch);
            
            // Get the representation of the component from the schema ...
            SchemaComponentId componentId = patch.target();
            if (componentId instanceof EntityType) {
                Document representation = Schema.getOrCreateComponent(componentId, schema);
                Message.setAfter(patchRequest, representation);
                
                // Send the request to the output stream, partitioned by the entity type ...
                collector.send(new OutgoingMessageEnvelope(Streams.schemaLearning(dbId), componentId, componentId, patchRequest));
            }
        });
    }
    
    private void processEntityUpdate(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator)
            throws Exception {
        EntityId entityId = Identifier.parseEntityId(env.getKey());
        DatabaseId dbId = entityId.databaseId();
        EntityType type = entityId.type();
        
        // Send the request to the output stream, partitioned by the entity type ...
        collector.send(new OutgoingMessageEnvelope(Streams.schemaLearning(dbId), type, entityId, env.getMessage()));
    }
    
}
