/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;

/**
 * A service (or task in Samza parlance) to extract all of the {@link Patch patches} from a {@link Batch batch} request
 * and forward each patch as a separate request in the output stream.
 * <p>
 * This service consumes the "{@link Streams#entityBatches entity-batches}" topic, where each incoming message is a {@link Batch
 * batch} containing one or more {@link Patch patches} on entities in the same database.
 * <p>
 * This service produces a message for each patch on the "{@link Streams#entityPatches entity-patches}" topic.
 * <p>
 * This service produces messages on the "{@link Streams#entityPatches entity-patches}" topic.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class EntityBatchService implements StreamTask {

    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        // The key is a random request number ...
        Document batchRequest = (Document) env.getMessage();
        DatabaseId dbId = Message.getDatabaseId(batchRequest);

        // Construct the batch from the request ...
        Batch<EntityId> batch = Batch.from(batchRequest);

        // Fire off a separate request for each patch ...
        int parts = batch.patchCount();
        AtomicInteger partCounter = new AtomicInteger(0);
        batch.forEach(patch -> {
            // Construct the response message and fire it off ...
            Document patchRequest = Message.createPatchRequest(batchRequest, patch);

            // Set the headers and the request parts ...
            Message.copyHeaders(batchRequest, patchRequest);
            Message.setParts(patchRequest, partCounter.incrementAndGet(), parts);

            EntityId entityId = patch.target();
            if (dbId.equals(entityId.databaseId())) {
                // Send the message for this patch (only if the patched entity is in the same database as the batch request) ...
                String msgId = entityId.asString();
                collector.send(new OutgoingMessageEnvelope(Streams.entityPatches(dbId), msgId, patchRequest));
            }
        });
    }
}
