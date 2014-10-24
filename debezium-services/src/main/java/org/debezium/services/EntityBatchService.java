/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.debezium.core.doc.Document;
import org.debezium.core.id.EntityId;
import org.debezium.core.id.Identifier;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Topics;

/**
 * A service (or task in Samza parlance) that simply extracts all of the {@link Patch patches} from a {@link Batch batch} request
 * and forwards each patch as a separate request in the output stream.
 * 
 * @author Randall Hauch
 */
public class EntityBatchService implements StreamTask {
    
    private static final SystemStream OUTPUT = new SystemStream("kafka", Topics.ENTITY_PATCHES);
    
    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        EntityId id = Identifier.parseEntityId(env.getKey());
        Document batchRequest = (Document) env.getMessage();
        
        // Construct the batch from the request ...
        Batch<EntityId> batch = Batch.from(batchRequest);
        assert batch.appliesTo(id);

        // Fire off a separate request for each patch ...
        batch.forEach((patch)->{
            // Construct the response message and fire it off ...
            Document patchRequest = Message.createPatchRequest(batchRequest, patch);
            collector.send(new OutgoingMessageEnvelope(OUTPUT, patchRequest));
        });
    }
}
