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
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Topic;

/**
 * A service (or task in Samza parlance) to accumulate all of the partial responses, and when all parts are available to
 * publish them the aggregate (containing all partial responses) on the "{@value Topic#COMPLETE_RESPONSES}"
 * topic, which is partitioned by client ID.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class ResponseAccumulatorService implements StreamTask, InitableTask {
    
    private static final SystemStream COMPLETE_RESPONSES = new SystemStream("kafka", Topic.COMPLETE_RESPONSES);

    private KeyValueStore<String, Document> cache;
    
    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        this.cache = (KeyValueStore<String, Document>) context.getStore("responses-cache");
    }
    
    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String responseId = (String) env.getKey();
        Document response = (Document) env.getMessage();
        String clientId = Message.getClient(response);
        if (Message.getParts(response) == 1) {
            // This is the only message in the batch, so forward it on directly ...
            collector.send(new OutgoingMessageEnvelope(COMPLETE_RESPONSES, clientId, responseId, response));
            return;
        }
        
        // Otherwise, there is more than 1 part to the batch ...
        String responseKey = clientId + "/" + Message.getRequest(response);
        Document aggregateResponse = cache.get(responseKey);
        boolean done = false;
        if (aggregateResponse == null) {
            // This is the first part we've seen ...
            aggregateResponse = Message.createAggregateResponseFrom(response);
        } else {
            // We already have an aggregate ...
            done = Message.addToAggregateResponse(aggregateResponse, response);
        }
        
        if (done) {
            // FIRST send the message ...
            collector.send(new OutgoingMessageEnvelope(COMPLETE_RESPONSES, clientId, responseId, aggregateResponse));
            // And only if that is successful THEN remove from the cache ...
            this.cache.delete(responseKey);
        } else {
            // Update the cache with the updated but still incomplete aggregate response ...
            this.cache.put(responseKey, aggregateResponse);
        }
    }
}
