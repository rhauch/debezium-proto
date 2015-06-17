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
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Status;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Operation;
import org.debezium.core.message.Topic;

/**
 * A service (or task in Samza parlance) responsible for locally storing schema definitions in a share-nothing approach.
 * Multiple instances of this service do not share storage: each is entirely responsible for the data on the incoming partitions.
 * <p>
 * This service consumes the "{@value Topic#SCHEMA_PATCHES}" topic, where each incoming message is a {@link Patch
 * patch} containing operations on the schema definition.
 * <p>
 * This service produces messages describing the changed schemas on the "{@value Topic#SCHEMA_UPDATES}" topic (partitioned by
 * database identifier), and all read-only requests or errors on the "{@value Topic#PARTIAL_RESPONSES}" topic (partitioned by
 * client identifier).
 * <p>
 * This service uses Samza's storage feature to maintain a durable log of all changes and then uses an in-process database for
 * quick access. If this service fails, another can be restarted and can completely recover the data from the durable log.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class SchemaStorageService implements StreamTask, InitableTask {

    public static final String SEND_RESPONSE_WITH_UDATE = "task.send.response.with.update";

    private static final String SYSTEM_NAME = "kafka";
    private static final SystemStream SCHEMA_UPDATES = new SystemStream(SYSTEM_NAME, Topic.SCHEMA_UPDATES);
    private static final SystemStream PARTIAL_RESPONSES = new SystemStream(SYSTEM_NAME, Topic.PARTIAL_RESPONSES);

    private KeyValueStore<String, Document> store;
    private boolean sendResponseUponUpdate = false;

    public SchemaStorageService() {
        System.out.println("Creating SchemaStorageService instance");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        System.out.println("SchemaStorageService.init(...)");
        this.sendResponseUponUpdate = config.getBoolean(SEND_RESPONSE_WITH_UDATE, sendResponseUponUpdate);
        this.store = (KeyValueStore<String, Document>) context.getStore("schema-store");
        System.out.println("SchemaStorageService.init(...) completed");
    }

    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        try {
            System.out.println("SchemaStorageService.process(...) begin");
            String dbIdStr = (String) env.getKey();
            DatabaseId dbId = Identifier.parseDatabaseId(dbIdStr);
            Document request = (Document) env.getMessage();

            // Construct the patch from the request ...
            Patch<DatabaseId> patch = Patch.from(request);
            assert patch.target().equals(dbId);

            // Construct the response message ...
            Document response = Message.createResponseFromRequest(request);

            // Look up the entity in the store ...
            Document schema = store.get(dbIdStr);

            if (schema == null) {
                // The schema does not exist ...
                if (!patch.isCreation()) {
                    // The entity did not exist ...
                    Message.setStatus(response, Status.DOES_NOT_EXIST);
                    Message.addFailureReason(response, "Database '" + dbIdStr + "' does not exist.");
                    Message.setEnded(response, System.currentTimeMillis());
                    sendResponse(response, dbIdStr, collector);
                    System.out.println("SchemaStorageService.process(...) completed with DOES_NOT_EXIST");
                    return;
                }
                // Otherwise it was a creation, so create it ...
                schema = Document.create();
            } else if (patch.isReadRequest()) {
                // We're reading an existing schema ...
                assert schema != null;
                Message.setAfter(response, schema);
                Message.setEnded(response, System.currentTimeMillis());
                sendResponse(response, dbIdStr, collector);
                System.out.println("SchemaStorageService.process(...) completed read request");
                return;
            }

            // Apply the patch ...
            if (patch.apply(schema, (failedOp) -> record(failedOp, response))) {
                // The schema was successfully changed, so store the changes ...
                store.put(dbIdStr, schema);
                Message.setAfter(response, schema);
                Message.setEnded(response, System.currentTimeMillis());

                // Output the result ...
                collector.send(new OutgoingMessageEnvelope(SCHEMA_UPDATES, dbIdStr, dbIdStr, response));

                // And (depending upon the config) also send the response to the partial responses stream ...
                if (sendResponseUponUpdate) sendResponse(response, dbIdStr, collector);
            } else {
                // Otherwise the patch failed, so just output it as unchanged ...
                sendResponse(response, dbIdStr, collector);
            }
            System.out.println("SchemaStorageService.process(...) completed");
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
