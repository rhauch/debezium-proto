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
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Action;
import org.debezium.core.message.Message.Status;

/**
 * A service (or task in Samza parlance) responsible for coalescing all of the changes and notifications for each device.
 * <p>
 * This service consumes the "{@link Streams#changesByDevice changes-by-device}" topic (partitioned by device), where each
 * incoming message is keyed by the device and contains information about the entity and whether it was {@link Action#CREATED
 * created}, {@link Action#UPDATED updated}, or {@link Action#DELETED deleted}.
 * <p>
 * This service also consumes the "{@link Streams#requestNotifications request-notifications}" topic (partitioned by device),
 * which contains requests from a device for information about all notifications of entities being created, updated, or deleted
 * since the last request from that device. Each request clears out the accumulated notifications for that device.
 * <p>
 * This service uses Samza's storage feature to maintain a durable log of all changes for each device, and to use an
 * in-process database for quick access. If this service fails, another can be restarted and can completely recover the database
 * from the durable log.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class CoallescingService implements StreamTask, InitableTask {

    private KeyValueStore<String, Document> notificationsByDevice;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        this.notificationsByDevice = (KeyValueStore<String, Document>) context.getStore("device-notifications");
    }

    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        SystemStreamPartition stream = env.getSystemStreamPartition();
        if (Streams.isChangesByDevice(stream)) {
            recordChangeForDevice(env, collector, coordinator);
        } else if (Streams.isRequestNotifications(stream)) {
            requestNotifications(env, collector, coordinator);
        }
    }
    
    private void requestNotifications(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator)
            throws Exception {
        String device = (String)env.getKey();
        Document message = (Document)env.getMessage();
        String clientId = Message.getClient(message);

        // Construct the response message ...
        Document response = Message.createResponseFromRequest(message);
        
        Document notifications = notificationsByDevice.get(device);
        if ( notifications != null ) {
            // There are some notifications ...
            Message.setAfter(response, notifications);
        }
        Message.setStatus(response, Status.SUCCESS);
        collector.send(new OutgoingMessageEnvelope(Streams.partialResponses(), clientId, device, response));
        notificationsByDevice.delete(device);
    }
    
    private void recordChangeForDevice(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator)
            throws Exception {
        Document message = (Document) env.getMessage();
            EntityId entityId = Identifier.parseEntityId(env.getKey());
            DatabaseId dbId = entityId.databaseId();
            EntityType type = entityId.type();
            
            // Send the patch response to the output stream, partitioned by the entity type ...
            collector.send(new OutgoingMessageEnvelope(Streams.schemaLearning(dbId), type, entityId, message));
    }
}