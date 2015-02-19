/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.System.EntityTypes;
import org.debezium.core.component.ZoneId;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Action;
import org.debezium.core.message.Message.Field;

/**
 * A service (or task in Samza parlance) responsible for summarizing the changes to entities and writing them to another topic
 * partitioned by zone.
 * <p>
 * This service consumes the "{@link Streams#entityUpdates entity-updates}" topic (partitioned by {@link EntityId entity ID"}),
 * but processes incoming messages in different ways:
 * <ol>
 * <li>Updates to zone subscriptions are represented as updated {@link EntityTypes#ZONE_SUBSCRIPTION $zoneSubscription} entities,
 * and these are simply rewritten to the output topic partitioned by the ZoneId that the subscription watches.</li>
 * <li>All updates (including zone subscriptions) are evaluated to determine whether the entity was created, updated, or deleted,
 * and this minimal information is written to the output topic partitioned by the entity's ZoneId.</li>
 * </ol>
 * This service always writes to the "{@link Streams#zoneChanges zone-changes}" topic, it keeps not state, and does not store
 * or cache any information.
 * <p>
 * 
 * @author Randall Hauch
 */
public final class ChangesInZoneService implements StreamTask {

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String entId = (String) envelope.getKey();
        Document message = (Document) envelope.getMessage();
        EntityId entityId = Identifier.parseEntityId(entId);
        String zone = entityId.zoneId().asString();
        
        if (EntityTypes.ZONE_SUBSCRIPTION.equals(entityId.type().entityTypeName())) {
            // This is an update to the "$zoneSubscription" collection, so first forward the whole message to the subscribed zone ...
            Document subscription = Message.getAfterOrBefore(message);
            ZoneId subscribedZone = Message.getZoneId(subscription,entityId.databaseId());
            zone = subscribedZone.asString();
            collector.send(new OutgoingMessageEnvelope(Streams.zoneChanges(entityId.databaseId()), zone, entId, message));
        }
        
        // For all updates (including subscription changes), figure out whether the entity was created, updated, or deleted ...
        Action action = Message.determineAction(message);
        Document summary = Document.create();
        Message.addId(summary, entityId);
        Message.copyCompletionTime(message, summary);
        summary.setString(Field.ACTION, action.description());
        collector.send(new OutgoingMessageEnvelope(Streams.zoneChanges(entityId.databaseId()), zone, entId, summary));
    }
}
