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
import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.System.EntityTypes;
import org.debezium.core.component.ZoneId;
import org.debezium.core.component.ZoneSubscription.Interest;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Action;
import org.debezium.core.message.Message.Field;

/**
 * A service (or task in Samza parlance) responsible for using zone subscriptions to identify to which devices entity changes
 * should be forwarded.
 * <p>
 * This service consumes the "{@link Streams#zoneChanges zone-changes}" topic (partitioned by zone ID), but it handles two kinds
 * of incoming messages:
 * <ol>
 * <li>Updates for entities in the {@link EntityTypes#ZONE_SUBSCRIPTION $zoneSubscription} internal collection are evaluated to
 * identify changes to subscriptions. These input messages are consumed to update the internal persisted mapping between zones and
 * devices.</li>
 * <li>A simple summary of the change to an entity, which contains the entity's ID and an {@link Field#ACTION action} that
 * specifies whether the entity was {@link Action#CREATED created}, {@link Action#UPDATED updated}, or {@link Action#DELETED
 * deleted}. Each of these changes is reproduced for each device subscribed to the entity's zone onto the "
 * {@link Streams#changesByDevice changes-by-device}" topic (partitioned by device).</li>
 * </ol>
 * <p>
 * This service uses Samza's storage feature to maintain durable logs and quick-access caches of all devices subscribed to each
 * zone. If this service fails, another can be restarted and can completely recover the cached content from the durable log.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class ZoneWatchService implements StreamTask, InitableTask {

    private KeyValueStore<String, Document> devicesByZoneId;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        this.devicesByZoneId = (KeyValueStore<String, Document>) context.getStore("zone-devices");
    }

    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        try {
            EntityId entityId = Identifier.parseEntityId(env.getKey());
            DatabaseId dbId = entityId.databaseId();
            EntityType type = entityId.type();
            if (EntityTypes.ZONE_SUBSCRIPTION.equals(type.entityTypeName())) {
                // This is a change to a subscription ...
                Document message = (Document) env.getMessage();
                Document subscriptionEntity = Message.getAfter(message);
                if (subscriptionEntity == null) {
                    Document removedSubscription = Message.getBefore(message);
                    removeSubscription(dbId, entityId.id(), removedSubscription);
                } else {
                    updateSubscription(dbId, entityId.id(), subscriptionEntity);
                }
            } else {
                // This is a regular change to an entity, so send it to all of the devices ...
                ZoneId zoneId = entityId.zoneId();
                Document zoneDevices = devicesByZoneId.get(zoneId.asString());
                if (zoneDevices != null) {
                    // There is at least one subscribed device ...
                    Document message = (Document) env.getMessage();
                    Action action = Message.determineAction(message);
                    SystemStream output = Streams.changesByDevice(dbId);
                    // Duplicate the message for each device that is interested in the action ...
                    zoneDevices.stream().forEach(field -> {
                        String device = field.getName().toString();
                        int interestMask = field.getValue().asInteger().intValue();
                        if (action.satisfies(interestMask)) {
                            collector.send(new OutgoingMessageEnvelope(output, device, device, message));
                        }
                    });
                }
            }

        } catch (RuntimeException t) {
            t.printStackTrace();
            throw t;
        }
    }

    private void removeSubscription(DatabaseId dbId, String subscriptionId, Document oldSubscription) {
        if (oldSubscription != null) {
            // Get the zone and device info from the subscription ...
            String device = Message.getDevice(oldSubscription);
            ZoneId zoneId = Message.getZoneId(oldSubscription, dbId);
            assert device != null;
            assert zoneId != null;
            String zoneIdStr = zoneId.asString();
            Document zoneDevices = devicesByZoneId.get(zoneIdStr);
            if (zoneDevices != null && zoneDevices.remove(device) != null) {
                devicesByZoneId.put(zoneIdStr, zoneDevices);
            }
        }
    }

    private void updateSubscription(DatabaseId dbId, String subscriptionId, Document updatedSubscription) {
        // Get the zone and device info from the subscription ...
        String device = Message.getDevice(updatedSubscription);
        ZoneId zoneId = Message.getZoneId(updatedSubscription, dbId);
        assert device != null;
        assert zoneId != null;
        String zoneIdStr = zoneId.asString();
        boolean onCreate = updatedSubscription.getBoolean("creates", false);
        boolean onUpdate = updatedSubscription.getBoolean("updates", false);
        boolean onDelete = updatedSubscription.getBoolean("deletes", false);
        int interestMask = Interest.mask(onCreate, onUpdate, onDelete);
        Document zoneDevices = devicesByZoneId.get(zoneIdStr);
        if (zoneDevices == null) {
            zoneDevices = Document.create();
        }
        zoneDevices.setNumber(device, interestMask);
        devicesByZoneId.put(zoneIdStr, zoneDevices);
    }
}
