/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.HashMap;
import java.util.Map;

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
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Path;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Field;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Topic;

/**
 * A service (or task in Samza parlance) responsible for monitoring incoming changes to entities and updating the schemas.
 * <p>
 * This service consumes the "{@value Topic#SCHEMA_LEARNING}" topic, which is partitioned by entity type. Each incoming message is
 * one of two types:
 * <ol>
 * <li>A completed patch on a single <em>entity type</em>, containing the entity type representation in the {@link Field#AFTER
 * after} field; or</li>
 * <li>A completed patch on a single <em>entity</em>, containing the {@link Field#BEFORE before} and {@link Field#AFTER after}
 * representations of the entity.</li>
 * </ol>
 * <p>
 * This service produces messages describing learned changes to the schemas on the "{@value Topic#SCHEMA_PATCHES}" topic.
 * <p>
 * <p>
 * This uses Samza's storage feature to maintain a cache of schema information for the entity types seen by the incoming entities.
 * Samza stores all cache updates in a durable log and uses an in-process database for quick access. If a process containing this
 * service fails, another can be restarted and can completely recover the cache from the durable log.
 * <p>
 * Currently, this service uses an algorithm to update the entity types that is single-pass. For example, if a patch on an entity
 * suggests a new field, the algorithm does not check previously-seen patches or entities to determine whether the new field is
 * mandatory (instead it will assume the field is optional). Likewise, if a patch on an entity removes a field, the algorithm does
 * not check previously-seen patches or entities to determine whether that field should be removed (instead it is marked as
 * optional). Thus, only fields added in the first entity in the collection will be considered non-optional.
 * 
 * @author Randall Hauch
 *
 */
@NotThreadSafe
public class SchemaLearningService implements StreamTask, InitableTask {

    public static final String CLIENT_ID = SchemaLearningService.class.getSimpleName();

    private static final SystemStream SCHEMA_PATCHES = new SystemStream("debezium", Topic.SCHEMA_PATCHES);

    private KeyValueStore<String, Document> entityTypesCache;
    private Map<EntityType, LearningEntityTypeModel> models = new HashMap<>();
    private FieldCountTracker fieldTracker;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        this.entityTypesCache = (KeyValueStore<String, Document>) context.getStore("schema-learning-cache");
        this.fieldTracker = new FieldCountTracker();
        // Load the models from the cache ...
        entityTypesCache.all().forEachRemaining(entry -> {
            EntityType type = Identifier.parseEntityType(entry.getKey());
            updateModel(type, entry.getValue());
        });
    }

    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String idStr = (String) env.getKey();
        Identifier id = Identifier.parseIdentifier(idStr);
        Document msg = (Document) env.getMessage();

        // Most messages will be entity changes, so look for that first ...
        if (id instanceof EntityId) {
            EntityId entityId = (EntityId) id;
            EntityType type = entityId.type();
            Patch<EntityId> patch = Patch.from(msg);
            LearningEntityTypeModel model = modelFor(type);
            Document beforePatch = Message.getBefore(msg);
            Document afterPatch = Message.getAfter(msg);
            // Try to update the model with the patch. The function is called when the model's representation changed and
            // we have to send a schema patch. Note that we keep the (updated) model, but do not update the entityTypesCache and
            // instead wait until the schema patch comes back to us via a message with an EntityType identifier (handled below),
            // in which case we simply overwrite the model. We do this because users may have already manually changed the
            // entity type, but those have to get totally ordered via the stream. IOW, by doing it this way, changes to the
            // entity type - whether from us or from clients - will be ordered and handled correctly, and we always update
            // the model and cache based upon those properly ordered changes.
            model.adapt(beforePatch, patch, afterPatch, entityTypePatch -> {
                DatabaseId dbId = type.databaseId();
                String dbIdStr = dbId.asString();
                Document entityTypePatchRequest = entityTypePatch.asDocument();
                Message.addHeaders(entityTypePatchRequest, CLIENT_ID);
                collector.send(new OutgoingMessageEnvelope(SCHEMA_PATCHES, dbIdStr, dbIdStr, entityTypePatchRequest));
            });
        } else if (id instanceof EntityType) {
            // This is a message describing that the entity type has been changed (by someone other than us),
            // so update our cached representation ...
            EntityType type = (EntityType) id;
            Document representation = Message.getAfter(msg);
            entityTypesCache.put(type.asString(), representation);
            updateModel(type, representation);
        }
    }

    private void updateModel(EntityType type, Document representation) {
        LearningEntityTypeModel model = new LearningEntityTypeModel(type, representation, fieldTracker);
        models.put(type, model);
    }

    private LearningEntityTypeModel modelFor(EntityType type) {
        LearningEntityTypeModel model = models.get(type);
        if (model == null) {
            // We're seeing this entity type for the first time, so it must be new ...
            Document representation = Document.create();
            model = new LearningEntityTypeModel(type, representation, fieldTracker);
            models.put(type, model);
        }
        return model;
    }

    private static final class FieldCountTracker implements LearningEntityTypeModel.FieldUsage {

        @Override
        public void markNewEntity(EntityType type) {
        }

        @Override
        public boolean markAdded(EntityType type, Path fieldPath) {
            return false;
        }

        @Override
        public boolean markRemoved(EntityType type, Path fieldPath) {
            return false;
        }

    }
}
