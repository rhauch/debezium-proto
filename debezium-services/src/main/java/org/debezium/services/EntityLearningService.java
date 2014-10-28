/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
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
import org.debezium.core.message.Message.Field;
import org.debezium.core.message.Patch;
import org.debezium.services.learn.LearningEntityTypeModel;

/**
 * A service (or task in Samza parlance) responsible for monitoring incoming changes to entities and updating the schemas.
 * <p>
 * This service consumes the "{@link Streams#schemaLearning schema-learning}" topic, which is partitioned by entity type by the
 * {@link EntityLearningPartitioner} service. Each incoming message is one of two types:
 * <ol>
 * <li>A completed patch on a single entity type, containing the entity type representation in the "{@link Field#AFTER after}"
 * field; or</li>
 * <li>A completed patch on a single entity.</li>
 * </ol>
 * <p>
 * This service produces messages describing learned changes to the schemas on the "{@link Streams#schemaPatches schema-patches}"
 * topic.
 * <p>
 * <p>
 * This uses Samza's storage feature to maintain a cache of schema information for the entity types seen by the incoming entities.
 * Samza stores all cache updates in a durable log and uses an in-process database for quick access. If a process containing this
 * service fails, another can be restarted and can completely recover the cache from the durable log.
 * 
 * @author Randall Hauch
 *
 */
@NotThreadSafe
public class EntityLearningService implements StreamTask, InitableTask {
    
    public static final String CLIENT_ID = EntityLearningService.class.getSimpleName();
    
    private KeyValueStore<String, Document> entityTypesCache;
    private Map<EntityType, LearningEntityTypeModel> models = new HashMap<>();
    
    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        this.entityTypesCache = (KeyValueStore<String, Document>) context.getStore("schema-learning-cache");
        // Load the models from the cache ...
        entityTypesCache.all().forEachRemaining((entry) -> {
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
            Document entityRepresentation = Message.getAfter(msg);
            // Try to update the model with the patch. The function is called when the model's representation changed and
            // we have to send a schema patch. Note that we keep the (updated) model, but do not update the entityTypesCache and
            // instead wait until the schema patch comes back to us via a message with an EntityType identifier (handled below),
            // in which case we simply overwrite the model. We do this because users may have already manually changed the
            // entity type, but those have to get totally ordered via the stream. IOW, by doing it this way, changes to the
            // entity type - whether from us or from clients - will be ordered and handled correctly, and we always update
            // the model and cache based upon those properly ordered changes.
            model.adapt(patch, entityRepresentation, (entityTypePatch) -> {
                DatabaseId dbId = type.databaseId();
                String dbIdStr = dbId.asString();
                Document entityTypePatchRequest = entityTypePatch.asDocument();
                Message.addHeaders(entityTypePatchRequest, CLIENT_ID);
                collector.send(new OutgoingMessageEnvelope(Streams.schemaPatches(dbId), dbIdStr, dbIdStr, entityTypePatchRequest));
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
        LearningEntityTypeModel model = new LearningEntityTypeModel(type,representation);
        models.put(type, model);
    }
    
    private LearningEntityTypeModel modelFor(EntityType type) {
        LearningEntityTypeModel model = models.get(type);
        if (model == null) {
            // We're seeing this entity type for the first time, so it must be new ...
            Document representation = Document.create();
            model = new LearningEntityTypeModel(type,representation);
            models.put(type, model);
        }
        return model;
    }
}
