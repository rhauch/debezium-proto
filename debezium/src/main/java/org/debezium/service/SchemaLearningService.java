/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.debezium.Configuration;
import org.debezium.Debezium;
import org.debezium.kafka.Serdes;
import org.debezium.message.Document;
import org.debezium.message.Message;
import org.debezium.message.Message.Field;
import org.debezium.message.Patch;
import org.debezium.message.Patch.Replace;
import org.debezium.message.Topic;
import org.debezium.model.EntityCollection;
import org.debezium.model.EntityId;
import org.debezium.model.EntityType;
import org.debezium.model.EntityTypeLearningModel;
import org.debezium.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service responsible for building and maintaining a <em>schema model</em> for each of the {@link EntityType}s seen in the
 * assigned partitions. The service consumes entity updates from the {@value Topic#ENTITY_UPDATES} Kafka topic (partitioned
 * by {@link EntityId}) recorded from the upstream {@link EntityStorageService}. Changes in these schema models are then
 * output to the {@value Topic#ENTITY_TYPE_UPDATES}, partitioned by {@link EntityType}.
 * <p>
 * This service uses local storage to cache and maintain data. Therefore, when multiple service instances are run in a cluster,
 * each service instance should be assigned a range of the {@value Topic#ENTITY_UPDATES} partitions. This ensures that all updates
 * from each partition are handled by the same {@link SchemaLearningService} instance. The output of this service can also be
 * partitioned and then aggregated by the {@link SchemaService}.
 * 
 * <h2>Service execution</h2>
 * <p>
 * This service has a {@link #main(String[]) main} method that when run starts up a Kafka stream processing job with a custom
 * topology and configuration. Each instance of the KafkaProcess can run a configurable number of threads, and users can start
 * multiple instances of their process job by starting multiple JVMs to run this service's {@link #main(String[])} method.
 * 
 * <h2>Configuration</h2>
 * <p>
 * Each SchemaLearningService has a logical <em>service identifier</em> that will be sent in output messages and consumed by the
 * {@link SchemaService} to identify previously-read messages from the same service instance. Therefore, each service instance
 * should have a unique service ID, and by default the service will generate a unique service ID and persist it so that it can
 * will reuse the same ID upon restart. However, it can be explicitly set via the "{@code service.id}" configuration property, and
 * doing so will overwrite any previously-generated and -persisted ID.
 * <p>
 * This service uses local storage to persist the schema learned information for each entity type. Therefore, if multiple service
 * instances are used, each service should be configured with a static range of partitions that it will consume, ensuring that all
 * changes to entities for a given entity type will always be sent to the same service instance and stored locally within that
 * service.
 * <p>
 * By default this service relies upon Kafka automatically flushing any accumulated local state and committing the offsets. This
 * is not only easier but often more efficient, as it regulates the number of operations that need to be performed while
 * processing large numbers of or even unbounded input messages. A lower auto-commit interval will result in more frequent commits
 * (and thus more network traffic), but it will reduce the amount of effort and time required to recover after a service crash or
 * failure; a larger auto-commit interval results in less-frequently committing offsets and a slightly higher rate of processing
 * input messages, but requires additional effort and time required to recover after a service crash or failure. To enable
 * auto-commit, set the "{@code auto.commit.enabled}" property to "{@code true}" and the "{@code auto.commit.interval.ms=}"
 * property to the desired interval in milliseconds. Auto-commit is enabled by default default with a default interval of 60
 * seconds. Simply set the "{@code auto.commit.enabled}" property to "{@code false}" to have the service explicitly commit after
 * completely processing every input message.
 * 
 * <h2>Exactly-once processing</h2>
 * <p>
 * The service is designed to process each entity update <em>exactly once</em>. When recovering from an unexpected crash, the
 * {@link EntityStorageService} may produce a small number of duplicate messages that it could not be sure were output before the
 * crash. However, each of these duplicate messages will have the same entity revision number. This service consumes the entity
 * updates, and uses these entity revision numbers to ensure that each distinct entity update is processed <em>exactly once</em>.
 * <p>
 * Likewise, this service computes for each entity type (or collection) a <em>revision number</em> that is monotonically
 * increasing and incremented each time the entity type's model changes. This value is included in all output messages so that
 * downstream services can likewise determine if any output messages produced by this service when recovering from a crash are
 * duplicates of messages sent before the crash.
 * 
 * @author Randall Hauch
 */
public final class SchemaLearningService extends ServiceProcessor {

    /**
     * Run this service using the Kafka Streams library. This will start the number of threads specified in the configuration and
     * determine the partitions to be read by coordinating with the other instances.
     * 
     * @param args the command-line arguments
     */
    public static void main(String[] args) {
        runner().run(args);
    }

    /**
     * Obtain a ServiceRunner that will run this service using the Kafka Streams library. When the runner is run, it will
     * start the number of threads specified in the configuration and determine the partitions to be read by coordinating with the
     * other instances.
     * 
     * @return the ServiceRunner instance; never null
     */
    public static ServiceRunner runner() {
        return ServiceRunner.use(SchemaLearningService.class, SchemaLearningService::topology)
                            .setVersion(Debezium.getVersion());
    }

    /**
     * The stream processing topology for this service. The topology consists of a single source and this service as the sole
     * processor.
     * 
     * @param config the configuration for the topology; may not be null
     * @return the topology builder; never null
     */
    public static TopologyBuilder topology(Configuration config) {
        TopologyBuilder topology = new TopologyBuilder();
        topology.addSource("input", Serdes.stringDeserializer(), Serdes.document(), Topic.ENTITY_UPDATES);
        topology.addProcessor("service", processorDef(new SchemaLearningService(config)), "input");
        topology.addSink("output", Topic.ENTITY_TYPE_UPDATES, Serdes.stringSerializer(), Serdes.document(), "service");
        return topology;
    }

    /**
     * The name of the local store used by the service to track model revisions for last known entity revisions.
     */
    public static final String REVISIONS_STORE_NAME = "schema-learning-revisions";

    /**
     * The name of the local store used by the service to track the learning model for each entity type.
     */
    public static final String MODELS_STORE_NAME = "schema-learning-models";

    /**
     * The name of this service.
     */
    public static final String SERVICE_NAME = "schema-learning";

    private static final class EntityRevisions {
        protected final long entityRevision;
        protected final long modelRevision;

        protected EntityRevisions(long entityRevision, long modelRevision) {
            this.entityRevision = entityRevision;
            this.modelRevision = modelRevision;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLearningService.class);
    private static final String METADATA_KEY = "$$$";
    private static final String SERVICE_ID_KEY = "id";
    private final ConcurrentMap<EntityType, EntityTypeLearningModel> transientModels = new ConcurrentHashMap<>();
    private KeyValueStore<String, EntityRevisions> persistedRevisions;
    private KeyValueStore<String, Document> persistedModels;
    private String serviceId;
    private long punctuateInterval;

    // Some fields used in every single call to 'process' ...
    private transient final AtomicBoolean updated = new AtomicBoolean();
    private transient EntityRevisions entityStatus = null;
    private transient EntityId entityId = null;
    private transient EntityType type = null;
    private transient EntityTypeLearningModel model = null;
    private transient Patch<EntityId> patch = null;
    private transient Document beforePatch = null;
    private transient Document afterPatch = null;

    public SchemaLearningService(Configuration config) {
        super(SERVICE_NAME, config);
        serviceId = config.getString("service.id");
        punctuateInterval = config.getLong("service.punctuate.interval.ms", 30*1000);
    }

    @Override
    protected void init() {
        this.persistedRevisions = new InMemoryKeyValueStore<>(REVISIONS_STORE_NAME, context());
        this.persistedModels = new InMemoryKeyValueStore<>(MODELS_STORE_NAME, context());
        // Load the models from the store ...
        this.persistedModels.all().forEachRemaining(entry -> {
            EntityType type = Identifier.parseEntityType(entry.key());
            EntityTypeLearningModel model = new EntityTypeLearningModel(type, entry.value());
            transientModels.put(type, model);
        });
        // Get or create a unique ID for this service group ...
        Document meta = this.persistedModels.get(METADATA_KEY);
        if (serviceId != null) {
            // Set via the config properties ...
            meta.setString(SERVICE_ID_KEY, serviceId);
            this.persistedModels.put(METADATA_KEY, meta);
        } else {
            // Read or create one ...
            if (meta == null) meta = Document.create();
            if (serviceId == null) serviceId = UUID.randomUUID().toString();
            meta.setString(SERVICE_ID_KEY, serviceId);
            this.persistedModels.put(METADATA_KEY, meta);
        }
        
        // And register ourselves for punctuation ...
        if ( punctuateInterval > 0 ) context().schedule(punctuateInterval);
    }

    @Override
    protected void process(String topic, int partition, long offset, String idStr, Document message) {
        // Almost all of the time (other than crash recovery) we need to get these every time ...
        entityStatus = persistedRevisions.get(idStr);
        entityId = Identifier.parseEntityId(idStr);
        type = entityId.type();
        model = modelFor(type);

        // Get the entity revision number and compare to what we've previously stored ...
        long revision = Message.getRevision(message);
        if (entityStatus != null && revision <= entityStatus.entityRevision) {
            // We've already seen this revision, so we're recovering from a crash.
            // See if the model revision matches our current model ...
            if (model.revision() >= entityStatus.modelRevision) {
                // Model is same or newer than used for entity, so we're good ...
                return;
            }

            // The model is older than was last seen by this entity. That means the service likely crashed after performing
            // the 'entityRevisions.put' with this entity and an updated model, but before the model could be persisted.
            // So the persisted model is at the state before this entity was processed, so we need to re-apply the changes to the
            // model and try storing the model again. We do this by just continuing as usual ...
        }

        // Parse the key and message ...
        patch = Patch.from(message);
        beforePatch = Message.getBefore(message);
        afterPatch = Message.getAfter(message);

        // Try to update the model with the patch. The supplied function is called only when the model's representation changed
        // and we have to send a schema patch.
        updated.set(false);
        model.adapt(beforePatch, patch, afterPatch, context().timestamp(), false, (before, previouslyModified, entityTypePatch) -> {
            String dbIdStr = type.databaseId().asString();
            Document request = entityTypePatch.filter(this::removeUsages).asDocument();
            Message.setClient(request, serviceId);
            Message.setRevision(request, model.revision());
            Message.setBegun(request, previouslyModified);
            Message.setEnded(request, model.lastModified());
            Message.setAfter(request, model.asDocument().clone());
            persistedRevisions.put(idStr, new EntityRevisions(revision, model.revision()));
            context().forward(dbIdStr, request);
            persistedModels.put(type.asString(), model.resetChangeStatistics().asDocument());
            updated.set(true);
        });

        if (!updated.get()) {
            // The model was not updated but the statistics were, so store the updated model and entity status ...
            persistedRevisions.put(idStr, new EntityRevisions(revision, model.revision()));
            persistedModels.put(type.asString(), model.asDocument());
        }
    }

    /**
     * Periodically output the current models with latest metrics that include changes since the last call to
     * {@link #punctuate(long)}. This enables downstream consumers to update each type's statistics even though
     * the actual schema for each type might not have changed.
     */
    @Override
    public void punctuate(long streamTime) {
        // Go through all of the models and send updated entity types with latest metrics ...
        transientModels.forEach((entityType, model) -> {
            long previouslyModified = model.lastModified();
            // And send the update ...
            model.recalculateFieldUsages().getPatch(streamTime).ifPresent(patch -> {
                // Update the model with the new field usages by applying our patch ...
                // Document before = model.asDocument().clone();
                patch.apply(model.asDocument(), (failedPath, failedOp) -> {
                    LOGGER.error("Unable to apply {} to model {}: {}", failedOp, model.type(), model);
                });
                // Create the request message without the usage patch ...
                Document request = patch.asDocument();
                request.remove(Field.OPS);
                // Message.setBefore(request, before);
                Message.setClient(request, serviceId);
                Message.setRevision(request, model.revision());
                Message.setBegun(request, previouslyModified);
                Message.setEnded(request, model.lastModified());
                Message.setAfter(request, model.asDocument());
                // Send the message with the updated metrics ...
                String dbIdStr = entityType.databaseId().asString();
                context().forward(dbIdStr, request);
            });
        });

        // Go through all of the models, reset the metrics, and store the updated models ...
        transientModels.forEach((entityType, model) -> {
            // Reset the metrics and then store the updated
            persistedModels.put(entityType.asString(), model.resetChangeStatistics().asDocument());
        });
    }

    @Override
    public void close() {
        this.persistedRevisions.close();
        this.persistedModels.close();
    }

    private boolean removeUsages(Patch.Operation operation) {
        if (operation instanceof Replace) {
            Replace replace = (Replace) operation;
            if (replace.path().endsWith("/" + EntityCollection.FieldName.USAGE)) return false;
        }
        return true;
    }

    private EntityTypeLearningModel modelFor(EntityType type) {
        return transientModels.computeIfAbsent(type, t -> new EntityTypeLearningModel(type, Document.create()));
    }
}
