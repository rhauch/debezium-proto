/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.debezium.Configuration;
import org.debezium.Debezium;
import org.debezium.annotation.NotThreadSafe;
import org.debezium.kafka.Serdes;
import org.debezium.message.Document;
import org.debezium.message.Message;
import org.debezium.message.Message.Field;
import org.debezium.message.Patch;
import org.debezium.message.Topic;
import org.debezium.model.DatabaseId;
import org.debezium.model.EntityType;
import org.debezium.model.EntityTypeLearningModel;
import org.debezium.model.Identifier;
import org.debezium.util.Collect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service responsible for building and maintaining an <em>aggregate schema model</em> for each of the {@link EntityType}s
 * in the system. The service consumes partitioned schema updates from the {@value Topic#ENTITY_TYPE_UPDATES} Kafka topic
 * (partitioned by {@link EntityType}) recorded from the upstream {@link SchemaLearningService} instances. This
 * service also consumes the {@value Topic#SCHEMA_PATCHES} topic (partitioned by {@link DatabaseId}) and applies the
 * user-constructed patches to the appropriate schema model. Changes in these aggregate schema models are then output to the
 * {@value Topic#SCHEMA_UPDATES}, partitioned by {@link DatabaseId}. The service also outputs the changes and all read-only
 * requests or errors on the {@value Topic#PARTIAL_RESPONSES} topic.
 * <p>
 * This service uses local storage to cache and maintain data. Therefore, when multiple service instances are run in a cluster,
 * each service instance should be assigned a range of the {@value Topic#ENTITY_TYPE_UPDATES} partitions. This ensures that all
 * updates from each partition are handled by the same {@link SchemaService} instance.
 * 
 * <h2>Service execution</h2>
 * <p>
 * This service has a {@link #main(String[]) main} method that when run starts up a Kafka stream processing job with a custom
 * topology and configuration. Each instance of the KafkaProcess can run a configurable number of threads, and users can start
 * multiple instances of their process job by starting multiple JVMs to run this service's {@link #main(String[])} method.
 * 
 * <h2>Configuration</h2>
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
 * This service does a number of things to ensure that input messages are processed exactly one time and that EntityType models
 * are updated correctly, consistently, and accurately. First, the service ensures that each incoming model updates (e.g., patches
 * and metric changes) are applied <em>exactly once</em>. It does this by using the revision numbers of the upstream model
 * changes, and ignoring any changes with revision numbers it's already seen. To guarantee this is done atomically and
 * consistently, this service persists for each upstream {@link SchemaLearningService} instance the most recent revision number of
 * the upstream model <em>and</em> this service's aggregate model's revision number. The service then outputs any changes to the
 * aggregate model and persists the updated aggregate model.
 * <p>
 * If the service crashes before the incoming model revision and aggregate model revision are recorded, upon recovery the service
 * will re-process the input messages, skipping those with model revisions that have already been seen, and begin processing the
 * first input message that has not been seen and correctly and completely updating the aggregate model as expected.
 * <p>
 * If the service crashes after the revisions are recorded for that upstream service but before the updated aggregate model can be
 * persisted, then upon recovery the service will re-process the input messages, skipping those with model revisions and aggregate
 * model revisions that have already been seen. Eventually it will come across the input message that it had partially processed,
 * but since the restored aggregate model revision is less than the revision read in for this input message, it will re-process
 * this message and continue as it had prior to the crash.
 * <p>
 * Note that in this last scenario, the service may have output the update to the aggregate model before the crash. Upon recovery
 * the service would re-process the same input message, correctly update the aggregate model (which it didn't persist before the
 * crash) and its revision number, re-write the output message with the same aggregate model revision number, and finally persist
 * the aggregate model. Downstream consumers can use the aggregate model revision number to ensure <em>exactly once</em>
 * processing of this service's output messages.
 * <p>
 * Interestingly, downstream services might not need to worry about this. If this service were to crash, it would be while
 * processing one of the aggregate model updates, and upon recovery this service might re-write at most a duplicate output message
 * for this one aggregate model. Since all update messages for a single aggregate model are totally ordered, the worst case
 * scenario is that downstream consumers receive two identical messages with exactly the same aggregate model. Therefore,
 * depending upon the downstream service, it may be able to simply re-process the duplicate message with no ill effects.
 * 
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public final class SchemaService extends ServiceProcessor {

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
        return ServiceRunner.use(SchemaService.class, SchemaService::topology)
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
        topology.addSource("input-updates", Serdes.stringDeserializer(), Serdes.document(), Topic.ENTITY_TYPE_UPDATES);
        topology.addSource("input-patches", Serdes.stringDeserializer(), Serdes.document(), Topic.SCHEMA_PATCHES);
        topology.addProcessor("service", processorDef(new SchemaService(config)), "input-updates", "input-patches");
        topology.addSink("output", Topic.SCHEMA_UPDATES, Serdes.stringSerializer(), Serdes.document(), "service");
        topology.addSink("responses", Topic.PARTIAL_RESPONSES, Serdes.stringSerializer(), Serdes.document(), "service");
        return topology;
    }

    /**
     * Get the set of input, output, and store-related topics that this service uses.
     * 
     * @return the set of topics; never null, but possibly empty
     */
    public static Set<String> topics() {
        return Collect.unmodifiableSet(Topic.ENTITY_TYPE_UPDATES, Topic.SCHEMA_UPDATES, Topic.SCHEMA_PATCHES,
                                       Topic.Stores.SCHEMA_MODELS, Topic.Stores.SCHEMA_REVISIONS, Topic.Stores.SCHEMA_OVERRIDES);
    }

    /**
     * The name of the local store used by the service to track model revisions for each upstream learning service.
     */
    public static final String REVISIONS_STORE_NAME = Topic.Stores.SCHEMA_REVISIONS;

    /**
     * The name of the local store used by the service to track the aggregate model for each entity type.
     */
    public static final String MODELS_STORE_NAME = Topic.Stores.SCHEMA_MODELS;

    /**
     * The name of the local store used by the service to track the patches (for each entity type) that users submit to override
     * the learned/derived model.
     */
    public static final String MODEL_OVERRIDES_STORE_NAME = Topic.Stores.SCHEMA_OVERRIDES;

    /**
     * The name of this service.
     */
    public static final String SERVICE_NAME = "schema";

    private static final class ModelRevisions {
        protected final long partialModelRevision;
        protected final long aggregateModelRevision;

        protected ModelRevisions(long partialModelRevision, long aggregateModelRevision) {
            this.partialModelRevision = partialModelRevision;
            this.aggregateModelRevision = aggregateModelRevision;
        }
    }

    private static final Deserializer<ModelRevisions> REVISIONS_DESERIALIZER = deserializerFor(ModelRevisions::new);
    private static final Serializer<ModelRevisions> REVISIONS_SERIALIZER = serializerFor(mr -> mr.partialModelRevision,
                                                                                         mr -> mr.aggregateModelRevision);

    private static final int UPDATES = 0;
    private static final int RESPONSES = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaService.class);

    private KeyValueStore<String, ModelRevisions> persistedRevisions;
    private KeyValueStore<String, Document> persistedModels;
    private KeyValueStore<String, Document> modelOverrides;
    private final Map<EntityType, EntityTypeLearningModel> transientModels = new HashMap<>();
    private long punctuateInterval;

    // Some fields used in every single call to 'process' ...
    private transient boolean updated;
    private transient EntityTypeLearningModel aggregateModel = null;
    private transient Patch<EntityType> patch = null;

    public SchemaService(Configuration config) {
        super(SERVICE_NAME, config);
        punctuateInterval = config.getLong("service.punctuate.interval.ms", 30 * 1000);
    }

    @Override
    protected void init() {
        // Create the stores ...
        this.persistedRevisions = Stores.create(REVISIONS_STORE_NAME, context())
                                        .withStringKeys()
                                        .withValues(REVISIONS_SERIALIZER, REVISIONS_DESERIALIZER)
                                        .inMemory()
                                        .build();
        this.persistedModels = Stores.create(MODELS_STORE_NAME, context())
                                     .withStringKeys()
                                     .withValues(Serdes.document(), Serdes.document())
                                     .inMemory()
                                     .build();
        this.modelOverrides = Stores.create(MODEL_OVERRIDES_STORE_NAME, context())
                                    .withStringKeys()
                                    .withValues(Serdes.document(), Serdes.document())
                                    .inMemory()
                                    .build();

        // Load the models from the store ...
        this.persistedModels.all().forEachRemaining(entry -> {
            EntityType type = Identifier.parseEntityType(entry.key());
            EntityTypeLearningModel model = new EntityTypeLearningModel(type, entry.value());
            transientModels.put(type, model);
        });

        // And register ourselves for punctuation ...
        if (punctuateInterval > 0) context().schedule(punctuateInterval);
    }

    @Override
    protected void process(String topic, int partition, long offset, String idStr, Document request) {
        if (Topic.ENTITY_TYPE_UPDATES.equals(topic)) {
            processUpdate(partition, offset, idStr, request);
        } else if (Topic.SCHEMA_PATCHES.equals(topic)) {
            processPatch(partition, offset, idStr, request);
        }
    }

    protected void processUpdate(int partition, long offset, String idStr, Document request) {
        // DatabaseId dbId = Identifier.parseDatabaseId(idStr);
        EntityType type = Message.getEntityType(request);
        String serviceId = Message.getClient(request);

        // Find the transient aggregate model for this entity type ...
        aggregateModel = aggregateModelFor(type);

        // Get the last revision number we've seen for the upstream service that produced this message ...
        ModelRevisions serviceRevisions = persistedRevisions.get(serviceId);
        long revision = Message.getRevision(request);
        if (serviceRevisions != null && revision <= serviceRevisions.partialModelRevision) {
            // We've already seen this revision, so we're recovering from a crash.
            // See if the model revision matches our current model ...
            if (aggregateModel.revision() >= serviceRevisions.aggregateModelRevision) {
                // Model is same or newer than used for entity, so we're good ...
                return;
            }

            // The model is older than was last seen by the partial model update. That means the service likely crashed after
            // performing the 'persistentRevisions.put' with this partial model update and an updated aggregate model, but
            // before the aggregate model could be persisted. So the persisted aggregate model is at the state before this
            // partial model update was processed, so we need to re-apply the changes from this partial model update
            // to update the aggregate model and try storing the model again. We do this by just continuing as usual ...
        }

        // Parse the key and message ...
        patch = Patch.from(request);

        // Try to update the model with the patch and the metrics. The supplied function is called only when the model's
        // representation changed and we have to send an update for the aggregate schema.
        long previouslyModified = aggregateModel.lastModified();
        Document aggregateBefore = aggregateModel.asDocument(false); // already a clone, don't want metrics
        Document partialAfter = Message.getAfter(request);
        EntityTypeLearningModel partial = new EntityTypeLearningModel(type, partialAfter);
        updated = aggregateModel.merge(partial.metrics(), patch, context().timestamp(), (path, failedOp) -> {
            LOGGER.error("Unable to apply {} to aggregate model {}: {}", failedOp, type, aggregateModel);
        });

        if (updated) {
            // The model was updated so we need to send an update ...
            Document response = patch.asDocument();
            if (aggregateBefore != null && !aggregateBefore.isEmpty()) Message.setBefore(request, aggregateBefore);
            Document updatedModel = applyOverrides(type, aggregateModel.asDocument().clone());
            Message.setAfter(response, updatedModel);
            Message.setRevision(response, aggregateModel.revision());
            Message.setBegun(response, previouslyModified);
            Message.setEnded(response, aggregateModel.lastModified());
            // Store the persisted revisions first. If the service crashes before we do this, we won't have recorded anything
            // with this message. If the service crashes after we do this but before we persist the models, when we recover
            // we will re-process the input, correctly update the model, and re-send the output with the same revision number
            // as we might have sent before the crash. But either way, the model will be updated consistently and correctly.
            persistedRevisions.put(type.asString(), new ModelRevisions(revision, aggregateModel.revision()));
            context().forward(type.databaseId().asString(), response, UPDATES);
            persistedModels.put(type.asString(), aggregateModel.resetChangeStatistics().asDocument());

        } else {
            // The model was not updated but the statistics were, so store the updated aggregate model and model revisions,
            // but do not reset the change statistics!
            persistedRevisions.put(idStr, new ModelRevisions(revision, aggregateModel.revision()));
            persistedModels.put(type.asString(), aggregateModel.asDocument());
        }
    }
    
    protected Document applyOverrides( EntityType entityType, Document model ) {
        Document existingOverride = modelOverrides.get(entityType.asString());
        if ( existingOverride != null ) {
            Patch<EntityType> overridePatch = Patch.forEntityType(existingOverride);
            overridePatch.apply(model,(failedPath, failedOp) -> {
                LOGGER.error("Unable to apply override patch {} to model {}: {}", failedOp, entityType, model);
            });
        }
        return model;
    }

    protected void processPatch(int partition, long offset, String idStr, Document request) {
        Patch<EntityType> patch = Patch.forEntityType(request);
        EntityType type = patch.target();
        String typeStr = type.asString();
        aggregateModel = aggregateModelFor(type);
        
        // Construct the response message ...
        Document response = Message.createResponseFromRequest(request);
        Message.setOperations(response, request);
        Message.setBefore(response, aggregateModel.asDocument().clone());
        Message.setBegun(response, aggregateModel.lastModified());

        // Load the existing patch for this entity type ...
        Document override = request;
        Patch<EntityType> overridePatch = patch;
        Document existingOverride = modelOverrides.get(typeStr);
        if ( existingOverride != null ) {
            overridePatch = Patch.forEntityType(override);
            override = overridePatch.append(patch).asDocument();
        }

        // Compute a new representation of the model, including all latest overrides ...
        Document model = aggregateModel.asDocument().clone();
        overridePatch.apply(model,(failedPath, failedOp) -> {
            LOGGER.error("Unable to apply override patch {} to model {}: {}", failedOp, type, model);
        });
        
        // Finish the response message ...
        Message.setAfter(response, model);
        Message.setRevision(response, aggregateModel.revision());
        Message.setEnded(response, aggregateModel.lastModified());

        // Send the update to the partial response and updates topics ...
        context().forward(Message.getClient(response), response, RESPONSES);    // TODO: use type as key, partition on client ID
        context().forward(type.databaseId().asString(), response, UPDATES);

        // Store the new override patch ...
        modelOverrides.put(typeStr, override);
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
            if (model.metrics().hasChanges()) {
                long previouslyModified = model.lastModified();
                // Our aggregate model has already advanced, so we don't have anything that represents the 'before' state.
                // However, since the patch is generated based upon the incremented values, the patch will be correct.
                Document before = model.recreateDocumentBeforeRecentUsageChanges();
                Patch<EntityType> patch = model.recalculateFieldUsages().getPatch(streamTime).orElse(null);
                Document response = null;
                if (patch != null) {
                    // Update the model with the new field usages by applying our patch ...
                    patch.apply(model.asDocument(), (failedPath, failedOp) -> {
                        LOGGER.error("Unable to apply {} to model {}: {}", failedOp, model.type(), model);
                    });
                    response = patch.asDocument();
                    before = applyOverrides(entityType, before);
                    Message.setBefore(response, before);
                } else {
                    // There's actually nothing to update and no patch, but output the results anyway (without before) ...
                    response = Document.create();
                    response.setString(Field.DATABASE_ID, entityType.databaseId().asString());
                    response.setString(Field.COLLECTION, entityType.entityTypeName());
                }
                // Create the request message ...
                Document after = applyOverrides(entityType, model.asDocument(false));
                Message.setAfter(response, after);
                Message.setRevision(response, model.revision());
                Message.setBegun(response, previouslyModified);
                Message.setEnded(response, model.lastModified());
                // Send the message with the updated metrics ...
                String dbIdStr = entityType.databaseId().asString();
                context().forward(dbIdStr, response, UPDATES);
            }
        });

        // Go through all of the models, reset the metrics, and store the updated models ...
        transientModels.forEach((entityType, model) -> {
            // Reset the metrics and then store the updated
            persistedModels.put(entityType.asString(), model.resetChangeStatistics().asDocument());
        });
    }

    @Override
    public void close() {
        logger().trace("Shutting down service '{}'", getName());
        this.persistedModels.close();
        logger().debug("Service '{}' shutdown", getName());
    }

    private EntityTypeLearningModel aggregateModelFor(EntityType type) {
        return transientModels.computeIfAbsent(type, t -> new EntityTypeLearningModel(type, Document.create()));
    }
}
