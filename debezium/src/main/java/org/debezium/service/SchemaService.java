/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.processor.KafkaSource;
import org.apache.kafka.clients.processor.PTopology;
import org.apache.kafka.clients.processor.ProcessorProperties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.stream.state.InMemoryKeyValueStore;
import org.apache.kafka.stream.state.KeyValueStore;
import org.debezium.Debezium;
import org.debezium.annotation.NotThreadSafe;
import org.debezium.message.Document;
import org.debezium.message.DocumentSerdes;
import org.debezium.message.Message;
import org.debezium.message.Message.Field;
import org.debezium.message.Patch;
import org.debezium.message.Topic;
import org.debezium.model.DatabaseId;
import org.debezium.model.EntityType;
import org.debezium.model.EntityTypeLearningModel;
import org.debezium.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service responsible for building and maintaining an <em>aggregate schema model</em> for each of the {@link EntityType}s
 * in the system. The service consumes partitioned schema updates from the {@value Topic#ENTITY_TYPE_UPDATES} Kafka topic
 * (partitioned by {@link EntityType}) recorded from the upstream {@link SchemaLearningService} instances. This
 * service also consumes the {@value Topic#SCHEMA_PATCHES} topic (partitioned by {@link DatabaseId}) and applies the
 * user-constructed patches to the appropriate schema model. Changes in these aggregate schema models are then output to the
 * {@value Topic#SCHEMA_UPDATES}, partitioned by {@link DatabaseId}.
 * <p>
 * This service uses local storage to cache and maintain data. Therefore, when multiple service instances are run in a cluster,
 * each service instance should be assigned a range of the {@value Topic#ENTITY_TYPE_UPDATES} partitions. This ensures that all
 * updates from each partition are handled by the same {@link SchemaService} instance.
 * 
 * <h1>Service execution</h1>
 * <p>
 * This service has a {@link #main(String[]) main} method that when run starts up a Kafka stream processing job with a custom
 * topology and configuration. Each instance of the KafkaProcess can run a configurable number of threads, and users can start
 * multiple instances of their process job by starting multiple JVMs to run this service's {@link #main(String[])} method.
 * 
 * <h1>Configuration</h1>
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
 * <h1>Exactly-once processing</h1>
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
        return ServiceRunner.use(SchemaService.class, SchemaLearningTopology.class)
                            .setVersion(Debezium.getVersion());
    }

    /**
     * The stream processing topology for this service. The topology consists of a single source and this service as the sole
     * processor.
     */
    public static final class SchemaLearningTopology extends PTopology {

        @SuppressWarnings("unchecked")
        @Override
        public void build() {
            KafkaSource<String, Document> source = addSource(new StringDeserializer(), new DocumentSerdes(), Topic.ENTITY_TYPE_UPDATES);
            addProcessor(new SchemaService(properties), source);
        }
    }

    private static final class ModelRevisions {
        protected final long partialModelRevision;
        protected final long aggregateModelRevision;

        protected ModelRevisions(long partialModelRevision, long aggregateModelRevision) {
            this.partialModelRevision = partialModelRevision;
            this.aggregateModelRevision = aggregateModelRevision;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaService.class);

    private KeyValueStore<String, ModelRevisions> persistedRevisions;
    private KeyValueStore<String, Document> persistedModels;
    private final Map<EntityType, EntityTypeLearningModel> transientModels = new HashMap<>();

    // Some fields used in every single call to 'process' ...
    private transient boolean updated;
    private transient EntityTypeLearningModel aggregateModel = null;
    private transient Patch<EntityType> patch = null;

    public SchemaService(ProcessorProperties config) {
        super("schema", config);
    }

    @Override
    protected void init() {
        this.persistedRevisions = new InMemoryKeyValueStore<>("schema-revisions", context());
        this.persistedModels = new InMemoryKeyValueStore<>("schema-models", context());
        // Load the models from the store ...
        this.persistedModels.all().forEachRemaining(entry -> {
            EntityType type = Identifier.parseEntityType(entry.key());
            EntityTypeLearningModel model = new EntityTypeLearningModel(type, entry.value());
            transientModels.put(type, model);
        });
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
            Message.setAfter(response, aggregateModel.asDocument().clone());
            Message.setRevision(response, aggregateModel.revision());
            Message.setBegun(response, previouslyModified);
            Message.setEnded(response, aggregateModel.lastModified());
            // Store the persisted revisions first. If the service crashes before we do this, we won't have recorded anything
            // with this message. If the service crashes after we do this but before we persist the models, when we recover
            // we will re-process the input, correctly update the model, and re-send the output with the same revision number
            // as we might have sent before the crash. But either way, the model will be updated consistently and correctly.
            persistedRevisions.put(type.asString(), new ModelRevisions(revision, aggregateModel.revision()));
            context().send(Topic.SCHEMA_UPDATES, type.databaseId().asString(), request);
            persistedModels.put(type.asString(), aggregateModel.resetChangeStatistics().asDocument());

        } else {
            // The model was not updated but the statistics were, so store the updated aggregate model and model revisions,
            // but do not reset the change statistics!
            persistedRevisions.put(idStr, new ModelRevisions(revision, aggregateModel.revision()));
            persistedModels.put(type.asString(), aggregateModel.asDocument());
        }
    }

    protected void processPatch(int partition, long offset, String idStr, Document request) {

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
                Document request = null;
                if (patch != null) {
                    // Update the model with the new field usages by applying our patch ...
                    patch.apply(model.asDocument(), (failedPath, failedOp) -> {
                        LOGGER.error("Unable to apply {} to model {}: {}", failedOp, model.type(), model);
                    });
                    request = patch.asDocument();
                    Message.setBefore(request, before);
                } else {
                    // There's actually nothing to update and no patch, but output the results anyway (without before) ...
                    request = Document.create();
                    request.setString(Field.DATABASE_ID, entityType.databaseId().asString());
                    request.setString(Field.COLLECTION, entityType.entityTypeName());
                }
                // Create the request message ...
                Message.setAfter(request, model.asDocument(false));
                Message.setRevision(request, model.revision());
                Message.setBegun(request, previouslyModified);
                Message.setEnded(request, model.lastModified());
                // Send the message with the updated metrics ...
                String dbIdStr = entityType.databaseId().asString();
                context().send(Topic.SCHEMA_UPDATES, dbIdStr, request);
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
        this.persistedModels.close();
    }

    private EntityTypeLearningModel aggregateModelFor(EntityType type) {
        return transientModels.computeIfAbsent(type, t -> new EntityTypeLearningModel(type, Document.create()));
    }
}
