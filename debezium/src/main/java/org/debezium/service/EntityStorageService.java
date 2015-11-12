/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.Set;

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
import org.debezium.message.Message.Status;
import org.debezium.message.Patch;
import org.debezium.message.Patch.Operation;
import org.debezium.message.Path;
import org.debezium.message.Topic;
import org.debezium.model.EntityId;
import org.debezium.model.EntityType;
import org.debezium.util.Collect;

/**
 * A service responsible for reading the {@value Topic#ENTITY_PATCHES} Kafka topic containing {@link Patch} messages.
 * The service consumes these patches and for each looks up the stored entities in a local key-value store, applies the patch,
 * and outputs the result of the changes to the {@value Topic#ENTITY_UPDATES} topic. The service also outputs the changes
 * and all read-only requests or errors on the {@value Topic#PARTIAL_RESPONSES} topic.
 * 
 * <h2>Service execution</h2>
 * <p>
 * This service has a {@link #main(String[]) main} method that when run starts up a Kafka stream processing job with a custom
 * topology and configuration. Each instance of the KafkaProcess can run a configurable number of threads, and users can start
 * multiple instances of their process job by starting multiple JVMs to run this service's {@link #main(String[])} method.
 * 
 * <h2>Configuration</h2>
 * <p>
 * The {@link Patch}es in the {@value Topic#ENTITY_PATCHES} input topic are partitioned by {@link EntityType collection ID}, so
 * all patches for entities within a single collection will all be sent to the same partitions. Each service process configuration
 * should statically assign a range of partitions that it will consume, and the service will store the entities for those
 * partitions (i.e., collections) in its local key-value store. This approach means that all activities on a single entity will
 * always be handled by the same service process.
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
 * The service takes a number of steps to ensure that all input messages are processed by the service <em>exactly once</em>. Even
 * if the service is stopped or crashes, it merely needs to be restarted and the service will continue processing each and every
 * message exactly one time.
 * <p>
 * Recall that in Kafka consumers are responsible for tracking how far in the input streams they have read. This is done by
 * monitoring the input partition offsets for each consumed message and after processing is completed writing these offsets to a
 * separate Kafka's offset commit logs. Under normal operating circumstances, the service naturally achieves <em>exactly once</em>
 * processing. When the service is shut down gracefully, it will stop consuming messages, completely process the messages it has
 * read, and update the commit log; when it is then restarted, it will read its commit log to determine where in the input stream
 * it needs to start reading, with the result being that again all messages are processed <em>exactly once</em>.
 * <p>
 * But what happens when the service crashes after reading and partially processing an input message but before the offset are
 * committed to the log? In this case, when the service is restarted, it will start reading from the latest offset recorded in the
 * commit log, and that might mean that some messages might be consumed a second time. This service is designed so that even if it
 * consumes some messages a second time, the service will <em>process</em> them <em>exactly once</em>.
 * <p>
 * To achieve this, the service uses vector clocks to track the offsets of messages (patches) from each of the input stream
 * partitions. (Since the offsets in a partition are monotonically increasing, they can be viewed as a logical timestamp.) So
 * given {@code N} input stream partitions, each vector clock stores {@code N} offsets (or "timestamps") signifying the messages
 * that have been consumed and applied. However, the service does not need to use a single vector clock for the whole service,
 * since that's effectively what the commit log provides. Instead, the service gives each entity its own vector clock so that the
 * service can know the offsets of the last message from each partition that were applied to that particular entity. And because
 * the vector clock is stored <em>inside</em> each entity (in a metadata area), the updates to an entity that result from an input
 * patch and the updates to the vector clocks are persisted atomically.
 * <p>
 * When processing an input message, the service compares the message's offset in the stream partition to the offset recorded for
 * that partition in the entity's vector clock. If the message's offset is less than or equal to the offset in the vector clock,
 * then the service knows this message has already been processed and it can simply ignore it. If the message's offset is greater
 * than the offset in the vector clock, the message is new and the service applies the patch in the message, updates the entity
 * and the vector clock, sends the output, and commits the offset log.
 * <p>
 * The vector clocks let the service implement <em>exactly once</em> processing of all input messages, even when some input
 * messages are read more than once. Of course, that doesn't happen under normal operating circumstances (including graceful
 * shutdown), since the commit log remains an accurate and consistent record of which messages have been processed and input
 * messages are only consumed once. However, when/if the service crashes after processing some input messages but before the
 * offsets of those messages could be written to the commit log, the service will re-read some of those messages and the vector
 * clocks ensure they are still only processed <em>exactly once</em>.
 * <p>
 * <p>
 * Now, if the service crashes and recovers, it is possible for this service to produce output messages that were duplicates of
 * messages sent just before the crash. This is to be expected with a truly distributed event-based system that guarantees
 * <em>at least once</em> processing. The EntityStorageService computes a monotonically increasing <em>revision number</em> for
 * each entity as it applies each patch, and this revision number will be identical for duplicate output messages.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public final class EntityStorageService extends ServiceProcessor {

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
        return ServiceRunner.use(EntityStorageService.class, EntityStorageService::topology)
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
        topology.addSource("input", Serdes.stringDeserializer(), Serdes.document(), Topic.ENTITY_PATCHES);
        topology.addProcessor("service", processorDef(new EntityStorageService(config)), "input");
        topology.addSink("updates", Topic.ENTITY_UPDATES, Serdes.stringSerializer(), Serdes.document(), "service");
        topology.addSink("responses", Topic.PARTIAL_RESPONSES, Serdes.stringSerializer(), Serdes.document(), "service");
        return topology;
    }

    /**
     * Get the set of input, output, and store-related topics that this service uses.
     * 
     * @return the set of topics; never null, but possibly empty
     */
    public static Set<String> topics() {
        return Collect.unmodifiableSet(Topic.ENTITY_PATCHES, Topic.ENTITY_UPDATES, Topic.PARTIAL_RESPONSES,
                                       Topic.Stores.ENTITY_STORAGE);
    }

    /**
     * The name of the single store used by the service.
     */
    public static final String ENTITY_STORE_NAME = Topic.Stores.ENTITY_STORAGE;

    /**
     * The name of this service.
     */
    public static final String SERVICE_NAME = "entity-storage";

    private static final int UPDATES = 0;
    private static final int RESPONSES = 1;

    private KeyValueStore<String, Document> store;

    public EntityStorageService(Configuration config) {
        super(SERVICE_NAME, config);
    }

    @Override
    protected void init() {
        this.store = Stores.create(ENTITY_STORE_NAME, context())
                           .withStringKeys()
                           .withValues(Serdes.document(), Serdes.document())
                           .inMemory()
                           .build();
    }

    @Override
    protected void process(String topic, int partition, long offset, final String entityId, final Document request) {
        // Construct the patch from the request ...
        Patch<EntityId> patch = Patch.from(request);
        EntityId id = patch.target();

        // Construct the response message ...
        Document response = Message.createResponseFromRequest(request);

        // Look up the entity in the store ...
        Document storedEntity = store.get(entityId);

        if (patch.isReadRequest()) {
            // We're just reading the entity, and because reads don't can be re-done after a crash we don't need to update
            // the vector clock ...
            performRead(entityId, id, storedEntity, response);
            return;
        }

        // Now, the input request contains a patch request to modify the entity, so add the operations to the response ...
        Message.setOperations(response, request);

        // Make sure there is an entity document ...
        Document entity = null;
        long revision = 0;
        if (storedEntity == null) {
            storedEntity = Document.create();
            storedEntity.setNumber(Field.REVISION, revision);
            storedEntity.setArray(Field.ENTITY_VCLOCK);
            entity = storedEntity.setDocument(Field.ENTITY_DOC);
        } else {
            // Read the revision from the wrapper and update the entity ...
            revision = storedEntity.getLong(Field.REVISION, 0);
            entity = storedEntity.getDocument(Field.ENTITY_DOC);
            entity.setNumber(Field.REVISION, revision);
            // and if there is, capture a copy of it as the 'before' ...
            Message.setBefore(response, entity.clone());
        }

        // We use a vector clock to record the maximum offset from each partition that we've already seen. First, update this
        // vector clock using the offset and partition for the current message, and use that to determine if we've already
        // seen this patch (in which case we can simply ignore it).
        boolean patchIsNew = updateVectorClock(storedEntity.getArray(Field.ENTITY_VCLOCK), partition, offset);

        if (!patchIsNew) {
            // We've already seen this patch, tried to apply it, output the results, and stored the updated entity,
            // so there's nothing to do ...
        }

        // Otherwise, we've not yet seen this patch, so make sure the entity's '$rev' field matches that of the stored entity ...
        entity.setNumber(Field.REVISION, revision);

        // Apply the patch, which may populate an empty entity ...
        if (patch.apply(entity, (failedPath, failedOp) -> record(failedPath, failedOp, response))) {
            // The patch was successful, so update revision number on the entity and wrapper ...
            ++revision;
            entity.setNumber(Field.REVISION, revision);
            storedEntity.setNumber(Field.REVISION, revision);

            // Update the response with the resulting entity ...
            Message.setAfter(response, entity);
            Message.setEnded(response, System.currentTimeMillis());
            Message.setStatus(response, Status.SUCCESS);
            Message.setRevision(response, revision);

            // Output the updated entity to the entity-updates stream ...
            sendResult(response, id);
        } else {
            // Otherwise the patch failed, so update the response message ...
            Message.setStatus(response, Status.PATCH_FAILED);
            Message.setEnded(response, System.currentTimeMillis());
            Message.setRevision(response, revision);
        }

        // No matter whether the patch failed or succeeded, send the response to the partial-response output stream ...
        sendResponse(response, entityId);

        // And finally store the changed entity in its wrapper.
        //
        // NOTE: We always do this last, since we want to persist the vector clock changes *after* we send the output
        // messages. That way, if the service crashes after having already sent the output messages but before we persist
        // the updated entity, the client will get the response(s). Then when the service recovers, it might re-process some
        // input message(s) and re-send the output messages, but the output messages will be valid.
        store.put(entityId, storedEntity);
    }

    protected void performRead(String idStr, EntityId id, Document storedEntity, Document response) {
        // We're just reading the entity ...
        if (storedEntity == null) {
            // The entity does not exist ...
            Message.setStatus(response, Status.DOES_NOT_EXIST);
            Message.addFailureReason(response, "Entity '" + id + "' does not exist.");
            Message.setEnded(response, System.currentTimeMillis());
            sendResponse(response, idStr);
        } else {
            // We're reading an existing entity ...
            assert storedEntity != null;
            Document entity = storedEntity.getDocument(Field.ENTITY_DOC);
            Message.setAfter(response, entity);
            Message.setEnded(response, System.currentTimeMillis());
            Message.setStatus(response, Status.SUCCESS);
            // Set the revision ...
            long revision = storedEntity.getLong(Field.REVISION, 0);
            entity.setNumber(Field.REVISION, revision);

            sendResponse(response, idStr);
        }
    }

    @Override
    public void close() {
        logger().trace("Shutting down service '{}'", getName());
        this.store.close();
        logger().debug("Service '{}' shutdown", getName());
    }

    private void sendResponse(Document response, String idStr) {
        String clientId = Message.getClient(response);
        // TODO: Partition on the client ID, but now have to send it via the key ...
        context().forward(clientId, response, RESPONSES);
    }

    private void sendResult(Document response, EntityId id) {
        // TODO: Partition on the collection, but now have to send it via the key ...
        context().forward(id.asString(), response, UPDATES);
    }

    private void record(Path failedPath, Operation failedOperation, Document response) {
        Message.addFailureReason(response, failedOperation.failureDescription());
    }

}
