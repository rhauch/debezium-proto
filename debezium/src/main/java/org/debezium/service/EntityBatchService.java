/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.debezium.Configuration;
import org.debezium.Debezium;
import org.debezium.annotation.NotThreadSafe;
import org.debezium.kafka.Serdes;
import org.debezium.message.Batch;
import org.debezium.message.Document;
import org.debezium.message.Message;
import org.debezium.message.Message.Field;
import org.debezium.message.Message.Status;
import org.debezium.message.Patch;
import org.debezium.message.Topic;
import org.debezium.model.DatabaseId;
import org.debezium.model.EntityId;

/**
 * A service responsible for reading the {@value Topic#ENTITY_BATCHES} Kafka topic containing {@link Patch} and {@link Batch}
 * messages. The service consumes these messages and forwards patch messages directly to {@link Topic#ENTITY_PATCHES} while
 * for batches extracts all individual patches within the batch and sends them (with updated {@link Field#PARTS part fields})
 * to {@link Topic#ENTITY_PATCHES}.
 * 
 * <h2>Service execution</h2>
 * <p>
 * This service has a {@link #main(String[]) main} method that when run starts up a Kafka stream processing job with a custom
 * topology and configuration. Each instance of the KafkaProcess can run a configurable number of threads, and users can start
 * multiple instances of their process job by starting multiple JVMs to run this service's {@link #main(String[])} method.
 * 
 * <h2>Configuration</h2>
 * <p>
 * The {@value Topic#ENTITY_PATCHES} input topic is partitioned by the request ID of each patch and batch message, ensuring that
 * all batch and patch requests from a single client will all be sent to the same partition and processed by the service instance,
 * and that all requests from a single client are totally ordered. Each service process configuration can statically assign a
 * range of partitions that it will consume.
 * <p>
 * This service relies upon Kafka committing the offsets of all input messages that have been consumed so that, upon restart, it
 * will start processing the first message after the last committed offset. By default, this service will do this automatically
 * every 60 seconds; this is easier and more efficient, since it limits the number of operations that need to be performed while
 * processing large numbers of or even unbounded input messages. Using automatic commits also means that, should the service crash
 * after processing some messages without auto-committing their offsets, upon recovery it will re-process a second time those same
 * uncommitted messages. To use automatic commits, set the "{@code auto.commit.enabled}" property to "{@code true}" and the "
 * {@code auto.commit.interval.ms}" property to the desired interval in milliseconds.
 * <p>
 * Therefore, <strong><em>it is recommended that auto-commit be disabled</em></strong> so that this service will commit the offset
 * after processing each input message, reducing the likelihood of this service generating a duplicate output message. To disable
 * automatic commits, set the "{@code auto.commit.enabled}" property to "{@code false}".
 * 
 * <h2>Exactly-once and At-least once processing</h2>
 * <p>
 * This service will consume all input messages <em>exactly once</em> when operating normally. However, if the service crashes, it
 * is likely to produce some messages that are duplicates of some messages output prior to the crash. How many messages might be
 * re-processed or re-produced upon startup/recovery is dictated by how many input messages were fully-processed before committing
 * the offsets. As such, during recovery this service can only guarantee it will process <em>at least once</em> those messages
 * fully processed but not fully committed prior to the crash, but any subsequent messages (not processed at all) will be
 * processed <em>exactly once</em>.
 * <p>
 * To minimize the potential for duplicate downstream messages following a crash, turn off auto-commits so that the service
 * explicitly commits after fully-processing each input message. If the service were to crash after consuming an input message but
 * before producing any output messages, then upon recovery the service will continue where it left off and no duplicates will be
 * resent. However, if the service were to crash after consuming an input message and after producing some of the corresponding
 * output messages, then upon recovery the service will re-process that input message and will produce new output messages,
 * potentially resulting in some duplicate messages for this one input message.
 * <p>
 * It is likely that this is acceptable, because only this service will be outputting messages for that database, so any duplicate
 * output messages will immediately follow the identical messages output before the crash. Since patches are idempotent, the
 * duplicates will likely have no adverse effect.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public final class EntityBatchService extends ServiceProcessor {

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
        return ServiceRunner.use(EntityBatchService.class, EntityBatchService::topology)
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
        topology.addSource("input", Serdes.stringDeserializer(), Serdes.document(), Topic.ENTITY_BATCHES);
        topology.addProcessor("service", processorDef(new EntityBatchService(config)), "input");
        topology.addSink("patches", Topic.ENTITY_PATCHES, Serdes.stringSerializer(), Serdes.document(), "service");
        topology.addSink("responses", Topic.PARTIAL_RESPONSES, Serdes.stringSerializer(), Serdes.document(), "service");
        return topology;
    }
    
    /**
     * The name of this service.
     */
    public static final String SERVICE_NAME = "entity-batch";
    
    private static final int PATCHES = 0;
    private static final int RESPONSES = 1;

    public EntityBatchService(Configuration config) {
        super(SERVICE_NAME, config);
    }

    @Override
    protected void init() {
    }

    @Override
    protected void process(String topic, int partition, long offset, String requestId, Document request) {
        // Construct the batch from the request ...
        Batch<EntityId> batch = Batch.from(request);
        if (batch == null) {
            // This is not a batch and therefore is likely a patch ...
            EntityId id = Message.getEntityId(request);
            if (id != null) {
                Message.setParts(request, 1, 1);
                context().forward(id.asString(), request,PATCHES);
            }
            return;
        }
        if (batch.isEmpty()) {
            return;
        }

        DatabaseId dbId = Message.getDatabaseId(request);
        if (dbId == null) batch.stream().findFirst().get().target().databaseId();

        // Fire off a separate request for each patch ...
        int parts = batch.patchCount();
        AtomicInteger partCounter = new AtomicInteger(0);
        batch.forEach(patch -> {
            // Construct the response message and fire it off ...
            Document patchRequest = Message.createPatchRequest(request, patch);

            // Set the headers and the request parts ...
            Message.copyHeaders(request, patchRequest);
            Message.setParts(patchRequest, partCounter.incrementAndGet(), parts);

            EntityId entityId = patch.target();
            if (dbId.equals(entityId.databaseId())) {
                // Send the message for this patch (only if the patched entity is in the same database as the batch request) ...
                context().forward(entityId.type().asString(), request, PATCHES);
            } else {
                // The batch was poorly formed, so write it out to the partial response topic ...
                Document response = Message.createResponseFromRequest(request);
                Message.setStatus(response, Status.PATCH_FAILED);
                Message.addFailureReason(response, "Malformed batch: entity '" + patch.target()
                        + "' is not in the same batch's target database of '" + dbId + "'");
                Message.setEnded(response, context().timestamp());
                context().forward(requestId, response, RESPONSES);
            }
        });
    }

    @Override
    public void close() {
    }
}
