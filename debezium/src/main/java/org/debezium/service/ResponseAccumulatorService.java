/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.debezium.Configuration;
import org.debezium.Debezium;
import org.debezium.annotation.NotThreadSafe;
import org.debezium.kafka.Serdes;
import org.debezium.message.Document;
import org.debezium.message.Message;
import org.debezium.message.Topic;
import org.debezium.util.Collect;

/**
 * A service responsible for aggregating the partial responses that make up a batch (multi-part) request. This service consumes
 * the {@value Topic#PARTIAL_RESPONSES} Kafka topic, which is partitioned by client ID so that all responses for a single client
 * appear in a single partition and thus handled by a single instance of this service. This service builds up the complete
 * response for a batch request, adding the partial response for each part of the larger request. When complete, this service
 * outputs the aggregate response to the {@value Topic#COMPLETE_RESPONSES} topic partitioned by client ID.
 * 
 * <h2>Service execution</h2>
 * <p>
 * This service has a {@link #main(String[]) main} method that when run starts up a Kafka stream processing job with a custom
 * topology and configuration. Each instance of the KafkaProcess can run a configurable number of threads, and users can start
 * multiple instances of their process job by starting multiple JVMs to run this service's {@link #main(String[])} method.
 * 
 * <h2>Configuration</h2>
 * <p>
 * The ResponseAccumulatorService keeps an in-memory LRU cache of recently used aggregate response {@link Document} objects, and
 * the service ensures this is a subset of the persisted store of uncompleted aggregate responses. The capacity of the LRU cache
 * should be a power of two for efficient use of memory, and it should be large enough to hold most of the recently-used aggregate
 * responses and if possible should be at least as large as the number of anticipated in-flight aggregate responses divided by the
 * number of service instances. The default size is 1024. Use the "{@code max.aggregate.cache.size}" configuration property to
 * explicitly set the maximum size of the LRU cache.
 * <p>
 * The ResponseAccumulatorService can be configured to clear out upon startup any aggregate responses that were previously
 * completed but not removed from this service's storage. Such stale aggregate responses occur very rarely and only when the
 * service crashes after completely processing and sending a completed aggregate response but before the aggregate response can be
 * removed from the service's local store. By default the maximum aggregate response age is 60 minutes, and this will often be
 * acceptable given that crashes are infrequent and at most a single completed aggregate response is maintained per crash.
 * However, a maximum age in minutes can be explicitly set using the "{@code aggregate.max.age.minutes}" configuration property.
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
 * auto-commit, set the "{@code auto.commit.enabled}" property to "{@code true}" and the "{@code auto.commit.interval.ms}"
 * property to the desired interval in milliseconds. Auto-commit is enabled by default default with a default interval of 60
 * seconds. Simply set the "{@code auto.commit.enabled}" property to "{@code false}" to have the service explicitly commit after
 * completely processing every input message.
 * 
 * <h2>Exactly-once processing</h2>
 * <p>
 * This service is tolerant of duplicate messages on the input {@value Topic#PARTIAL_RESPONSES} topic: if a message with a partial
 * response is read and that response has already been seen, the duplicate response will simply be ignored.
 * <p>
 * In the event a service instance crashes, upon restart it will use the auto-committed offsets to determine where it needs to
 * start consuming the input stream. If some of the consumed input messages were previously read and completely processed prior to
 * the crash, this service will discover this and will ignore these duplicate input messages. However, it is possible that the
 * service crashed after outputting the last aggregate response prior to recording that it was complete, and in this case the
 * recovering service may re-write one duplicate aggregate response message to the {@value Topic#COMPLETE_RESPONSES} topic. This
 * duplicate aggregate response message will be identical to the message previously written prior to the crash, and thus
 * downstream consumers can decide whether to ignore or re-process the duplicate.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public final class ResponseAccumulatorService extends ServiceProcessor {

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
        return ServiceRunner.use(ResponseAccumulatorService.class, ResponseAccumulatorService::topology)
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
        topology.addSource("input", Serdes.stringDeserializer(), Serdes.document(), Topic.PARTIAL_RESPONSES);
        topology.addProcessor("service", processorDef(new ResponseAccumulatorService(config)), "input");
        topology.addSink("responses", Topic.COMPLETE_RESPONSES, Serdes.stringSerializer(), Serdes.document(), "service");
        return topology;
    }

    /**
     * The name of this service.
     */
    public static final String SERVICE_NAME = "response-accumulator";

    private final Map<String, Document> transientResponses;
    private KeyValueStore<String, Document> aggregateResponses;
    private KeyValueStore<Integer, Long> inputOffsets;
    private final long maxAgeInMillis;
    private final int maxCacheSize;

    public ResponseAccumulatorService(Configuration config) {
        super(SERVICE_NAME, config);
        // Read the configuration ...
        maxAgeInMillis = TimeUnit.MINUTES.toMillis(config.getLong("aggregate.max.age.minutes", 60));
        maxCacheSize = config.getInteger("max.aggregate.cache.size", 1024);
        transientResponses = Collect.fixedSizeMap(maxCacheSize);
    }

    @Override
    protected void init() {
        this.aggregateResponses = new InMemoryKeyValueStore<>("aggregate-responses", context());
        this.inputOffsets = new InMemoryKeyValueStore<>("aggregate-inputs", context());

        // Load the models from the store, removing any that are too old ...
        Set<String> oldKeys = new HashSet<>();
        this.aggregateResponses.all().forEachRemaining(entry -> {
            // If the response is completed and old, then mark it for deletion ...
            if (Message.isAggregateResponseCompletedAndExpired(entry.value(), this::isExpired)) {
                oldKeys.add(entry.key());
            }
        });
        // And finally remove all of the expired responses ...
        oldKeys.forEach(this.aggregateResponses::delete);
    }

    protected boolean isExpired(long completionTimestamp) {
        assert context().timestamp() > 1L && context().timestamp() < Long.MAX_VALUE;
        long ageInMillis = Math.abs(completionTimestamp - context().timestamp());
        return ageInMillis > maxAgeInMillis;
    }

    @Override
    protected void process(String topic, int partition, long offset, String clientId, Document partialResponse) {
        // Determine if we've seen this message before ...
        Integer partitionKey = Integer.valueOf(partition);
        Long completedOffset = this.inputOffsets.get(partitionKey);
        if (offset <= completedOffset.longValue()) {
            // We're recovering from a crash by re-processing input messages (based upon the last committed offset) and
            // we know we've seen and completely processed this message prior to the crash. So we can skip this message ...
            return;
        }

        if (Message.getParts(partialResponse) == 1) {
            // This is the only message in the batch, so forward it on directly ...
            context().forward(clientId, partialResponse);

            // And record that we've processed it ...
            this.inputOffsets.put(partitionKey, Long.valueOf(offset));
            return;
        }

        // Otherwise, there is more than 1 part to the batch ...
        String requestKey = clientId + "/" + Message.getRequest(partialResponse);
        Document aggregateResponse = getAggregateResponse(requestKey);
        boolean done = false;
        if (aggregateResponse == null) {
            // This is the first part we've seen ...
            aggregateResponse = Message.createAggregateResponseFrom(partialResponse);
        } else {
            // We already have an aggregate ...
            done = Message.addToAggregateResponse(aggregateResponse, partialResponse, context().timestamp());
        }

        if (done) {
            // First write out the aggregate response ...
            context().forward(clientId, aggregateResponse);
            // Record that we've processed this input message ...
            this.inputOffsets.put(partitionKey, Long.valueOf(offset));
            // Finally remove the aggregate response from our store ...
            removeAggregateResponse(requestKey);
        } else {
            // Store the aggregate response in our store ...
            updateAggregateResponse(requestKey, aggregateResponse);
            // Write out the aggregate response ...
            context().forward(clientId, aggregateResponse);
            // And record that we've processed this input message ...
            this.inputOffsets.put(partitionKey, Long.valueOf(offset));
        }
    }

    @Override
    public void close() {
        this.aggregateResponses.close();
        this.inputOffsets.close();
    }

    private Document getAggregateResponse(String requestKey) {
        // Look first in the fixed-size LRU transient cache ...
        Document aggregateResponse = transientResponses.get(requestKey);
        if (aggregateResponse == null) {
            // We didn't find it, so look in the persistent store ...
            aggregateResponse = aggregateResponses.get(requestKey);
            if (aggregateResponse != null) {
                // We found it, so put it back into the transient cache ...
                transientResponses.put(requestKey, aggregateResponse);
            }
        }
        return aggregateResponse;
    }

    private void removeAggregateResponse(String requestKey) {
        aggregateResponses.delete(requestKey);
        transientResponses.remove(requestKey);
    }

    private void updateAggregateResponse(String requestKey, Document aggregateResponse) {
        aggregateResponses.put(requestKey, aggregateResponse);
        transientResponses.put(requestKey, aggregateResponse);
    }
}
