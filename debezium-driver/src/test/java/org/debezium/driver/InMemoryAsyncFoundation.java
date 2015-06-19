/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import kafka.consumer.TopicFilter;
import kafka.producer.KeyedMessage;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;
import org.debezium.core.serde.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link Foundation} that uses in-memory queues as topics, and runs each registered
 * {@link #subscribe(String, TopicFilter, int, Decoder, Decoder, MessageConsumer) subscriber} in a separate thread.
 */
public class InMemoryAsyncFoundation implements Foundation {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String systemName;
    private final Supplier<Executor> executor;
    private final ConcurrentMap<String, InMemoryStream> streams = new ConcurrentHashMap<>();
    private final Set<SystemStreamPartition> ssps = Collections.newSetFromMap(new ConcurrentHashMap<SystemStreamPartition, Boolean>());
    private final Set<SystemStreamPartition> unmodssps = Collections.unmodifiableSet(ssps);
    private volatile boolean running = true;

    public InMemoryAsyncFoundation(String systemName, Configuration config, Supplier<Executor> executor) {
        this.systemName = systemName;
        this.executor = executor;
    }
    
    public void createStreams( String... streamNames ) {
        for ( String streamName : streamNames ) {
            getStream(streamName);
        }
    }

    protected InMemoryStream getStream(String streamName) {
        return streams.compute(streamName, (key,stream)->{
            if ( stream == null ) {
                logger.trace("FOUNDATION: creating new topic '{}'",streamName);
                stream = new InMemoryStream(systemName, streamName);
                ssps.add(stream.getSystemStreamPartition());
            }
            return stream;
        });
    }

    @Override
    public MessageProducer producer() {
        return this::sendMessage;
    }

    private boolean sendMessage(KeyedMessage<byte[], byte[]> message) {
        // Send to the correct stream (which will be created if not there) ...
        return getStream(message.topic()).send(message);
    }

    public Set<SystemStreamPartition> getSystemStreamPartitions() {
        return unmodssps;
    }

    @Override
    public <KeyType, MessageType> void subscribe(String groupId, TopicFilter topicFilter, int numThreads, Decoder<KeyType> keyDecoder,
                                                 Decoder<MessageType> messageDecoder, MessageConsumer<KeyType, MessageType> consumer) {
        logger.trace("FOUNDATION: subscribing {} in group '{}' to topics {}",consumer,groupId,topicFilter);
        // Accumulate the consumer group for each topic that this subscriber will need to check ...
        List<ConsumerGroup> suppliers = new ArrayList<>();
        boolean excludeInternalTopics = false;
        this.streams.entrySet()
                    .stream()
                    .filter(entry -> topicFilter.isTopicAllowed(entry.getKey(), excludeInternalTopics))
                    .forEach(entry -> suppliers.add(entry.getValue().addConsumer(groupId)));

        // Add a single thread that continually checks the consumer group for each of the matching topics ...
        AtomicLong offset = new AtomicLong();
        executor.get().execute(() -> {
            logger.trace("FOUNDATION: adding thread to execute consumer {}",consumer);
            while (running) {
                suppliers.forEach(consumerGroup -> {
                    KeyedMessage<byte[], byte[]> message = consumerGroup.get(offset);
                    if (message != null && running) {
                        if ( logger.isTraceEnabled() ) {
                            logger.trace("FOUNDATION: got message '{}' from group '{}'",new String(message.key()),consumerGroup);
                        }
                        KeyType key = keyDecoder.fromBytes(message.key());
                        MessageType m = messageDecoder.fromBytes(message.message());
                        if (key == null) throw new RuntimeException("Decoded key as null");
                        if (m == null) throw new RuntimeException("Decoded message as null for key '" + key + "'");
                        String topic = message.topic();
                        if (running) {
                            try {
                                consumer.consume(topic, 1, offset.get(), key, m);
                            } catch (Throwable t) {
                                logger.error("Error consuming message on topic {} with offset {} and key {}", t, topic, offset.get(), key);
                            }
                        }
                    }
                });
            }
        });
    }

    @Override
    public void shutdown() {
        running = false;
    }

    protected final class ConsumerGroup implements Consumer<KeyedMessage<byte[], byte[]>> {
        private final Queue<KeyedMessage<byte[], byte[]>> queue = new LinkedList<>();
        private final AtomicLong offset = new AtomicLong();
        private final String groupId;
        public ConsumerGroup( String groupId ) {
            this.groupId = groupId;
        }

        @Override
        public synchronized void accept(KeyedMessage<byte[], byte[]> message) {
            if ( logger.isTraceEnabled() ) {
            logger.trace("FOUNDATION: consumer group '{}' received message '{}' on topic '{}'; adding to queue",groupId,new String(message.key()),message.topic());
            }
            queue.add(message);
            offset.incrementAndGet();
        }

        public synchronized KeyedMessage<byte[], byte[]> get(AtomicLong offset) {
            offset.set(this.offset.get());
            KeyedMessage<byte[],byte[]> message = queue.poll();
            if ( message != null && logger.isTraceEnabled() ) {
                logger.trace("FOUNDATION: consumer group '{}' consuming message '{}' on topic '{}'",groupId,new String(message.key()),message.topic());
            }
            return message;
        }
        @Override
        public String toString() {
            return groupId;
        }
    }

    protected final class InMemoryStream implements MessageProducer {

        private SystemStreamPartition ssp;
        private ConcurrentMap<String, ConsumerGroup> consumersByGroupId = new ConcurrentHashMap<>();

        public InMemoryStream(String systemName, String streamName) {
            ssp = new SystemStreamPartition(systemName, streamName, new Partition(1));
        }

        public SystemStreamPartition getSystemStreamPartition() {
            return ssp;
        }

        @Override
        public boolean send(KeyedMessage<byte[], byte[]> message) {
            if ( logger.isTraceEnabled() ) {
            logger.trace("FOUNDATION: sending message '{}' on topic '{}' to consumer groups",new String(message.key()),message.topic());
            }
            // Write out to each of the consumer groups (if there are any) ...
            consumersByGroupId.forEach((groupId, group) -> group.accept(message));
            return true;
        }

        public ConsumerGroup addConsumer(String groupId) {
            return consumersByGroupId.compute(groupId, (key, group) -> group != null ? group : new ConsumerGroup(groupId));
        }
        @Override
        public String toString() {
            return consumersByGroupId.keySet().toString();
        }

    }

}
