/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import kafka.consumer.TopicFilter;
import kafka.producer.KeyedMessage;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link MessageBus} that uses in-memory queues as topics, and runs each registered
 * {@link #subscribe(String, TopicFilter, int, Deserializer, Deserializer, MessageConsumer) subscriber} in a separate thread.
 */
public class InMemoryAsyncMessageBus implements MessageBus {

    private final String name;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Supplier<Executor> executor;
    private final ConcurrentMap<String, InMemoryStream> streams = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    public InMemoryAsyncMessageBus(String name, Supplier<Executor> executor) {
        this.name = name != null ? name : this.getClass().getName();
        this.executor = executor;
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    public void createStreams( String... streamNames ) {
        for ( String streamName : streamNames ) {
            getStream(streamName);
        }
    }

    protected InMemoryStream getStream(String streamName) {
        return streams.compute(streamName, (key,stream)->{
            if ( stream == null ) {
                logger.trace("BUS: creating new topic '{}'",streamName);
                stream = new InMemoryStream(streamName);
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

    @Override
    public <KeyType, MessageType> void subscribe(String groupId, TopicFilter topicFilter, int numThreads, Deserializer<KeyType> keyDecoder,
                                                 Deserializer<MessageType> messageDecoder, MessageConsumer<KeyType, MessageType> consumer) {
        logger.trace("BUS: subscribing {} in group '{}' to topics {}",consumer,groupId,topicFilter);
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
            logger.debug("BUS: adding thread to execute consumer {}",consumer);
            while (running) {
                suppliers.forEach(consumerGroup -> {
                    KeyedMessage<byte[], byte[]> message = consumerGroup.get(offset);
                    if (message != null && running) {
                        if ( logger.isTraceEnabled() ) {
                            logger.trace("BUS: got message '{}' from group '{}'",new String(message.key()),consumerGroup);
                        }
                        KeyType key = keyDecoder.deserialize(message.topic(),message.key());
                        MessageType m = messageDecoder.deserialize(message.topic(),message.message());
                        if (key == null) throw new RuntimeException("Decoded key as null");
                        if (m == null) throw new RuntimeException("Decoded message as null for key '" + key + "'");
                        String topic = message.topic();
                        if (running) {
                            try {
                                consumer.consume(topic, 1, offset.get(), key, m);
                            } catch (Throwable t) {
                                logger.error("Error consuming message on topic '{}' with offset {} and key {}", topic, offset.get(), key, t);
                            }
                        }
                    }
                });
            }
            logger.debug("BUS: Completed thread to execute consumer {}", consumer);
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
                logger.trace("BUS: consumer group '{}' received message '{}' on topic '{}'; adding to queue",groupId,new String(message.key()),message.topic());
            }
            queue.add(message);
            offset.incrementAndGet();
        }

        public synchronized KeyedMessage<byte[], byte[]> get(AtomicLong offset) {
            offset.set(this.offset.get());
            KeyedMessage<byte[],byte[]> message = queue.poll();
            if ( message != null && logger.isTraceEnabled() ) {
                logger.trace("BUS: consumer group '{}' consuming message '{}' on topic '{}'",groupId,new String(message.key()),message.topic());
            }
            return message;
        }
        @Override
        public String toString() {
            return groupId;
        }
    }

    protected final class InMemoryStream implements MessageProducer {

        private ConcurrentMap<String, ConsumerGroup> consumersByGroupId = new ConcurrentHashMap<>();

        public InMemoryStream(String streamName) {
        }

        @Override
        public boolean send(KeyedMessage<byte[], byte[]> message) {
            if ( logger.isTraceEnabled() ) {
                logger.trace("BUS: sending message '{}' on topic '{}' to consumer groups: {}",new String(message.key()),message.topic(),consumersByGroupId.keySet());
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
