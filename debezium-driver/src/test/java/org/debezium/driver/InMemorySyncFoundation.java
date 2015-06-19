/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.TopicFilter;
import kafka.producer.KeyedMessage;

import org.debezium.core.serde.Decoder;

/**
 * An implementation of {@link Foundation} that will forward published messages directly (in the same thread) to the subscribers.
 */
public class InMemorySyncFoundation implements Foundation {

    @FunctionalInterface
    private static interface CurrentOffset {
        public long offsetFor(String topic);
    }

    private static final class Subscriber<KeyType, MessageType> {
        private final TopicFilter topicFilter;
        private final Decoder<KeyType> keyDecoder;
        private final Decoder<MessageType> messageDecoder;
        private final MessageConsumer<KeyType, MessageType> consumer;

        public Subscriber(String groupId, TopicFilter topicFilter, int numThreads, Decoder<KeyType> keyDecoder,
                Decoder<MessageType> messageDecoder, MessageConsumer<KeyType, MessageType> consumer) {
            this.topicFilter = topicFilter;
            this.keyDecoder = keyDecoder;
            this.messageDecoder = messageDecoder;
            this.consumer = consumer;
        }

        public boolean consume(KeyedMessage<byte[], byte[]> message, int partition, long offset ) {
            // First determine if the message even applies ...
            boolean excludeInternalTopics = false;
            if (topicFilter.isTopicAllowed(message.topic(), excludeInternalTopics)) {
                // Decode the key and message ...
                KeyType key = keyDecoder.fromBytes(message.key());
                MessageType m = messageDecoder.fromBytes(message.message());
                if (key == null) throw new RuntimeException("Decoded key as null");
                if (m == null) throw new RuntimeException("Decoded message as null for key '" + key + "'");
                String topic = message.topic();
                return consumer.consume(topic, partition, offset, key, m);
            }
            return false;
        }
    }

    private final List<Subscriber<?, ?>> subscribers = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, AtomicLong> offsetsByTopicName = new ConcurrentHashMap<>();

    public InMemorySyncFoundation() {
    }

    @Override
    public MessageProducer producer() {
        return this::sendMessage;
    }

    @Override
    public <KeyType, MessageType> void subscribe(String groupId, TopicFilter topicFilter, int numThreads, Decoder<KeyType> keyDecoder,
                                                 Decoder<MessageType> messageDecoder, MessageConsumer<KeyType, MessageType> consumer) {
        subscribers.add(new Subscriber<>(groupId, topicFilter, numThreads, keyDecoder, messageDecoder, consumer));
    }

    @Override
    public void shutdown() {
        subscribers.clear();
    }

    private boolean sendMessage(KeyedMessage<byte[], byte[]> message) {
        // Determine the offset for this topic ...
        long offset = nextOffsetForTopic(message.topic());

        // Send the message to all of the applicable subscribers ...
        subscribers.forEach(subscriber -> subscriber.consume(message,1,offset));
        return true;
    }

    private long nextOffsetForTopic(String topic) {
        return offsetsByTopicName.compute(topic, (t, offset) -> {
            if (offset == null) new AtomicLong(0);
            offset.incrementAndGet();
            return offset;
        }).get();
    }

}
