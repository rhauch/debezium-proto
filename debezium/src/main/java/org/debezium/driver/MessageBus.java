/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import kafka.consumer.TopicFilter;

import org.apache.kafka.common.serialization.Deserializer;

/**
 * An abstraction for the messaging foundation.
 */
public interface MessageBus {

    /**
     * Get the name of this message bus, for logging and reporting purposes.
     * 
     * @return the name; may not be null
     */
    public String getName();

    /**
     * Get the producer.
     * 
     * @return the producer; never null
     */
    public MessageProducer producer();

    /**
     * Subscribe to one or more topics.
     * 
     * @param groupId the identifier of the consumer's group; may not be null
     * @param topicFilter the filter for the topics; may not be null
     * @param numThreads the number of threads on which consumers should be called
     * @param keyDeserializer the deserializer that should be used to convert the {@code byte[]} key into an object form expected
     *            by the consumer
     * @param messageDeserializer the deserializer that should be used to convert the {@code byte[]} message into an object form
     *            expected by the consumer
     * @param consumer the consumer, which should be threadsafe if {@code numThreads} is more than 1
     */
    public <KeyType, MessageType> void subscribe(String groupId, TopicFilter topicFilter, int numThreads,
                                                 Deserializer<KeyType> keyDeserializer,
                                                 Deserializer<MessageType> messageDeserializer,
                                                 MessageConsumer<KeyType, MessageType> consumer);

    /**
     * Release all resources used by this message bus.
     */
    public void shutdown();
}
