/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

/**
 * A function that consumes a message from a topic.
 *
 * @param <KeyType> the type of key
 * @param <MessageType> the type of message
 */
@FunctionalInterface
public interface MessageConsumer<KeyType, MessageType> {
    /**
     * Consume a message at the given offset in the given partition from the named topic.
     * 
     * @param topic the name of the topic
     * @param partition the partition number
     * @param offset the logical offset within the partition
     * @param key the key for the message
     * @param message the message
     * @return {@code true} if the message was fully consumed, or {@code false} if the message was not fully consumed
     */
    boolean consume(String topic, int partition, long offset, KeyType key, MessageType message);
}