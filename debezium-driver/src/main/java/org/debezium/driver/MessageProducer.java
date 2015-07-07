/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import kafka.producer.KeyedMessage;

/**
 * A producer of messages.
 */
@FunctionalInterface
public interface MessageProducer {
    /**
     * Send a message.
     * 
     * @param message the message to send
     * @return true if the message was sent, or false otherwise
     */
    public boolean send(KeyedMessage<byte[], byte[]> message);
}