/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.util.concurrent.TimeUnit;

import org.debezium.Testing;
import org.debezium.client.Debezium.Acknowledgement;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class DbzNodeExternalKafkaTest extends AbstractDbzNodeTest {

    //@Ignore
    @Test
    public void shouldConnectToKafka() throws InterruptedException {
        Testing.Print.enable();
        Testing.Debug.enable();
        
        DbzConfiguration config = (DbzConfiguration) Debezium.configure()
                .clientId(DatabaseTest.class.getSimpleName())
                .withBroker("localhost:9092")
                .withZookeeper("localhost:2181/")
                .acknowledgement(Acknowledgement.ALL)
                .lazyInitialization(true)
                .build();
        startWith(config.getDocument());
        sendAndReceiveMessages(100, 1, "dbz-node-test", 10, TimeUnit.SECONDS);
    }
}
