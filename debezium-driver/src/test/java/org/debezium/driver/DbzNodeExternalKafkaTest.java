/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.TimeUnit;

import org.debezium.Testing;
import org.debezium.driver.Debezium.Acknowledgement;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
@Ignore
public class DbzNodeExternalKafkaTest extends AbstractDbzNodeTest {

    @Test
    public void shouldConnectToKafka() throws InterruptedException {
        Testing.Print.enable();
        Testing.Debug.enable();
        
        startWith(Debezium.configure()
                .clientId(DatabaseTest.class.getSimpleName())
                .withBroker("localhost:9092")
                .withZookeeper("localhost:2181/")
                .acknowledgement(Acknowledgement.ALL)
                .initializeProducerImmediately(true));
        sendAndReceiveMessages(100, 1, "dbz-node-test", 10, TimeUnit.SECONDS);
    }
}
