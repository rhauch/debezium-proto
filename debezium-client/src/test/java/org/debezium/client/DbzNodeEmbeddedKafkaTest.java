/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.debezium.KafkaTestCluster;
import org.debezium.client.Debezium.Acknowledgement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
@Ignore("This doesn't seem to allow consumers to connect to it.")
public class DbzNodeEmbeddedKafkaTest extends AbstractDbzNodeTest {

    private static KafkaTestCluster kafka;

    @BeforeClass
    public static void beforeAll() throws Exception {
        kafka = KafkaTestCluster.forTest(DbzNodeEmbeddedKafkaTest.class);
    }
    
    @AfterClass
    public static void afterAll() throws IOException {
        kafka.stopAndCleanUp();
    }

    @Test
    public void shouldConnectToKafka() throws InterruptedException {
        System.out.println("Kafka broker string: " + kafka.getKafkaBrokerString());
        System.out.println("Zookeeper connect string: " + kafka.getZkConnectString());
        
        DbzConfiguration config = (DbzConfiguration) Debezium.configure()
                .clientId(DatabaseTest.class.getSimpleName())
                .withBroker(kafka.getKafkaBrokerString())
                .withZookeeper(kafka.getZkConnectString())
                .acknowledgement(Acknowledgement.ALL)
                .lazyInitialization(true)
                .build();
        startWith(config.getDocument());
        sendAndReceiveMessages(10, 1, "dbz-embedded-node-test", 10, TimeUnit.SECONDS);
    }

}
