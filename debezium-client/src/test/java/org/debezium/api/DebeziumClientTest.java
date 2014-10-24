/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api;

import static org.fest.assertions.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.debezium.KafkaTestCluster;
import org.debezium.Testing;
import org.debezium.core.id.Identifier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DebeziumClientTest implements Testing {
    
    private static KafkaTestCluster kafka;
    private static Debezium.Configuration config;

    private Debezium.Client client;
    
    @BeforeClass
    public static void beforeAll() throws Exception {
        kafka = KafkaTestCluster.forTest(DebeziumClientTest.class);
        config = Debezium.configure()
                         .clientId(DebeziumClientTest.class.getName())
                         .withBroker(kafka.getKafkaBrokerString())
                         .build();
    }
    
    @AfterClass
    public static void afterAll() throws Exception {
        kafka.stopAndCleanUp();
    }
    
    @After
    public void afterEach() {
        if ( client != null ) {
            try {
                client.shutdown(5,TimeUnit.SECONDS);
            } finally {
                client = null;
            }
        }
    }
    
    @Test
    public void shouldConnect() throws Exception {
        Print.enable();
        client = Debezium.start(config);
        Testing.print("Calling 'Database.loadData()' to preload a bunch of messages ...");
        Database database = client.connect(Identifier.of("firstDatabase") ,"jsmith");
        assertThat(database).isNotNull();
        //database.loadData();
        Testing.print("Completed ...");
    }
}
