/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.test;

import java.util.concurrent.TimeUnit;

import org.debezium.Testing;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Identifier;
import org.debezium.core.util.Stopwatch;
import org.debezium.driver.Database;
import org.debezium.driver.Debezium;
import org.debezium.driver.DebeziumConnectionException;
import org.debezium.driver.InMemorySystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class InMemoryIntegrationTest implements Testing {

    private final DatabaseId dbId = Identifier.of("my-db");
    private final String username = "jsmith";
    private final String device = "my-device";
    private final String appVersion = "2.1";
    private InMemorySystem system;
    private Debezium.Client client;

    @Before
    public void beforeEach() {
        system = new InMemorySystem();
        client = system;
    }

    @After
    public void afterEach() {
        try {
            system.shutdown(10, TimeUnit.SECONDS);
        } finally {
            system = null;
            client = null;
        }
    }

    @Test(expected = DebeziumConnectionException.class)
    public void shouldNotConnectToNonExistantDatabase() {
        DatabaseId dbId = Identifier.of("non-existant-db");
        Database db = client.connect(dbId, username, device, appVersion);
        assertThat(db.isConnected()).isTrue();
        db.close();
    }

    @Test
    public void shouldCreateAndConnectToDatabase() {
        //Testing.Print.enable();
        Database db = client.provision(dbId, username, device, appVersion);
        assertThat(db.isConnected()).isTrue();
        db.close();
        Stopwatch sw = Stopwatch.restartable();
        for (int i = 0; i != 10; ++i) {
            sw.start();
            // Connect again ...
            db = client.connect(dbId, username, device, appVersion);
            sw.stop();
            assertThat(db.isConnected()).isTrue();
            db.close();
        }
        Testing.print("Time to connect to database 10x: " + sw );
    }

}
