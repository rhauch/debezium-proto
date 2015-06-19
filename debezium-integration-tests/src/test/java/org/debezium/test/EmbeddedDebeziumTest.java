/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.test;

import java.util.concurrent.TimeUnit;

import org.debezium.Testing;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.util.Stopwatch;
import org.debezium.core.util.Stopwatch.StopwatchSet;
import org.debezium.driver.Database;
import org.debezium.driver.Debezium;
import org.debezium.driver.DebeziumConnectionException;
import org.debezium.driver.EmbeddedDebezium;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import static org.fest.assertions.Fail.fail;

/**
 * A set of integration tests for Debezium, using {@link EmbeddedDebezium} instances.
 * @author Randall Hauch
 */
public class EmbeddedDebeziumTest implements Testing {

    private final DatabaseId dbId = Identifier.of("my-db");
    private final EntityType contact = Identifier.of(dbId,"contact");
    private final String username = "jsmith";
    private final String device = "my-device";
    private final String appVersion = "2.1";
    private EmbeddedDebezium system;
    private Debezium.Client client;
    private Stopwatch sw;

    @Before
    public void beforeEach() {
        system = new EmbeddedDebezium();
        client = system;
        sw = Stopwatch.accumulating();
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
        Testing.Print.enable();
        sw.start();
        Database db = client.provision(dbId, username, device, appVersion);
        sw.stop();
        assertThat(db.isConnected()).isTrue();
        db.close();
        Testing.print("Time to provision database: " + sw );
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

    @Test
    public void shouldCreateDatabaseAndReadAndConnectToDatabase() throws InterruptedException {
        Testing.Print.enable();
        Database db = client.provision(dbId, username, device, appVersion);
        assertThat(db.isConnected()).isTrue();
        StopwatchSet stopwatches = Stopwatch.multiple();
        for ( int i=0; i!=10; ++i ) {
            EntityId entityId = Identifier.newEntity(contact);
            Stopwatch sw = stopwatches.create().start();
            db.readEntity(entityId,outcome->{
                sw.stop();
                    if ( !outcome.failed() ) fail("Should not have found this entity: " + entityId);
            });
        }
        stopwatches.await(10, TimeUnit.SECONDS);
        Testing.print("Average time to read 10 non-existent entities: " + stopwatches.averageAsString() );
    }

}
