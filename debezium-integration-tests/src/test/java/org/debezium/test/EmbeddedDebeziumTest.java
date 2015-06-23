/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.debezium.Testing;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.util.VariableLatch;
import org.debezium.driver.AbstractDebeziumTest;
import org.debezium.driver.Database;
import org.debezium.driver.Debezium.Client;
import org.debezium.driver.DebeziumConnectionException;
import org.debezium.driver.EmbeddedDebezium;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import static org.fest.assertions.Fail.fail;

/**
 * A set of integration tests for Debezium, using {@link EmbeddedDebezium} instances.
 * 
 * @author Randall Hauch
 */
public class EmbeddedDebeziumTest extends AbstractDebeziumTest {

    @Override
    protected Client createClient() {
        return new EmbeddedDebezium();
    }

    @Override
    protected void shutdownClient(Client client, long timeout, TimeUnit unit) {
        ((EmbeddedDebezium) client).shutdown(10, TimeUnit.SECONDS);
    }

    @Test(expected = DebeziumConnectionException.class)
    public void shouldNotConnectToNonExistantDatabase() throws InterruptedException {
        DatabaseId dbId = Identifier.of("non-existant-db");
        try (Database db = client.connect(dbId, username, device, appVersion)) { // should throw exception
            assertThat(db.isConnected()).isFalse();
            fail("Unexpectedly found existing database");
        }
    }

    @Test
    public void shouldCreateOneEntity() {
        Database db = provisionDatabase();
        try {
            // Create the entity ...
            Collection<EntityId> ids = createEntities(db, 1, contactType);
            EntityId id = ids.stream().findFirst().get();
            // Read the entity ...
            db.readEntity(id, outcome -> {
                if (outcome.failed()) {
                    fail("Should not have failed; reason: " + outcome.failureReason());
                } else {
                    assertThat(outcome.result().count()).isEqualTo(1);
                    outcome.result().forEach(entity -> Testing.print("Received read entity: " + entity));
                }
            });
        } finally {
            db.close();
        }
    }

    @Test
    public void shouldCreateMultipleEntities() {
        //Testing.Print.enable();
        Database db = provisionDatabase();
        try {
            // Create the entities ...
            Collection<EntityId> ids = createEntities(db, 15, contactType);
            ids.forEach(id -> {
                // Read each entity one-by-one ...
                db.readEntity(id, outcome -> {
                    if (outcome.failed()) {
                        fail("Should not have failed; reason: " + outcome.failureReason());
                    } else {
                        assertThat(outcome.result().count()).isEqualTo(1);
                        outcome.result().forEach(entity -> Testing.print("Received read entity with id " + entity.id()));
                    }
                });
            });
            // Read the set of entities in one request ...
            db.readEntities(ids, outcome -> {
                if (outcome.failed()) {
                    fail("Should not have failed; reason: " + outcome.failureReason());
                } else {
                    assertThat(outcome.result().count()).isEqualTo(15);
                    outcome.result().forEach(entity -> Testing.print("Received read entity with id " + entity.id()));
                }
            });
        } finally {
            db.close();
        }
    }

    @Test
    public void shouldCreateAndConnectToDatabase() throws InterruptedException {
        // Testing.Print.enable();
        time("provision database", 1, this::provisionDatabase, this::closeDatabase);
        time("connect 10x", 10, this::connectToDatabase, this::closeDatabase);
    }

    @Test
    public void shouldCreateDatabaseAndConnectToDatabaseAndReadNonExistingEntities() throws InterruptedException {
        //Testing.Print.enable();
        provisionAndTime("submit 10 read requests for non-existent entities", 10, db -> {
            CountDownLatch latch = new CountDownLatch(1);
            EntityId entityId = Identifier.newEntity(contactType);
            db.readEntity(entityId, outcome -> {
                if (outcome.failed()) fail("Should not have failed; reason: " + outcome.failureReason());
                assertThat(outcome.result().count()).isEqualTo(0);
                latch.countDown();
            });
            latch.await(10, TimeUnit.SECONDS);
        });
    }

    @Test
    public void shouldCreateDatabaseAndConnectToDatabaseAndReadExistingEntity() throws InterruptedException {
        //Testing.Print.enable();
        provisionAndTime("submit 10 requests to read 1 existing entity", createEntities(1, contactType), 10, (db, ids) -> {
            CountDownLatch latch = new CountDownLatch(1);
            db.readEntities(ids, outcome -> {
                if (outcome.failed()) fail("Should not have failed; reason: " + outcome.failureReason());
                assertThat(outcome.result().count()).isEqualTo(ids.size());
                latch.countDown();
            });
            latch.await(10, TimeUnit.SECONDS);
        });
    }

    @Test
    public void shouldCreateDatabaseAndConnectToDatabaseAndReadExistingEntities() throws InterruptedException {
        //Testing.Print.enable();
        provisionAndTime("submit 30 requests to read 20 existing entities", createEntities(20, contactType), 1, (db, ids) -> {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicLong readCount = new AtomicLong();
            db.readEntities(ids, outcome -> {
                if (outcome.failed()) fail("Should not have failed; reason: " + outcome.failureReason());
                readCount.set(outcome.result().count());
                latch.countDown();
            });
            latch.await(10, TimeUnit.SECONDS);
            assertThat(readCount.get()).isEqualTo(ids.size());
        });
    }

    @Test
    public void shouldCreateDatabaseAndConnectToDatabaseAndReadExistingEntitiesInMultipleBatches() throws InterruptedException {
        //Testing.Print.enable();
        provisionAndTime("submit 10 requests to read 1 existing entity", 5, db -> {
            List<EntityId> ids = createEntities(db,15, contactType);
            VariableLatch latch = new VariableLatch(1);
            db.readEntities(ids, outcome -> {
                if (outcome.failed()) fail("Should not have failed; reason: " + outcome.failureReason());
                assertThat(outcome.result().count()).isEqualTo(ids.size());
                latch.countDown();
            });
            latch.await(10, TimeUnit.SECONDS);
            latch.countUp();
            List<EntityId> ids2 = createEntities(db,50, contactType);
            db.readEntities(ids2, outcome -> {
                if (outcome.failed()) fail("Should not have failed; reason: " + outcome.failureReason());
                assertThat(outcome.result().count()).isEqualTo(ids2.size());
                latch.countDown();
            });
            latch.await(10, TimeUnit.SECONDS);
            latch.countUp(ids.size());
            ids2.forEach(id -> {
                // Read each entity one-by-one ...
                db.readEntity(id, outcome -> {
                    if (outcome.failed()) {
                        fail("Should not have failed; reason: " + outcome.failureReason());
                    } else {
                        assertThat(outcome.result().count()).isEqualTo(1);
                        outcome.result().forEach(entity -> Testing.print("Received read entity with id " + entity.id()));
                    }
                    latch.countDown();
                });
            });
            latch.await(10, TimeUnit.SECONDS);
        });
    }
}
