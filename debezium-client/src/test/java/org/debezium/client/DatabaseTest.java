/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.debezium.KafkaTestCluster;
import org.debezium.client.Debezium.Acknowledgement;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.Schema;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Patch;
import org.fest.assertions.Fail;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Randall Hauch
 */
@Ignore("We can't run the client locally because the backend services don't run in unit test")
public class DatabaseTest {

    private static KafkaTestCluster kafka;
    private static Debezium.Client client = null;

    private String username = "jsmith";
    private DatabaseId databaseId = Identifier.of("firstDatabase");
    private EntityType type = Identifier.of(databaseId, "Address");
    private Database database;

    @BeforeClass
    public static void beforeAll() throws Exception {
        kafka = KafkaTestCluster.forTest(DatabaseTest.class);
        Configuration config = Debezium.configure()
                                       .clientId(DatabaseTest.class.getSimpleName())
                                       .withBroker(kafka.getKafkaBrokerString())
                                       .withZookeeper(kafka.getZkConnectString())
                                       .acknowledgement(Acknowledgement.ALL)
                                       .lazyInitialization(true)
                                       .build();
        client = Debezium.start(config);
    }

    @AfterClass
    public static void afterAll() throws IOException {
        try {
            if (client != null) client.shutdown(5, TimeUnit.SECONDS);
        } finally {
            kafka.stopAndCleanUp();
        }
    }

    @Before
    public void beforeEach() {
        database = client.connect(databaseId, username);
        assertThat(database).isNotNull();
    }

    @After
    public void afterEach() {
        database.close();
    }

    @Test
    public void shouldBeAvailableAfterStartup() throws InterruptedException, TimeoutException {
        assertThat(database.isConnected()).isTrue();
    }

    @Test
    public void shouldReadSchema() throws InterruptedException, TimeoutException {
        database.readSchema((outcome) -> {
            if (outcome.succeeded()) {
                Schema schema = outcome.result();
                assertThat(schema.id()).isNotNull();
            } else {
                // Unable to read the schema, so handle the error
                String reason = outcome.failureReason();
                assertThat(reason).isNotNull();
            }
        }).await(10,TimeUnit.SECONDS);
    }

    @Test
    public void shouldReadEntitiesAndIterate() throws InterruptedException, TimeoutException {
        final Collection<EntityId> entityIds = new ArrayList<>();
        entityIds.add(Identifier.of(type, "myEntityId"));
        database.readEntities(entityIds, (outcome) -> {
            if (outcome.succeeded()) {
                long numEntitiesRead = outcome.result().filter(Entity::exists).count();
                assertThat(numEntitiesRead).isEqualTo(entityIds.size());

                outcome.result().filter(Entity::exists).forEach(entity -> {
                    // Iterate over each existing entity ...
                    assertThat(entity.document()).isNotNull();
                });

                outcome.result().filter(Entity::isMissing).forEach(entity -> {
                    assertThat(entity.document()).isNull();
                });
            } else {
                String reason = outcome.failureReason();
                assertThat(reason).isNotNull();
            }
        }).await(10,TimeUnit.SECONDS);
    }

    @Test
    public void shouldReadSingleEntity() throws InterruptedException, TimeoutException {
        EntityId entityId = Identifier.of(type, "myEntityId");
        database.readEntity(entityId, (outcome) -> {
            if (outcome.succeeded()) {
                // We can do all kinds of things with the stream of entities ...
                boolean exists = outcome.result().filter(Entity::exists).count() == 1L;
                assertThat(exists).isTrue();

                // Plus everything else
            } else {
                // Unable to read, so handle the error
                String reason = outcome.failureReason();
                assertThat(reason).isNotNull();
            }
        }).await(10,TimeUnit.SECONDS);
    }

    @Test
    public void shouldChangeEntities() throws InterruptedException, TimeoutException {
        Batch<EntityId> batch = null;
        database.changeEntities(batch, (outcome) -> {
            if (outcome.succeeded()) {
                outcome.result().forEach(change -> {
                    if (change.succeeded()) {
                        // We successfully changed this entity by applying our patch ...
                    } else {
                        // Our patch for this entity could not be applied ...
                        switch (change.status()) {
                            case OK:
                                Fail.fail("This should never happen, since we already know that the change failed");
                                break;
                            case DOES_NOT_EXIST:
                                // The entity was removed by someone else, so we should delete it locally ...
                                break;
                            case PATCH_FAILED:
                                // We put preconditions into our patch that were not satisfied, so perhaps we
                                // should rebuild a new patch with the latest representation of the target ...
                                Patch<EntityId> failedPatch = change.patch();
                                Entity target = change.target();
                                assert failedPatch != null;
                                assert target != null;
                                break;
                        }
                    }
                });
            } else {
                // Unable to even submit the batch, so handle the error. Perhaps the system is not available, or
                // our request was poorly formed (e.g., was empty). The best way to handle this is to do different
                // things based upon the exception type ...
                String reason = outcome.failureReason();
                assert reason != null;
            }
        }).await(5L,TimeUnit.SECONDS);
    }

    // @Test
    // public void shouldReadSchema() {
    // database.readSchema((outcome) -> {
    // if (outcome.succeeded()) {
    // for (EntityCollection collection : outcome.result()) {
    // assert collection != null;
    // }
    // } else {
    // // Unable to read the schema, so handle the error
    // Throwable t = outcome.cause();
    // assert t != null;
    // }
    // });
    // }
    //
    // @Test
    // public void shouldReadSingleTypeFromSchema() {
    // database.readSchema(type, (outcome) -> {
    // if (outcome.succeeded()) {
    // EntityCollection collection = outcome.result();
    // if (collection != null) {
    // // The collection exists, so do something with this ...
    // }
    // } else {
    // // Unable to read the schema, so handle the error
    // Throwable t = outcome.cause();
    // assert t != null;
    // }
    // });
    // }

}
