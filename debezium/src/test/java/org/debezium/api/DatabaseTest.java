/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.debezium.KafkaTestCluster;
import org.debezium.api.Database.ChangeEntity;
import org.debezium.api.Debezium.Acknowledgement;
import org.debezium.api.message.Batch;
import org.debezium.api.message.Patch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Randall Hauch
 */
public class DatabaseTest {
    
    private static KafkaTestCluster kafka;
    private static Debezium.Client client = null;

    private String username = "jsmith";
    private DatabaseId databaseId = Identifier.of("firstDatabase");
    private EntityType type = Identifier.of(databaseId, "Address");
    private Database database;
    
    @BeforeClass
    public static void beforeAll() throws Exception {
        kafka = KafkaTestCluster.forTest(DebeziumClientTest.class);
        Debezium.Configuration config = Debezium.configure()
                .clientId(DatabaseTest.class.getSimpleName())
                .withBroker(kafka.getKafkaBrokerString())
                .acknowledgement(Acknowledgement.ALL)
                .build();
        client = Debezium.start(config);
    }
    
    @AfterClass
    public static void afterAll() throws IOException {
        if ( client != null ) client.shutdown(5,TimeUnit.SECONDS);
        kafka.stopAndCleanUp();
    }
    
    @Before
    public void beforeEach() {
        database = client.connect(databaseId,username);
        assertThat(database).isNotNull();
    }
    
    @After
    public void afterEach() {
        database.close();
    }
    
    @Test
    public void shouldReadEntitiesAndIterate() {
        final Collection<EntityId> entityIds = new ArrayList<>();
        entityIds.add(Identifier.of(type, "myEntityId"));
        database.readEntities(entityIds, (outcome) -> {
            if (outcome.succeeded()) {
                for (Entity entity : outcome.result()) {
                    assert entity != null;
                }
                // Any entity not in the results does not exist ...
            } else {
                // Unable to read, so handle the error
                Throwable t = outcome.cause();
                assert t != null;
            }
        });
    }
    
    @Test
    public void shouldReadEntitiesAndLookupById() {
        final Collection<EntityId> entityIds = new ArrayList<>();
        entityIds.add(Identifier.of(type, "myEntityId"));
        database.readEntities(entityIds, (outcome) -> {
            if (outcome.succeeded()) {
                for (EntityId id : entityIds) {
                    Entity entity = outcome.result().get(id);
                    if (entity != null) {
                        // This entity does exist ...
                    } else {
                        // This entity does not exist ...
                    }
                }
            } else {
                // Unable to read, so handle the error
                Throwable t = outcome.cause();
                assert t != null;
            }
        });
    }
    
    @Test
    public void shouldReadSingleEntity() {
        EntityId entityId = Identifier.of(type, "myEntityId");
        database.readEntity(entityId, (outcome) -> {
            if (outcome.succeeded()) {
                Entity entity = outcome.result();
                assert entity != null;
            } else {
                // Unable to read, so handle the error
                Throwable t = outcome.cause();
                assert t != null;
            }
        });
    }
    
    @Test
    public void shouldChangeEntities() throws InterruptedException {
        Batch<EntityId> batch = null;
        CountDownLatch latch = new CountDownLatch(1); // let's us wait for the handler to be called ...
        database.changeEntities(batch, (outcome) -> {
            if (outcome.succeeded()) {
                // We successfully submitted the batch, but see which of our patches were successfully applied ...
                for (ChangeEntity change : outcome.result()) {
                    if (change.succeeded()) {
                        // We successfully changed this entity by applying our patch ...
                    } else {
                        // Our patch for this entity could not be applied ...
                        switch (change.failureCode()) {
                            case PREVIOUSLY_DELETED:
                                // The entity was removed by someone else, so we should delete it locally ...
                                break;
                            case SCHEMA_NOT_SATISFIED:
                                // Our changes did not satisfy the schema requirements, so this is likely a
                                // programming error on our part ...
                                break;
                            case PRECONDITIONS_NOT_SATISFIED:
                                // We put preconditions into our patch that were not satisfied, so perhaps we
                                // should rebuild a new patch with the latest representation of the target ...
                                Patch<EntityId> failedPatch = change.failedPatch();
                                Entity target = change.unmodifiedTarget();
                                assert failedPatch != null;
                                assert target != null;
                                break;
                            case OPERATION_FAILED:
                                // Our changes could not be applied. This is rare since Debezium should be able
                                // to apply almost any patch as long as it satisfies the schema. So perhaps
                                // try rebuilding a new patch with the latest representation of the target ...
                                failedPatch = change.failedPatch();
                                target = change.unmodifiedTarget();
                                assert failedPatch != null;
                                assert target != null;
                                break;
                        }
                    }
                }
            } else {
                // Unable to even submit the batch, so handle the error. Perhaps the system is not available, or
                // our request was poorly formed (e.g., was empty). The best way to handle this is to do different
                // things based upon the exception type ...
                Throwable t = outcome.cause();
                assert t != null;
            }
            latch.countDown();
        });
        
        // This is a test case that is synchronous, so wait 5 seconds for the handler to be called ...
        latch.await(5L, TimeUnit.SECONDS);
    }
    
    @Test
    public void shouldReadSchema() {
        database.readSchema((outcome) -> {
            if (outcome.succeeded()) {
                for (EntityCollection collection : outcome.result()) {
                    assert collection != null;
                }
            } else {
                // Unable to read the schema, so handle the error
                Throwable t = outcome.cause();
                assert t != null;
            }
        });
    }
    
    @Test
    public void shouldReadSingleTypeFromSchema() {
        database.readSchema(type, (outcome) -> {
            if (outcome.succeeded()) {
                EntityCollection collection = outcome.result();
                if (collection != null) {
                    // The collection exists, so do something with this ...
            }
        } else {
            // Unable to read the schema, so handle the error
                Throwable t = outcome.cause();
                assert t != null;
            }
        });
    }
    
}
