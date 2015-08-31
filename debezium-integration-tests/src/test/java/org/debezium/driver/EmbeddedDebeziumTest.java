/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.TimeUnit;

import org.debezium.Testing;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Patch;
import org.debezium.driver.Debezium.BatchBuilder;
import org.debezium.driver.EntityChange.ChangeStatus;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * A set of integration tests for Debezium, using {@link EmbeddedDebezium} instances.
 * 
 * @author Randall Hauch
 */
public class EmbeddedDebeziumTest extends AbstractDebeziumTest {

    protected void printStatistics(String desc) {
        if (Testing.Print.isEnabled()) {
            ((EmbeddedDebezium) dbz).printStatistics(desc,Testing::print);
        }
    }

    @Override
    protected Debezium createClient() {
        return new EmbeddedDebezium();
    }

    @Test
    public void shouldCreateEmbeddedClientAndShutItDown() {
        // intentionally do nothing ...
    }

    @Test
    public void shouldConnectToDebeziumWithNoDatabases() {
        connect("db1");
        assertThat(session).isNotNull();
    }

    @Test
    public void shouldConnectAndProvisionNewDatabase() {
        connect("db1");
        provision("db1");
    }

    @Test
    public void shouldConnectAndNotBeAbleToProvisionNewDatabaseWithSameNameAsExisting() {
        connect("db1");
        provision("db1");
        try {
            provision("db1");
        } catch (DebeziumProvisioningException e) {
            // expected
        }
    }

    @Test
    public void shouldConnectAndProvisionAndReadSchemaOfEmptyDatabase() {
        //Testing.Print.enable();
        connect("db1");
        provision("db1");
        for (int i = 0; i != 15; ++i) {
            Schema schema = dbz.readSchema(session, "db1", 10, TimeUnit.SECONDS);
            assertThat(schema).isNotNull();
            assertThat(schema.entityTypes().isEmpty()).isTrue();
            ;
            assertThat(schema.asDocument().size()).isEqualTo(1);
            assertThat(schema.asDocument().get("db")).isEqualTo("db1");
        }
        printStatistics("shouldConnectAndProvisionAndReadSchemaOfEmptyDatabase");
    }

    @Test
    public void shouldConnectAndProvisionAndCreateOneEntityMultipleTimes() {
        //Testing.Print.enable();
        connect("my-db");
        provision("my-db");
        for (int i = 0; i != 15; ++i) {
            EntityId id = Identifier.newEntity(contactType);
            EntityChange change = dbz.changeEntity(session, Patch.create(id), 10, TimeUnit.SECONDS);
            assertThat(change.id()).isEqualTo(id.asString());
            assertThat(change.status()).isEqualTo(ChangeStatus.OK);
            assertThat(change.failed()).isFalse();
            assertThat(change.succeeded()).isTrue();
            assertThat(change.failureReasons().count()).isEqualTo(0);
            Entity entity = change.entity();
            assertThat(entity.exists()).isTrue();
            assertThat(entity.isMissing()).isFalse();
            assertThat(entity.id()).isEqualTo(id);
            Document doc = entity.asDocument();
            assertThat(doc).isNotNull();
        }
        printStatistics("shouldConnectAndProvisionAndCreateThenReadOneEntityMultipleTimes");
    }

    @Test
    public void shouldConnectAndProvisionAndCreateThenReadSeparatelyMultipleEntities() {
        //Testing.Print.enable();
        connect("my-db");
        provision("my-db");
        // Using batch ...
        BatchResult result = createEntities(100, contactType);
        assertThat(result.hasReads()).isFalse();
        assertThat(result.hasRemovals()).isFalse();
        assertThat(result.hasChanges()).isTrue();
        assertThat(result.isEmpty()).isFalse();
        assertThat(result.readStream().count()).isEqualTo(0);
        assertThat(result.reads().isEmpty()).isTrue();
        assertThat(result.removalStream().count()).isEqualTo(0);
        assertThat(result.removals().isEmpty()).isTrue();
        assertThat(result.changes().size()).isEqualTo(100);
        assertThat(result.changeStream().count()).isEqualTo(100);
        // Using separate read request for each entity ...
        result.changeStream().forEach(createResult->{
            EntityId id = createResult.entity().id();
            Entity readResult = dbz.readEntity(session, id, 10, TimeUnit.SECONDS);
            assertThat(createResult.entity()).isEqualTo(readResult);
        });
        printStatistics("shouldConnectAndProvisionAndCreateThenReadSeparatelyMultipleEntities");
    }

    @Test
    public void shouldConnectAndProvisionAndCreateThenUseBatchToReadMultipleEntities() {
        //Testing.Print.enable();
        connect("my-db");
        provision("my-db");
        // Using batch ...
        BatchResult result = createEntities(100, contactType);
        assertThat(result.hasReads()).isFalse();
        assertThat(result.hasRemovals()).isFalse();
        assertThat(result.hasChanges()).isTrue();
        assertThat(result.isEmpty()).isFalse();
        assertThat(result.readStream().count()).isEqualTo(0);
        assertThat(result.reads().isEmpty()).isTrue();
        assertThat(result.removalStream().count()).isEqualTo(0);
        assertThat(result.removals().isEmpty()).isTrue();
        assertThat(result.changes().size()).isEqualTo(100);
        assertThat(result.changeStream().count()).isEqualTo(100);
        // Use a single batch ...
        BatchBuilder batch = dbz.batch();
        result.changeStream().forEach(createResult->{
            EntityId id = createResult.entity().id();
            batch.readEntity(id);
        });
        BatchResult readResult = batch.submit(session, 10, TimeUnit.SECONDS);
        assertThat(readResult.hasReads()).isTrue();
        assertThat(readResult.hasRemovals()).isFalse();
        assertThat(readResult.hasChanges()).isFalse();
        assertThat(readResult.isEmpty()).isFalse();
        assertThat(readResult.readStream().count()).isEqualTo(100);
        assertThat(readResult.reads().isEmpty()).isFalse();
        assertThat(readResult.removalStream().count()).isEqualTo(0);
        assertThat(readResult.removals().isEmpty()).isTrue();
        assertThat(readResult.changes().size()).isEqualTo(0);
        assertThat(readResult.changeStream().count()).isEqualTo(0);
        result.changeStream().forEach(createdEntity->{
            Entity readEntity = readResult.reads().get(createdEntity.id());
            assertThat(createdEntity.entity()).isEqualTo(readEntity);
        });
        printStatistics("shouldConnectAndProvisionAndCreateThenUseBatchToReadMultipleEntities");
    }

    @Test
    public void shouldConnectAndProvisionAndCreateThenDeleteSeparatelyMultipleEntities() {
        //Testing.Print.enable();
        connect("my-db");
        provision("my-db");
        // Using batch ...
        BatchResult result = createEntities(100, contactType);
        assertThat(result.hasReads()).isFalse();
        assertThat(result.hasRemovals()).isFalse();
        assertThat(result.hasChanges()).isTrue();
        assertThat(result.isEmpty()).isFalse();
        assertThat(result.readStream().count()).isEqualTo(0);
        assertThat(result.reads().isEmpty()).isTrue();
        assertThat(result.removalStream().count()).isEqualTo(0);
        assertThat(result.removals().isEmpty()).isTrue();
        assertThat(result.changes().size()).isEqualTo(100);
        assertThat(result.changeStream().count()).isEqualTo(100);
        // Using separate read request for each entity ...
        result.changeStream().forEach(createResult->{
            EntityId id = createResult.entity().id();
            boolean success = dbz.destroyEntity(session, id, 10, TimeUnit.SECONDS);
            assertThat(success).isTrue();
        });
        printStatistics("shouldConnectAndProvisionAndCreateThenDeleteSeparatelyMultipleEntities");
    }

    @Test
    public void shouldConnectAndProvisionAndCreateThenUseBatchToDeleteMultipleEntities() {
        //Testing.Print.enable();
        connect("my-db");
        provision("my-db");
        // Using batch ...
        BatchResult result = createEntities(100, contactType);
        assertThat(result.hasReads()).isFalse();
        assertThat(result.hasRemovals()).isFalse();
        assertThat(result.hasChanges()).isTrue();
        assertThat(result.isEmpty()).isFalse();
        assertThat(result.readStream().count()).isEqualTo(0);
        assertThat(result.reads().isEmpty()).isTrue();
        assertThat(result.removalStream().count()).isEqualTo(0);
        assertThat(result.removals().isEmpty()).isTrue();
        assertThat(result.changes().size()).isEqualTo(100);
        assertThat(result.changeStream().count()).isEqualTo(100);
        // Use a single batch ...
        BatchBuilder batch = dbz.batch();
        result.changeStream().forEach(createResult->{
            EntityId id = createResult.entity().id();
            batch.destroyEntity(id);
        });
        BatchResult deleteResult = batch.submit(session, 10, TimeUnit.SECONDS);
        assertThat(deleteResult.hasReads()).isFalse();
        assertThat(deleteResult.hasRemovals()).isTrue();
        assertThat(deleteResult.hasChanges()).isFalse();
        assertThat(deleteResult.isEmpty()).isFalse();
        assertThat(deleteResult.readStream().count()).isEqualTo(0);
        assertThat(deleteResult.reads().isEmpty()).isTrue();
        assertThat(deleteResult.removalStream().count()).isEqualTo(100);
        assertThat(deleteResult.removals().isEmpty()).isFalse();
        assertThat(deleteResult.changes().size()).isEqualTo(0);
        assertThat(deleteResult.changeStream().count()).isEqualTo(0);
        result.removalStream().forEach(entityId->{
            boolean found = deleteResult.reads().containsKey(entityId);
            assertThat(found).isTrue();
        });
        printStatistics("shouldConnectAndProvisionAndCreateThenUseBatchToDeleteMultipleEntities");
    }

    @Test
    public void shouldNotRemoveNonExistingEntity() {
        //Testing.Print.enable();
        connect("my-db");
        provision("my-db");
        // Using batch ...
        BatchResult result = createEntities(1, contactType);
        EntityId existingId = result.changeStream().findFirst().get().entity().id();
        EntityId nonExistingId = Identifier.newEntity(contactType);
        // Destroy a non-existent entity ...
        boolean removed = dbz.destroyEntity(session, nonExistingId, 10, TimeUnit.SECONDS);
        assertThat(removed).isFalse();
        // Destroy an existing entity ...
        removed = dbz.destroyEntity(session, existingId, 10, TimeUnit.SECONDS);
        assertThat(removed).isTrue();
        printStatistics("shouldNotRemoveNonExistingEntity");
    }

    @Test
    public void shouldNotReadNonExistingEntity() {
        //Testing.Print.enable();
        connect("my-db");
        provision("my-db");
        // Using batch ...
        BatchResult result = createEntities(1, contactType);
        assertThat(result.isEmpty()).isFalse();
        
        // Read a non-existent entity ...
        EntityId nonExistingId = Identifier.newEntity(contactType);
        Entity readResult = dbz.readEntity(session, nonExistingId, 10, TimeUnit.SECONDS);
        assertThat(readResult.exists()).isFalse();
        assertThat(readResult.isMissing()).isTrue();
        assertThat(readResult.asDocument()).isNull();
        assertThat(readResult.id()).isEqualTo(nonExistingId);
        printStatistics("shouldNotReadNonExistingEntity");
    }
}
