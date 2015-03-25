/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.io.IOException;
import java.util.UUID;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Topic;
import org.debezium.samza.AbstractServiceTest;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 */
public class EntityBatchServiceTest extends AbstractServiceTest {
    
    private static final DatabaseId DB_ID = Identifier.of("db");
    
    private MetricService service;
    
    @Before
    public void beforeEach() {
        service = new MetricService();
    }

    protected Document batchToDocumentWithDbId( Batch<?> batch ) {
        Document message = batch.asDocument();
        Message.addId(message,DB_ID);
        return message;
    }
    
    @Test
    public void shouldHandleEmptyBatch() {
        Batch<Identifier> batch = Batch.create().build();
        OutputMessages output = process(service, random(), batchToDocumentWithDbId(batch));
        assertThat(output.isEmpty()).isTrue();
    }
    
    @Test
    public void shouldHandleBatchWithOnePatch() {
        EntityId id = Identifier.of("db", "collection", "ent1");
        Batch<Identifier> batch = Batch.create()
                                       .create(id).add("field1", Value.create(1)).end()
                                       .build();
        OutputMessages output = process(service, random(), batchToDocumentWithDbId(batch));
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(1, 1).hasKey(id).hasMessage(batch.patch(0).asDocument());
        assertNoMoreMessages(output);
    }
    
    @Test
    public void shouldHandleBatchWithMultiplePatchesOnDifferentEntities() {
        EntityId id1 = Identifier.of("db", "collection", "ent1");
        EntityId id2 = Identifier.of("db", "collection", "ent2");
        EntityId id3 = Identifier.of("db", "collection", "ent3");
        Batch<Identifier> batch = Batch.create()
                                       .create(id1).add("field1", Value.create(1)).end()
                                       .create(id2).add("field2", Value.create(2)).end()
                                       .create(id3).add("field3", Value.create(3)).end()
                                       .build();
        System.out.println(batch.asDocument());
        OutputMessages output = process(service, random(), batchToDocumentWithDbId(batch));
        assertThat(output.count()).isEqualTo(3);
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(1, 3).hasKey(id1).hasMessage(batch.patch(0).asDocument());
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(2, 3).hasKey(id2).hasMessage(batch.patch(1).asDocument());
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(3, 3).hasKey(id3).hasMessage(batch.patch(2).asDocument());
        assertNoMoreMessages(output);
    }
    
    /**
     * This is a pathological case, since the writer to the stream should never add a batch that contains more than one patch
     * per entity.
     */
    @Test
    public void shouldHandleBatchWithMultiplePatchesOnSameEntity() {
        EntityId id1 = Identifier.of("db", "collection", "ent1");
        Batch<Identifier> batch = Batch.create()
                                       .create(id1).add("field1", Value.create(1)).end()
                                       .create(id1).add("field2", Value.create(2)).end()
                                       .create(id1).add("field3", Value.create(3)).end()
                                       .build();
        OutputMessages output = process(service, random(), batchToDocumentWithDbId(batch));
        assertThat(output.count()).isEqualTo(3);
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(1, 3).hasKey(id1).hasMessage(batch.patch(0).asDocument());
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(2, 3).hasKey(id1).hasMessage(batch.patch(1).asDocument());
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(3, 3).hasKey(id1).hasMessage(batch.patch(2).asDocument());
        assertNoMoreMessages(output);
    }
    
    @Test
    public void shouldHandleBatchWithSinglePatch() throws IOException {
        Document batchRequest = readMessage(Topic.ENTITY_BATCHES, "create-contact.json");
        Document patchOutput = readMessage(Topic.ENTITY_PATCHES, "create-contact-1of1.json");
        EntityId id1 = Message.getEntityId(patchOutput);
        OutputMessages output = processMessage(service, batchRequest);
        assertThat(output.count()).isEqualTo(1);
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(1, 1).hasKey(id1).hasMessage(patchOutput);
        assertNoMoreMessages(output);
    }
    
    @Test
    public void shouldHandleBatchWithMultiplePatches() throws IOException {
        Document batchRequest = readMessage(Topic.ENTITY_BATCHES, "create-multiple-contacts.json");
        Document patchOutput1 = readMessage(Topic.ENTITY_PATCHES, "create-contact-1of2.json");
        Document patchOutput2 = readMessage(Topic.ENTITY_PATCHES, "create-contact-2of2.json");
        EntityId id1 = Message.getEntityId(patchOutput1);
        EntityId id2 = Message.getEntityId(patchOutput2);
        OutputMessages output = processMessage(service, batchRequest);
        assertThat(output.count()).isEqualTo(2);
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(1, 2).hasKey(id1).hasMessage(patchOutput1);
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(2, 2).hasKey(id2).hasMessage(patchOutput2);
        assertNoMoreMessages(output);
    }
    
    @Test
    public void shouldHandleBatchWithAllHeaders() {
        String clientId = UUID.randomUUID().toString();
        long requestNum = 301L;
        String user = "jsmith";
        long timestamp = System.currentTimeMillis();
        
        // Create the batch ...
        EntityId id = Identifier.of("db", "collection", "ent1");
        Batch<Identifier> batch = Batch.create()
                                       .create(id).add("field1", Value.create(1)).end()
                                       .build();
        
        Document msg = batch.asDocument();
        Message.addHeaders(msg, clientId, requestNum, user, timestamp);
        Message.addId(msg, DB_ID);
        OutputMessages output = process(service, random(), msg);
        assertThat(output.count()).isEqualTo(1);
        Document patch = batch.patch(0).asDocument();
        Message.addHeaders(patch, clientId, requestNum, user, timestamp);
        assertNextMessage(output).hasStream(Topic.ENTITY_PATCHES).isPart(1, 1).hasKey(id).hasMessage(patch);
        assertNoMoreMessages(output);
    }

}
