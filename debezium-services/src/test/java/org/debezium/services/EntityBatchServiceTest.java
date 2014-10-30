/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import static org.fest.assertions.Assertions.assertThat;

import java.util.UUID;

import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Topics;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Randall Hauch
 */
public class EntityBatchServiceTest extends AbstractServiceTest {
    
    private EntityBatchService service;
    
    @Before
    public void beforeEach() {
        service = new EntityBatchService();
    }
    
    @Test
    public void shouldHandleEmptyBatch() {
        Batch<Identifier> batch = Batch.create().build();
        OutputMessages output = process(service, random(), batch.asDocument());
        assertThat(output.isEmpty()).isTrue();
    }
    
    @Test
    public void shouldHandleBatchWithOnePatch() {
        EntityId id = Identifier.of("db", "collection", "ent1");
        Batch<Identifier> batch = Batch.create()
                                       .create(id).add("field1", Value.create(1)).end()
                                       .build();
        OutputMessages output = process(service, random(), batch.asDocument());
        assertNextMessage(output).hasStream(Topics.ENTITY_PATCHES).isPart(1, 1).hasKey(id).hasMessage(batch.patch(0).asDocument());
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
        OutputMessages output = process(service, random(), batch.asDocument());
        assertThat(output.count()).isEqualTo(3);
        assertNextMessage(output).hasStream(Topics.ENTITY_PATCHES).isPart(1, 3).hasKey(id1).hasMessage(batch.patch(0).asDocument());
        assertNextMessage(output).hasStream(Topics.ENTITY_PATCHES).isPart(2, 3).hasKey(id2).hasMessage(batch.patch(1).asDocument());
        assertNextMessage(output).hasStream(Topics.ENTITY_PATCHES).isPart(3, 3).hasKey(id3).hasMessage(batch.patch(2).asDocument());
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
        OutputMessages output = process(service, random(), batch.asDocument());
        assertThat(output.count()).isEqualTo(3);
        assertNextMessage(output).hasStream(Topics.ENTITY_PATCHES).isPart(1, 3).hasKey(id1).hasMessage(batch.patch(0).asDocument());
        assertNextMessage(output).hasStream(Topics.ENTITY_PATCHES).isPart(2, 3).hasKey(id1).hasMessage(batch.patch(1).asDocument());
        assertNextMessage(output).hasStream(Topics.ENTITY_PATCHES).isPart(3, 3).hasKey(id1).hasMessage(batch.patch(2).asDocument());
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
        OutputMessages output = process(service, random(), msg);
        assertThat(output.count()).isEqualTo(1);
        Document patch = batch.patch(0).asDocument();
        Message.addHeaders(patch, clientId, requestNum, user, timestamp);
        assertNextMessage(output).hasStream(Topics.ENTITY_PATCHES).isPart(1, 1).hasKey(id).hasMessage(patch);
        assertNoMoreMessages(output);
    }
}
