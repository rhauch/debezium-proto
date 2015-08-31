/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.util;

import java.util.concurrent.atomic.AtomicLong;

import org.debezium.message.Batch;
import org.debezium.model.DatabaseId;
import org.debezium.model.Entity;
import org.debezium.model.EntityId;
import org.debezium.model.EntityType;
import org.debezium.model.Identifier;
import org.debezium.util.RandomContent;
import org.debezium.util.RandomContent.ContentGenerator;
import org.debezium.util.RandomContent.IdGenerator;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class RandomContentTest {
    
    private static final RandomContent CONTENT = RandomContent.load("load-data.txt", RandomContentTest.class);

    private static DatabaseId DBNAME = Identifier.of("myDb");
    private static EntityType CONTACT = Identifier.of(DBNAME,"contact");
    private static EntityType CUSTOMER = Identifier.of(DBNAME,"customer");
    private ContentGenerator generator;
    
    @Before
    public void beforeEach() {
        generator = CONTENT.createGenerator();
    }
    
    @Test
    public void shouldCreateEntity() {
        EntityId id = Identifier.newEntity(CONTACT);
        Entity entity = generator.generateEntity(id);
        assertThat(entity).isNotNull();
        assertThat(entity.id()).isEqualTo(id);
        assertThat(entity.document().size()).isGreaterThan(1);  // more than just the ID
    }
    
    @Test
    public void shouldNotShareGenerators() {
        assertThat(CONTENT.createGenerator()).isNotSameAs(generator);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void shouldNotAllowNullEntityIdWhenGeneratingAnEntity() {
        generator.generateEntity(null);
    }
    
    @Test
    public void shouldGenerateBatch() {
        Batch<EntityId> batch = generator.generateBatch(3,5,CONTACT);
        assertThat(batch.appliesTo(DBNAME)).isTrue();
        AtomicLong numEdits = new AtomicLong();
        AtomicLong numRemoves = new AtomicLong();
        batch.forEach(patch->{
            if ( patch.isDeletion() ) numRemoves.incrementAndGet();
            else if ( !patch.isReadRequest() ) numEdits.incrementAndGet();
        });
        assertThat(numEdits.get()).isEqualTo(3);
        assertThat(numRemoves.get()).isEqualTo(5);
    }
    
    @Test
    public void shouldGenerateARandomSizedBatch() {
        IdGenerator idGen = CONTENT.createIdGenerator(3, 30, CONTACT, CUSTOMER );
        assertThat(idGen).isNotNull();
        Batch<EntityId> batch = generator.generateBatch(idGen);
        assertThat(batch.appliesTo(DBNAME)).isTrue();
        AtomicLong numEdits = new AtomicLong();
        AtomicLong numRemoves = new AtomicLong();
        AtomicLong numContacts = new AtomicLong();
        AtomicLong numCustomers = new AtomicLong();
        batch.forEach(patch->{
            if ( patch.isDeletion() ) numRemoves.incrementAndGet();
            else if ( !patch.isReadRequest() ) numEdits.incrementAndGet();
            EntityType type = patch.target().type();
            if ( type.equals(CONTACT)) numContacts.incrementAndGet();
            if ( type.equals(CUSTOMER)) numCustomers.incrementAndGet();
        });
        assertThat(numEdits.get()).isGreaterThanOrEqualTo(3);
        assertThat(numEdits.get()).isLessThanOrEqualTo(30);
        assertThat(numRemoves.get()).isGreaterThanOrEqualTo(3);
        assertThat(numRemoves.get()).isLessThanOrEqualTo(30);
        assertThat(numContacts.get() + numCustomers.get()).isEqualTo(batch.patchCount());
        assertThat(numContacts.get()).isGreaterThan(0);
        assertThat(numCustomers.get()).isGreaterThan(0);
    }
    
    

}
